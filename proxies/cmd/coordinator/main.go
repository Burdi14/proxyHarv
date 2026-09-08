// Command coordinator is the single writer to postgres (§2): it consumes
// RAW/RESULTS, maintains nodes, plans the L0/L1/L2/validator pyramid (§4),
// publishes list versions (§4) and watches vantage heartbeats.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"proxyfarm/internal/geo"
	"proxyfarm/internal/logx"
	"proxyfarm/internal/metrics"
	"proxyfarm/internal/model"
	"proxyfarm/internal/natsb"
	"proxyfarm/internal/score"
)

var log = logx.New("coordinator")

type config struct {
	pgDSN        string
	natsURL      string
	natsCreds    string
	mmdbPath     string
	probeDomain  string
	weights      score.Weights
	planEvery    time.Duration
	publishEvery time.Duration
	batchL0      int
	batchL1      int
	batchL2      int
	topL2        int
	topVal       int
	valL2Top     int
	metricsAddr  string
	reg          *metrics.Registry
}

func main() {
	cfg := &config{}
	var (
		migrate    = flag.Bool("migrate", false, "apply migrations and exit")
		seedSQL    = flag.String("seed-sql", "", "apply a .sql file (e.g. fixtures/subs_seed.sql) and exit")
		weightsStr = flag.String("score-weights", envOr("SCORE_WEIGHTS", ""), "w1,w2,w3,w4,w5 (§7)")
	)
	flag.StringVar(&cfg.pgDSN, "pg", envOr("PG_DSN", "postgres://proxyfarm:proxyfarm@localhost:5432/proxyfarm?sslmode=disable"), "postgres dsn")
	flag.StringVar(&cfg.natsURL, "nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "nats url")
	flag.StringVar(&cfg.natsCreds, "nats-creds", envOr("NATS_CREDS", ""), "nats credentials")
	flag.StringVar(&cfg.mmdbPath, "mmdb", envOr("MMDB_PATH", ""), "GeoLite2-City.mmdb path (empty = no geo)")
	flag.StringVar(&cfg.probeDomain, "probe-domain", envOr("PROBE_DOMAIN", ""), "probe domain for worker fallback targets")
	flag.DurationVar(&cfg.planEvery, "plan-every", envDurOr("PLAN_EVERY", time.Minute), "planner tick")
	flag.DurationVar(&cfg.publishEvery, "publish-every", envDurOr("PUBLISH_EVERY", 6*time.Hour), "publish period (§17)")
	flag.IntVar(&cfg.batchL0, "batch-l0", 100, "L0 job batch")
	flag.IntVar(&cfg.batchL1, "batch-l1", 50, "L1 job batch")
	flag.IntVar(&cfg.batchL2, "batch-l2", 10, "L2 job batch")
	flag.IntVar(&cfg.topL2, "top-l2", 300, "L2 candidates per cycle (§17)")
	flag.IntVar(&cfg.topVal, "top-val", 50, "validator top-N per cycle (§17)")
	flag.IntVar(&cfg.valL2Top, "val-l2-top", 10, "validator L2 top nodes")
	flag.StringVar(&cfg.metricsAddr, "metrics-addr", envOr("METRICS_ADDR", ":9102"), "prometheus listen addr")
	flag.Parse()

	cfg.weights = score.Default()
	if strings.TrimSpace(*weightsStr) != "" {
		if w, ok := score.ParseWeights(*weightsStr); ok {
			cfg.weights = w
		} else {
			log.Error("bad SCORE_WEIGHTS, using defaults")
		}
	}
	cfg.reg = metrics.NewRegistry()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := model.Open(ctx, cfg.pgDSN)
	if err != nil {
		log.Error("pg open", "err", err.Error())
		os.Exit(1)
	}
	defer store.Close()

	if *migrate {
		if err := store.Migrate(ctx); err != nil {
			log.Error("migrate", "err", err.Error())
			os.Exit(1)
		}
		log.Info("migrations applied")
		return
	}
	if *seedSQL != "" {
		body, err := os.ReadFile(*seedSQL)
		if err != nil {
			log.Error("read seed", "err", err.Error())
			os.Exit(1)
		}
		if err := store.SeedSQL(ctx, string(body)); err != nil {
			log.Error("seed", "err", err.Error())
			os.Exit(1)
		}
		log.Info("seed applied", "file", *seedSQL)
		return
	}

	nc, err := natsb.Connect(cfg.natsURL, cfg.natsCreds)
	if err != nil {
		log.Error("nats connect", "err", err.Error())
		os.Exit(1)
	}
	defer nc.Drain()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("jetstream", "err", err.Error())
		os.Exit(1)
	}
	if err := natsb.EnsureAll(ctx, js); err != nil {
		log.Error("ensure streams", "err", err.Error())
		os.Exit(1)
	}
	bus := natsb.NewBus(js)

	var locator *geo.Locator
	if cfg.mmdbPath != "" {
		locator, err = geo.New(cfg.mmdbPath, time.Hour) // DNS cache TTL 1h (§17)
		if err != nil {
			log.Error("mmdb open", "err", err.Error())
			os.Exit(1)
		}
		defer locator.Close()
	}

	run(ctx, store, bus, js, locator, cfg)
	log.Info("coordinator stopped")
}

func run(ctx context.Context, store model.Store, bus *natsb.Bus, js jetstream.JetStream, loc *geo.Locator, cfg *config) {
	// leader lease (replica protection, §3)
	instanceID := os.Getenv("HOSTNAME")
	if instanceID == "" {
		instanceID = "coordinator-local"
	}
	release, leader, err := bus.TryAcquireLease(ctx, instanceID, 30*time.Second, 10*time.Second)
	if err != nil {
		log.Error("lease", "err", err.Error())
		os.Exit(1)
	}
	defer release()
	if !leader() {
		log.Warn("another coordinator holds the lease; standing by")
		for !leader() && ctx.Err() == nil {
			time.Sleep(5 * time.Second)
		}
		if ctx.Err() != nil {
			return
		}
		log.Info("lease acquired")
	}

	c := &coord{store: store, bus: bus, js: js, loc: loc, cfg: cfg}

	// ensure worker consumers exist before publishing (interest retention §5)
	for _, cc := range []struct{ stream, durable, filter string }{
		{natsb.StreamRAW, natsb.DurableRaw, natsb.SubjectRaw},
		{natsb.StreamRESULTS, natsb.DurableResults, "results.>"},
		{natsb.StreamJOBSL0, natsb.DurableWorkerL0, "jobs.l0.>"},
		{natsb.StreamJOBSL1, natsb.DurableWorkerL1, "jobs.l1.>"},
		{natsb.StreamJOBSL2, natsb.DurableWorkerL2, "jobs.l2.>"},
	} {
		if _, err := natsb.EnsureConsumer(ctx, js, cc.stream, cc.durable, cc.filter); err != nil {
			log.Error("consumer ensure", "stream", cc.stream, "err", err.Error())
			os.Exit(1)
		}
	}

	go c.consumeRaw(ctx)
	go c.consumeResults(ctx)
	go c.watchVantages(ctx)
	go c.planLoop(ctx)
	go c.publishLoop(ctx)
	go c.maintenanceLoop(ctx)
	go c.serveMetrics(ctx)

	<-ctx.Done()
	time.Sleep(500 * time.Millisecond) // let goroutines unwind
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
