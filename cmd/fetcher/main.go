// Command fetcher downloads subscription sources, extracts share-links and
// publishes them as RAW batches (§3). Sources live in the DB (T-010).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"proxyfarm/internal/logx"
	"proxyfarm/internal/model"
	"proxyfarm/internal/natsb"
	"proxyfarm/internal/parse"
)

var log = logx.New("fetcher")

const maxBodyBytes = 32 << 20 // 32 MB per subscription

func main() {
	var (
		pgDSN     = flag.String("pg", envOr("PG_DSN", "postgres://proxyfarm:proxyfarm@localhost:5432/proxyfarm?sslmode=disable"), "postgres dsn")
		natsURL   = flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "nats url")
		natsCreds = flag.String("nats-creds", envOr("NATS_CREDS", ""), "nats credentials")
		once      = flag.Bool("once", false, "run one pass and exit")
		interval  = flag.Duration("interval", envDurOr("FETCH_INTERVAL", 30*time.Minute), "loop interval")
		timeout   = flag.Duration("timeout", 30*time.Second, "per-source http timeout")
		batch     = flag.Int("batch", 100, "RAW batch size (§17)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := model.Open(ctx, *pgDSN)
	if err != nil {
		log.Error("pg open", "err", err.Error())
		os.Exit(1)
	}
	defer store.Close()

	nc, err := natsb.Connect(*natsURL, *natsCreds)
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
	bus := natsb.NewBus(js)

	f := &fetcher{store: store, bus: bus, batch: *batch, client: &http.Client{Timeout: *timeout}}
	for {
		if err := f.pass(ctx); err != nil {
			log.Error("pass failed", "err", err.Error())
		}
		if *once || ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(*interval):
		}
	}
}

type fetcher struct {
	store  model.Store
	bus    *natsb.Bus
	batch  int
	client *http.Client
}

func (f *fetcher) pass(ctx context.Context) error {
	sources, err := f.store.ListSources(ctx, true)
	if err != nil {
		return err
	}
	log.Info("pass start", "sources", len(sources))
	dead := 0
	for _, src := range sources {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		status, n, err := f.fetchSource(ctx, src)
		if err != nil {
			dead++
			log.Warn("source failed", "source_id", src.ID, "err", err.Error())
		} else {
			log.Info("source fetched", "source_id", src.ID, "nodes", n, "status", status)
		}
		_ = f.store.UpdateSourceStatus(ctx, src.ID, status, time.Now().UTC())
	}
	if total := len(sources); total > 0 && dead*100/total > 30 {
		log.Warn("more than 30% of sources dead", "dead", dead, "total", total) // §12
	}
	return nil
}

func (f *fetcher) fetchSource(ctx context.Context, src model.Source) (status string, published int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return "err:badurl", 0, err
	}
	req.Header.Set("User-Agent", "proxyfarm/0.1 (+https://proxyfarm)")
	resp, err := f.client.Do(req)
	if err != nil {
		return "err:network", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("http_%d", resp.StatusCode), 0, fmt.Errorf("http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return "err:read", 0, err
	}

	nodes := parse.ParseMany(body) // handles base64 and plain bodies, dedups
	batchNodes := make([]natsb.RawNode, 0, f.batch)
	flush := func() error {
		if len(batchNodes) == 0 {
			return nil
		}
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s", src.ID, len(body), time.Now().UTC().Format(time.RFC3339))))
		msgID := "raw:" + hex.EncodeToString(sum[:8])
		batch := natsb.RawBatch{
			Event: "nodes.found", SourceID: src.ID,
			FetchedAt: time.Now().UTC().Format(time.RFC3339),
			Nodes:     batchNodes,
		}
		pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := f.bus.PublishRaw(pctx, msgID, batch); err != nil {
			return err
		}
		published += len(batchNodes)
		batchNodes = batchNodes[:0]
		return nil
	}
	for _, n := range nodes {
		batchNodes = append(batchNodes, natsb.RawNode{
			URI: n.URI, URIHash: n.URIHash, Protocol: n.Protocol,
			Transport: n.Transport, Host: n.Host, Port: n.Port,
		})
		if len(batchNodes) >= f.batch {
			if err := flush(); err != nil {
				return "err:publish", published, err
			}
		}
	}
	if err := flush(); err != nil {
		return "err:publish", published, err
	}
	return "ok", published, nil
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
