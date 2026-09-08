// Command worker runs the stateless tester (cloud) or validator (edge).
// Role is chosen by flag; state lives in NATS only (§2).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"proxyfarm/internal/logx"
	"proxyfarm/internal/metrics"
	"proxyfarm/internal/natsb"
	"proxyfarm/internal/parse"
	"proxyfarm/internal/probe"
)

var log = logx.New("worker")

type config struct {
	role        string
	vantage     string
	natsURL     string
	natsCreds   string
	singbox     string
	curl        string
	probeDomain string
	concurrency int
	metricsAddr string
	dryRun      bool
	nodesFile   string
	limit       int
	batchL0     int
	batchL1     int
	batchL2     int
	l2Bytes     int64
	dialTimeout time.Duration
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := &config{}
	flag.StringVar(&cfg.role, "role", envOr("ROLE", "tester"), "tester|validator")
	flag.StringVar(&cfg.vantage, "vantage", envOr("VANTAGE", ""), "vantage id, e.g. cloud-eu-1 / rf-home-1")
	flag.StringVar(&cfg.natsURL, "nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "nats url")
	flag.StringVar(&cfg.natsCreds, "nats-creds", envOr("NATS_CREDS", ""), "nats credentials file")
	flag.StringVar(&cfg.singbox, "singbox", envOr("SINGBOX", "sing-box"), "sing-box binary path")
	flag.StringVar(&cfg.curl, "curl", envOr("CURL", "curl"), "curl binary path")
	flag.StringVar(&cfg.probeDomain, "probe-domain", envOr("PROBE_DOMAIN", ""), "probe.<domain> for generate_204")
	flag.IntVar(&cfg.concurrency, "concurrency", envInt("PROBE_CONCURRENCY", 32), "probe concurrency")
	flag.StringVar(&cfg.metricsAddr, "metrics-addr", envOr("METRICS_ADDR", ":9101"), "prometheus listen addr")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "local mode: probe nodes from --nodes, no NATS")
	flag.StringVar(&cfg.nodesFile, "nodes", "", "nodes file (newline-separated uris or JSON array)")
	flag.IntVar(&cfg.limit, "limit", 100, "dry-run: max nodes to L1-probe")
	flag.IntVar(&cfg.batchL0, "batch-l0", 100, "L0 fetch batch")
	flag.IntVar(&cfg.batchL1, "batch-l1", 50, "L1 fetch batch")
	flag.IntVar(&cfg.batchL2, "batch-l2", 10, "L2 fetch batch")
	flag.Int64Var(&cfg.l2Bytes, "l2-bytes", 25_000_000, "L2 download bytes")
	flag.DurationVar(&cfg.dialTimeout, "l0-timeout", 3*time.Second, "L0 dial timeout")
	flag.Parse()

	if cfg.role != "tester" && cfg.role != "validator" {
		log.Error("bad role", "role", cfg.role)
		os.Exit(2)
	}
	if cfg.vantage == "" {
		if cfg.role == "tester" {
			cfg.vantage = "cloud-eu-1"
		} else {
			log.Error("--vantage required for validator")
			os.Exit(2)
		}
	}

	if cfg.dryRun {
		runDry(cfg)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	if cfg.metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", reg.Handler())
		go func() {
			if err := http.ListenAndServe(cfg.metricsAddr, mux); err != nil {
				log.Error("metrics server stopped", "err", err.Error())
			}
		}()
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
	bus := natsb.NewBus(js)

	// heartbeat → REGISTRY (coordinator watches vantage liveness, §10)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			if err := bus.Heartbeat(ctx, cfg.vantage, cfg.role); err != nil {
				log.Warn("heartbeat failed", "err", err.Error())
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()

	runner := probe.NewRunner(cfg.singbox, cfg.curl, cfg.concurrency,
		probe.DefaultProbeURLs(cfg.probeDomain), "", cfg.l2Bytes)

	w := &runtimeWorker{cfg: cfg, bus: bus, js: js, runner: runner, reg: reg}
	w.run(ctx)
	log.Info("worker stopped", "vantage", cfg.vantage)
}

type runtimeWorker struct {
	cfg    *config
	bus    *natsb.Bus
	js     jetstream.JetStream
	runner *probe.Runner
	reg    *metrics.Registry
}

func (w *runtimeWorker) run(ctx context.Context) {
	if w.cfg.role == "tester" {
		w.consume(ctx, natsb.StreamJOBSL0, natsb.DurableWorkerL0, "jobs.l0.>", w.cfg.batchL0)
		w.consume(ctx, natsb.StreamJOBSL1, natsb.DurableWorkerL1, "jobs.l1.>", w.cfg.batchL1)
		w.consume(ctx, natsb.StreamJOBSL2, natsb.DurableWorkerL2, "jobs.l2.>", w.cfg.batchL2)
	} else {
		w.consume(ctx, natsb.StreamJOBSVAL, natsb.DurableForValidator(w.cfg.vantage),
			"jobs.validator."+w.cfg.vantage, w.cfg.batchL1)
	}
	<-ctx.Done()
}

func (w *runtimeWorker) attach(ctx context.Context, stream, durable, filter string) (jetstream.Consumer, error) {
	if c, err := w.js.Consumer(ctx, stream, durable); err == nil {
		return c, nil
	}
	return natsb.EnsureConsumer(ctx, w.js, stream, durable, filter)
}

func (w *runtimeWorker) consume(ctx context.Context, stream, durable, filter string, batch int) {
	go func() {
		// consumer creation belongs to coordinator; attach, create only as fallback
		var cons jetstream.Consumer
		var err error
		for attempt := 0; attempt < 60; attempt++ {
			cons, err = w.attach(ctx, stream, durable, filter)
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
		if err != nil {
			log.Error("consumer unavailable", "stream", stream, "err", err.Error())
			return
		}
		log.Info("consuming", "stream", stream, "durable", durable, "vantage", w.cfg.vantage)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msgs, jobs, err := natsb.FetchMsgs[natsb.JobBatch](cons, batch, 2*time.Second)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if !errors.Is(err, jetstream.ErrNoMessages) && err.Error() != "nats: timeout" {
					time.Sleep(time.Second)
				}
				continue
			}
			for i, job := range jobs {
				w.processJob(ctx, msgs[i], job)
			}
		}
	}()
}

func (w *runtimeWorker) processJob(ctx context.Context, ack jetstream.Msg, job natsb.JobBatch) {
	start := time.Now()
	nodes := job.Nodes
	// random order: don't look like a scanner drill (§9.4)
	rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })

	results := make([]natsb.ResultItem, len(nodes))
	sem := make(chan struct{}, w.cfg.concurrency)
	done := make(chan int, len(nodes))
	for i, node := range nodes {
		sem <- struct{}{}
		go func(i int, node natsb.JobNode) {
			defer func() { <-sem; done <- i }()
			results[i] = w.probeNode(ctx, job.Stage, node)
		}(i, node)
	}
	for range nodes {
		<-done
	}

	out := natsb.ResultsBatch{
		JobID:   job.JobID,
		Vantage: w.cfg.vantage,
		Stage:   job.Stage,
		Results: results,
	}
	pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := w.bus.PublishResults(pctx, out)
	cancel()
	if err != nil {
		// §18.8: results must be confirmed before acking the job
		log.Error("publish results failed; nak for redelivery", "job_id", job.JobID, "err", err.Error())
		w.reg.JobBatch(streamForStage(job.Stage), "nak")
		_ = ack.Nak()
		return
	}
	if err := ack.Ack(); err != nil {
		log.Warn("ack failed", "job_id", job.JobID, "err", err.Error())
	}
	w.reg.JobBatch(streamForStage(job.Stage), "ok")
	log.Info("job done", "job_id", job.JobID, "stage", job.Stage,
		"nodes", len(nodes), "ms", time.Since(start).Milliseconds())
}

func streamForStage(stage string) string {
	switch stage {
	case "L0":
		return "jobs.l0"
	case "L1":
		return "jobs.l1"
	case "L2":
		return "jobs.l2"
	}
	return "jobs.val"
}

func (w *runtimeWorker) probeNode(ctx context.Context, stage string, node natsb.JobNode) natsb.ResultItem {
	start := time.Now()
	var res probe.Result
	switch stage {
	case "L0":
		res = probe.L0(ctx, node.Host, node.Port, w.cfg.dialTimeout)
	case "L1":
		res = w.runner.ProbeL1(ctx, node.Outbound)
	case "L2":
		res = w.runner.ProbeL2(ctx, node.Outbound)
	default: // VAL batch: probe as L1 (L2 arrives as its own stage-2 batch)
		res = w.runner.ProbeL1(ctx, node.Outbound)
	}
	w.reg.ProbeDuration(stage, time.Since(start).Seconds())
	code := res.Code
	if code == "" {
		code = "ok"
	}
	w.reg.ProbeRequest(stage, w.cfg.vantage, code)

	item := natsb.ResultItem{
		NodeID: node.NodeID,
		OK:     res.OK,
		TS:     time.Now().UTC().Format(time.RFC3339),
	}
	if res.LatencyMs > 0 {
		ms := res.LatencyMs
		item.LatencyMs = &ms
	}
	if res.SpeedMbps > 0 {
		mb := res.SpeedMbps
		item.SpeedMbps = &mb
	}
	if !res.OK {
		c := res.Code
		if c == "" {
			c = probe.CodeInternal
		}
		item.ErrorCode = &c
	}
	log.Debug("probe done", "node_id", node.NodeID, "stage", stage,
		"ms", res.LatencyMs, "code", item.ErrorCode)
	return item
}

// ---- dry-run (M0 DoD: local check of configs from a file) ----

func runDry(cfg *config) {
	if cfg.nodesFile == "" {
		log.Error("--nodes required with --dry-run")
		os.Exit(2)
	}
	uris, err := readNodesFile(cfg.nodesFile)
	if err != nil {
		log.Error("read nodes", "err", err.Error())
		os.Exit(1)
	}
	if cfg.limit > 0 && len(uris) > cfg.limit {
		uris = uris[:cfg.limit]
	}
	log.Info("dry run", "nodes", len(uris), "vantage", cfg.vantage)

	runner := probe.NewRunner(cfg.singbox, cfg.curl, cfg.concurrency,
		probe.DefaultProbeURLs(cfg.probeDomain), "", cfg.l2Bytes)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	out := []map[string]any{}
	alive, dead := 0, 0
	for _, n := range uris {
		node, err := parse.ParseLine(n)
		if err != nil || node == nil {
			continue
		}
		entry := map[string]any{
			"node":  node.Host,
			"proto": node.Protocol, "transport": node.Transport,
		}
		if isTCPTransport(node.Transport) {
			l0 := probe.L0(ctx, node.Host, node.Port, cfg.dialTimeout)
			entry["l0"] = l0.Code
			if !l0.OK {
				dead++
				entry["l1"] = "skipped"
				out = append(out, entry)
				continue
			}
		}
		l1 := runner.ProbeL1(ctx, node.Outbound)
		if l1.OK {
			alive++
			entry["l1"] = l1.LatencyMs
		} else {
			dead++
			entry["l1"] = l1.Code
		}
		out = append(out, entry)
	}
	summary := map[string]any{
		"checked": len(out), "l1_ok": alive, "failed": dead,
		"vantage": cfg.vantage, "ts": time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.MarshalIndent(summary, "", " ")
	fmt.Println(string(b))
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	_ = enc.Encode(out)
}

func isTCPTransport(t string) bool {
	switch t {
	case "tcp", "ws", "grpc", "http", "httpupgrade", "xhttp", "kcp":
		return true
	}
	return false
}

func readNodesFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	uris := []string{}
	ext := filepath.Ext(path)
	if ext == ".json" {
		var arr []string
		var obj struct {
			URIs  []string `json:"uris"`
			Nodes []struct {
				URI string `json:"uri"`
			} `json:"nodes"`
		}
		if err := json.Unmarshal(data, &arr); err == nil {
			uris = arr
		} else if err := json.Unmarshal(data, &obj); err == nil {
			for _, u := range obj.URIs {
				uris = append(uris, u)
			}
			for _, n := range obj.Nodes {
				uris = append(uris, n.URI)
			}
		}
	}
	if len(uris) == 0 {
		for _, line := range splitLines(string(data)) {
			uris = append(uris, line)
		}
	}
	// filter to parseable share-links
	out := make([]string, 0, len(uris))
	seen := map[string]bool{}
	for _, u := range uris {
		n, err := parse.ParseLine(u)
		if err != nil || n == nil || seen[n.URIHash] {
			continue
		}
		seen[n.URIHash] = true
		out = append(out, n.URI)
	}
	return out, nil
}

func splitLines(s string) []string {
	out := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := trimCR(s[start:i])
			if line != "" {
				out = append(out, line)
			}
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, trimCR(s[start:]))
	}
	return out
}

func trimCR(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\r' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	for len(s) > 0 && (s[0] == '\r' || s[0] == ' ') {
		s = s[1:]
	}
	return s
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}
