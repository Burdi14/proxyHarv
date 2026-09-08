package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"proxyfarm/internal/geo"
	"proxyfarm/internal/model"
	"proxyfarm/internal/natsb"
	"proxyfarm/internal/parse"
	"proxyfarm/internal/score"
)

type coord struct {
	store model.Store
	bus   *natsb.Bus
	js    jetstream.JetStream
	loc   *geo.Locator
	cfg   *config
}

// consumeRAW: share-link batches → nodes table (+ geo for new hosts).
func (c *coord) consumeRaw(ctx context.Context) {
	cons, err := natsb.EnsureConsumer(ctx, c.js, natsb.StreamRAW, natsb.DurableRaw, natsb.SubjectRaw)
	if err != nil {
		log.Error("raw consumer", "err", err.Error())
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, batches, err := natsb.FetchMsgs[natsb.RawBatch](cons, 10, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for i, batch := range batches {
			c.ingestBatch(ctx, batch)
			_ = msgs[i].Ack()
		}
	}
}

func (c *coord) ingestBatch(ctx context.Context, batch natsb.RawBatch) {
	for _, rn := range batch.Nodes {
		node := &model.Node{
			URIHash: rn.URIHash, URI: rn.URI, Protocol: rn.Protocol,
			Transport: rn.Transport, Host: rn.Host,
		}
		id, created, err := c.store.UpsertNode(ctx, node)
		if err != nil {
			log.Warn("upsert node", "err", err.Error())
			continue
		}
		if created {
			c.locateNode(ctx, id, rn.Host)
		}
	}
}

func (c *coord) locateNode(ctx context.Context, id int64, host string) {
	if c.loc == nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ip, g := c.loc.Locate(cctx, host)
	var cc, city *string
	var lat, lon *float64
	if g.OK {
		ccv, cityv, latv, lonv := g.CC, g.City, g.Lat, g.Lon
		cc, city, lat, lon = &ccv, &cityv, &latv, &lonv
	}
	if err := c.store.UpdateNodeGeo(ctx, id, ip, cc, city, lat, lon); err != nil {
		log.Warn("update geo", "node_id", id, "err", err.Error())
	}
}

// consumeResults: RESULTS → node_checks (idempotent §18.7) → aggregate.
func (c *coord) consumeResults(ctx context.Context) {
	cons, err := natsb.EnsureConsumer(ctx, c.js, natsb.StreamRESULTS, natsb.DurableResults, "results.>")
	if err != nil {
		log.Error("results consumer", "err", err.Error())
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, batches, err := natsb.FetchMsgs[natsb.ResultsBatch](cons, 10, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for i, batch := range batches {
			c.ingestResults(ctx, batch)
			_ = msgs[i].Ack()
		}
	}
}

func (c *coord) ingestResults(ctx context.Context, batch natsb.ResultsBatch) {
	for _, r := range batch.Results {
		ts := time.Now().UTC()
		if t, err := time.Parse(time.RFC3339, r.TS); err == nil {
			ts = t
		}
		chk := &model.Check{
			NodeID: r.NodeID, Vantage: batch.Vantage, Stage: batch.Stage,
			OK: r.OK, LatencyMs: r.LatencyMs, SpeedMbps: r.SpeedMbps,
			ErrorCode: r.ErrorCode, TS: ts, TSBucket: ts.Unix() / 300,
		}
		inserted, err := c.store.InsertCheck(ctx, chk)
		if err != nil {
			log.Warn("insert check", "node_id", r.NodeID, "err", err.Error())
			continue
		}
		if !inserted {
			continue // duplicate redelivery
		}
		if err := c.recomputeNode(ctx, r.NodeID); err != nil {
			log.Warn("recompute", "node_id", r.NodeID, "err", err.Error())
		}
		// L0 pass → queue the full L1 probe (§4)
		if batch.Stage == "L0" && r.OK {
			c.enqueueL1(ctx, []int64{r.NodeID})
		}
	}
}

// recomputeNode updates status/score/class from check history.
func (c *coord) recomputeNode(ctx context.Context, nodeID int64) error {
	node, err := c.store.GetNode(ctx, nodeID)
	if err != nil || node == nil {
		return err
	}
	byVantage, err := c.store.LatestByVantage(ctx, nodeID)
	if err != nil {
		return err
	}

	status := "dead"
	var lastL1OK *time.Time
	var bestLatency int
	var bestSpeed float64
	rfOK := false
	cloudOK := false
	for vantage, chk := range byVantage {
		if chk.Stage == "L1" && chk.OK {
			if lastL1OK == nil || chk.TS.After(*lastL1OK) {
				t := chk.TS
				lastL1OK = &t
				if chk.LatencyMs != nil {
					bestLatency = *chk.LatencyMs
				}
			}
			if isEdge(vantage) {
				rfOK = true
			} else {
				cloudOK = true
			}
			status = "alive"
		}
		if chk.Stage == "L2" && chk.OK {
			if chk.SpeedMbps != nil && *chk.SpeedMbps > bestSpeed {
				bestSpeed = *chk.SpeedMbps
			}
		}
	}
	bestClass := "none"
	switch {
	case rfOK:
		bestClass = "rf"
	case cloudOK:
		bestClass = "cloud"
	}

	recent, err := c.store.RecentL1(ctx, nodeID, "", 10)
	if err != nil {
		return err
	}
	var recentAny []bool
	// prefer the deciding vantage (rf when present)
	for vantage := range byVantage {
		if vantageHist, err := c.store.RecentL1(ctx, nodeID, vantage, 10); err == nil && len(vantageHist) > 0 {
			recentAny = vantageHist
			break
		}
	}
	if recentAny == nil {
		recentAny = recent
	}

	in := score.Input{
		Now: time.Now(), LastL1OKAt: derefTime(lastL1OK),
		RecentL1: recentAny, LatencyMs: bestLatency,
		SpeedMbps: bestSpeed, RFValidated: rfOK,
	}
	newScore := score.Compute(in, c.cfg.weights)

	update := model.NodeUpdate{
		NodeID: nodeID, Status: status, BestClass: bestClass,
		Score: newScore, LastL1OKAt: lastL1OK,
	}
	return c.store.UpdateNodeAggregate(ctx, update)
}

func isEdge(vantage string) bool { return len(vantage) >= 3 && vantage[:3] == "rf-" }

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// enqueueL1 publishes an L1 job for the given node ids.
func (c *coord) enqueueL1(ctx context.Context, ids []int64) {
	nodes := make([]natsb.JobNode, 0, len(ids))
	for _, id := range ids {
		n, err := c.store.GetNode(ctx, id)
		if err != nil || n == nil {
			continue
		}
		parsed, err := parse.ParseLine(n.URI)
		if err != nil || parsed == nil {
			continue
		}
		nodes = append(nodes, natsb.JobNode{NodeID: id, Outbound: parsed.Outbound})
	}
	if len(nodes) == 0 {
		return
	}
	job := natsb.JobBatch{
		JobID: newUUID(), Stage: "L1", CreatedAt: nowRFC3339(),
		Vantage: "cloud-eu-1", Nodes: nodes,
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.bus.PublishJobs(pctx, "L1", "cloud-eu-1", job); err != nil {
		log.Warn("publish L1", "err", err.Error())
	}
}

// watchVantages: REGISTRY heartbeats → vantages table + validator consumers.
func (c *coord) watchVantages(ctx context.Context) {
	kv, err := c.js.KeyValue(ctx, natsb.BucketREGISTRY)
	if err != nil {
		log.Error("registry kv", "err", err.Error())
		return
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		log.Error("registry watch", "err", err.Error())
		return
	}
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-w.Updates():
			if !ok {
				return
			}
			if entry == nil || entry.Operation() != jetstream.KeyValuePut {
				continue
			}
			key := entry.Key()
			if len(key) < 8 || key[:8] != "vantage." {
				continue
			}
			var hb struct {
				Vantage string `json:"vantage"`
				Role    string `json:"role"`
				TS      string `json:"ts"`
			}
			if err := json.Unmarshal(entry.Value(), &hb); err != nil {
				continue
			}
			ts, _ := time.Parse(time.RFC3339, hb.TS)
			kind := "cloud"
			if hb.Role == "validator" {
				kind = "edge"
			}
			v := &model.Vantage{ID: hb.Vantage, Kind: kind, Enabled: true, LastHeartbeat: &ts}
			if err := c.store.UpsertVantage(ctx, v); err != nil {
				log.Warn("upsert vantage", "err", err.Error())
				continue
			}
			if kind == "edge" {
				if _, err := natsb.EnsureConsumer(ctx, c.js, natsb.StreamJOBSVAL,
					natsb.DurableForValidator(hb.Vantage), "jobs.validator."+hb.Vantage+".>"); err != nil {
					log.Warn("validator consumer", "err", err.Error())
				}
			}
			log.Info("vantage heartbeat", "vantage", hb.Vantage, "kind", kind)
		}
	}
}

func (c *coord) serveMetrics(ctx context.Context) {
	if c.cfg.metricsAddr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", c.cfg.reg.Handler())
	srv := &http.Server{Addr: c.cfg.metricsAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("metrics server", "err", err.Error())
	}
}

func newUUID() string {
	b := make([]byte, 16)
	if _, err := readRandom(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // v4
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
