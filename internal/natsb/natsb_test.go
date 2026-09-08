package natsb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startNats(t *testing.T) string {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	port := 4321
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	name := fmt.Sprintf("proxyfarm-natsb-test-%d", port)
	_ = exec.Command("docker", "rm", "-f", name).Run()
	sd := t.TempDir()
	cmd := exec.Command("docker", "run", "--rm", "--name", name,
		"-p", fmt.Sprintf("%d:4222", port), "-v", sd+":/data",
		"nats:2.10-alpine", "-js", "-sd", "/data")
	if err := cmd.Start(); err != nil {
		t.Skipf("docker run failed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			return url
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("nats did not come up")
	return ""
}

func TestStreamsAndRoundtrip(t *testing.T) {
	url := startNats(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := Connect(url, "")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsureAllTiny(ctx, js); err != nil {
		t.Fatalf("EnsureAll: %v", err)
	}
	if err := EnsureAllTiny(ctx, js); err != nil { // idempotent
		t.Fatalf("EnsureAll twice: %v", err)
	}

	bus := NewBus(js)

	// interest retention: consumers must exist before publishing
	cons, err := EnsureConsumer(ctx, js, StreamRAW, DurableRaw, SubjectRaw)
	if err != nil {
		t.Fatal(err)
	}

	// RAW: publish with dedup msg-id → single delivery
	batch := RawBatch{Event: "nodes.found", SourceID: 1,
		FetchedAt: "2026-08-17T00:00:00Z",
		Nodes: []RawNode{{URI: "vless://u@1.2.3.4:443?security=none&type=tcp",
			URIHash: "sha256:aa", Protocol: "vless", Transport: "tcp", Host: "1.2.3.4", Port: 443}}}
	if err := bus.PublishRaw(ctx, "raw-dedupe-1", batch); err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishRaw(ctx, "raw-dedupe-1", batch); err != nil { // duplicate window 2m
		t.Fatal(err)
	}

	_, batches, err := FetchMsgs[RawBatch](cons, 10, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 {
		t.Fatalf("dedup: got %d batches, want 1", len(batches))
	}
	if batches[0].Nodes[0].Protocol != "vless" {
		t.Fatalf("roundtrip: %+v", batches[0])
	}

	// JOBS L1 → results (workqueue retention: consumer may exist before publish)
	wcons, err := EnsureConsumer(ctx, js, StreamJOBSL1, DurableWorkerL1, "jobs.l1.>")
	if err != nil {
		t.Fatal(err)
	}
	rcons, err := EnsureConsumer(ctx, js, StreamRESULTS, DurableResults, "results.>")
	if err != nil {
		t.Fatal(err)
	}
	job := JobBatch{JobID: "job-1", Stage: "L1", CreatedAt: "2026-08-17T00:00:00Z",
		Vantage: "cloud-eu-1",
		Nodes: []JobNode{{NodeID: 7, Outbound: json.RawMessage(`{"type":"socks","tag":"probe"}`)}}}
	if err := bus.PublishJobs(ctx, "L1", "cloud-eu-1", job); err != nil {
		t.Fatal(err)
	}
	rawMsgs, jobs, err := FetchMsgs[JobBatch](wcons, 5, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || len(jobs[0].Nodes) != 1 || jobs[0].Nodes[0].NodeID != 7 {
		t.Fatalf("jobs fetch: %+v", jobs)
	}
	res := ResultsBatch{JobID: "job-1", Vantage: "cloud-eu-1", Stage: "L1",
		Results: []ResultItem{{NodeID: 7, OK: true, LatencyMs: ptr(120), TS: "2026-08-17T00:00:05Z"}}}
	if err := bus.PublishResults(ctx, res); err != nil {
		t.Fatal(err)
	}
	for _, m := range rawMsgs {
		if err := m.Ack(); err != nil {
			t.Fatal(err)
		}
	}

	// results consumer sees it; duplicate publish filtered by msg-id
	if err := bus.PublishResults(ctx, res); err != nil {
		t.Fatal(err)
	}
	msgs2, results, err := FetchMsgs[ResultsBatch](rcons, 10, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results dedup: got %d, want 1", len(results))
	}
	if !results[0].Results[0].OK || *results[0].Results[0].LatencyMs != 120 {
		t.Fatalf("results roundtrip: %+v", results[0])
	}
	for _, m := range msgs2 {
		_ = m.Ack()
	}
}

func TestKVAndLease(t *testing.T) {
	url := startNats(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := Connect(url, "")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Drain()
	js, _ := jetstream.New(nc)
	if err := EnsureAllTiny(ctx, js); err != nil {
		t.Fatal(err)
	}
	bus := NewBus(js)

	if err := bus.PutPublish(ctx, 1, []byte(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := bus.PutPublish(ctx, 2, []byte(`{"version":2}`)); err != nil {
		t.Fatal(err)
	}
	v, val, err := bus.LatestPublish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v != 2 || string(val) != `{"version":2}` {
		t.Fatalf("latest: v=%d %s", v, val)
	}

	seen := make(chan int64, 4)
	go func() {
		_ = bus.WatchPublishes(ctx, func(ver int64, _ []byte) { seen <- ver })
	}()
	time.Sleep(300 * time.Millisecond)
	if err := bus.PutPublish(ctx, 3, []byte(`{"version":3}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-seen:
		if v < 1 {
			t.Fatalf("watch saw v%d", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not fire")
	}

	// lease: first wins, second not leader
	rel1, isLeader1, err := bus.TryAcquireLease(ctx, "coord-a", 10*time.Second, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer rel1()
	if !isLeader1() {
		t.Fatal("first coordinator should be leader")
	}
	rel2, isLeader2, err := bus.TryAcquireLease(ctx, "coord-b", 10*time.Second, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer rel2()
	if isLeader2() {
		t.Fatal("second coordinator must not be leader while lease healthy")
	}

	if err := bus.Heartbeat(ctx, "rf-home-1", "validator"); err != nil {
		t.Fatal(err)
	}
}

func TestMsgIDs(t *testing.T) {
	if MsgIDForJobs("j1") != "j1" {
		t.Fatal("jobs msg id")
	}
	if MsgIDForResults("j1", "rf-home-1", "L1") != "j1:rf-home-1:L1" {
		t.Fatal("results msg id")
	}
}

func ptr[T any](v T) *T { return &v }

var _ = os.Getenv
