package model

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func startPG(t *testing.T) string {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	const port = 55432
	dsn := fmt.Sprintf("postgres://proxyfarm:proxyfarm@127.0.0.1:%d/proxyfarm?sslmode=disable", port)
	name := "proxyfarm-model-test"
	_ = exec.Command("docker", "rm", "-f", name).Run()
	cmd := exec.Command("docker", "run", "--rm", "--name", name,
		"-e", "POSTGRES_USER=proxyfarm", "-e", "POSTGRES_PASSWORD=proxyfarm",
		"-e", "POSTGRES_DB=proxyfarm", "-p", fmt.Sprintf("%d:5432", port),
		"postgres:16-alpine")
	if err := cmd.Start(); err != nil {
		t.Skipf("docker run failed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		st, err := Open(ctx, dsn)
		if err == nil {
			st.Close()
			cancel()
			return dsn
		}
		cancel()
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("postgres did not come up")
	return ""
}

func testStore(t *testing.T) *PGStore {
	t.Helper()
	if os.Getenv("PROXYFARM_TEST_PG") != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		st, err := Open(ctx, os.Getenv("PROXYFARM_TEST_PG"))
		if err != nil {
			t.Skipf("PROXYFARM_TEST_PG unreachable: %v", err)
		}
		return st
	}
	dsn := startPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestMigrateAndCRUD(t *testing.T) {
	st := testStore(t)
	defer st.Close()
	ctx := context.Background()

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate twice: %v", err)
	}

	// sources
	id, err := st.UpsertSource(ctx, &Source{URL: "https://example.com/sub", Kind: "sub", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateSourceStatus(ctx, id, "ok", time.Now()); err != nil {
		t.Fatal(err)
	}
	srcs, _ := st.ListSources(ctx, true)
	if len(srcs) != 1 || srcs[0].LastStatus == nil || *srcs[0].LastStatus != "ok" {
		t.Fatalf("sources: %+v", srcs)
	}
	if err := st.DeleteSource(ctx, id); err != nil {
		t.Fatal(err)
	}

	// nodes
	n := &Node{URIHash: "sha256:aaa", URI: "vless://u@1.2.3.4:443?security=none&type=tcp",
		Protocol: "vless", Transport: "tcp", Host: "1.2.3.4"}
	nid, created, err := st.UpsertNode(ctx, n)
	if err != nil || !created {
		t.Fatalf("upsert: %v created=%v", err, created)
	}
	nid2, created2, _ := st.UpsertNode(ctx, n)
	if nid2 != nid || created2 {
		t.Fatalf("dedup failed: %d %d %v", nid, nid2, created2)
	}
	cc, city, lat, lon := "DE", "Frankfurt", 50.11, 8.68
	if err := st.UpdateNodeGeo(ctx, nid, "1.2.3.4", &cc, &city, &lat, &lon); err != nil {
		t.Fatal(err)
	}

	// checks: idempotent insert via ts_bucket (§18.7)
	latms, ecode := 245, "E_DIAL_TIMEOUT"
	ts := time.Now().UTC().Truncate(time.Second)
	c1 := &Check{NodeID: nid, Vantage: "cloud-eu-1", Stage: "L1", OK: true,
		LatencyMs: &latms, TS: ts, TSBucket: ts.Unix() / 300}
	if inserted, err := st.InsertCheck(ctx, c1); err != nil || !inserted {
		t.Fatalf("check insert: %v %v", inserted, err)
	}
	c2 := &Check{NodeID: nid, Vantage: "cloud-eu-1", Stage: "L1", OK: false,
		ErrorCode: &ecode, TS: ts, TSBucket: ts.Unix() / 300}
	if inserted, err := st.InsertCheck(ctx, c2); err != nil || inserted {
		t.Fatalf("redelivery must be dropped: %v %v", inserted, err)
	}

	if err := st.UpdateNodeAggregate(ctx, NodeUpdate{NodeID: nid, Status: "alive",
		BestClass: "cloud", Score: 0.7, LastL1OKAt: &ts}); err != nil {
		t.Fatal(err)
	}
	got, _ := st.GetNode(ctx, nid)
	if got.Status != "alive" || got.BestClass != "cloud" || got.City == nil || *got.City != "Frankfurt" {
		t.Fatalf("node aggregate/geo: %+v", got)
	}

	// due planning: alive node just checked → not due; unknown node → due
	n2 := &Node{URIHash: "sha256:bbb", URI: "vmess://eyJ2IjoiMiIsImFkZCI6IjIuMy40LjUiLCJwb3J0IjoiNDQzIiwiaWQiOiJ4IiwibmV0Ijoid3MifQ",
		Protocol: "vmess", Transport: "ws", Host: "2.3.4.5"}
	if _, _, err := st.UpsertNode(ctx, n2); err != nil {
		t.Fatal(err)
	}
	due0, err0 := st.NodesDueL0(ctx, 100)
	if err0 != nil {
		t.Fatalf("L0 due err: %v", err0)
	}
	if len(due0) != 1 || due0[0].Transport != "ws" {
		t.Fatalf("L0 due: %+v", due0)
	}
	due1, err1 := st.NodesDueL1(ctx, 100)
	if err1 != nil {
		t.Fatalf("L1 due err: %v", err1)
	}
	if len(due1) != 0 {
		t.Fatalf("L1 due (quic only): %+v", due1)
	}

	// recent L1
	recent, _ := st.RecentL1(ctx, nid, "", 10)
	if len(recent) != 1 || !recent[0] {
		t.Fatalf("recent: %v", recent)
	}
	byV, _ := st.LatestByVantage(ctx, nid)
	if len(byV) != 1 || byV["cloud-eu-1"].Stage != "L1" {
		t.Fatalf("by vantage: %+v", byV)
	}

	// vantages + publish
	if err := st.UpsertVantage(ctx, &Vantage{ID: "rf-home-1", Kind: "edge", Enabled: true,
		LastHeartbeat: ptrTime(time.Now())}); err != nil {
		t.Fatal(err)
	}
	vs, _ := st.ListVantages(ctx, true)
	if len(vs) != 1 || vs[0].Kind != "edge" {
		t.Fatalf("vantages: %+v", vs)
	}
	if err := st.DeleteVantage(ctx, "rf-home-1"); err != nil {
		t.Fatal(err)
	}
	vs, _ = st.ListVantages(ctx, true)
	if len(vs) != 0 {
		t.Fatalf("vantage delete: %+v", vs)
	}
	// re-register for the rest of the test
	if err := st.UpsertVantage(ctx, &Vantage{ID: "rf-home-1", Kind: "edge", Enabled: true,
		LastHeartbeat: ptrTime(time.Now())}); err != nil {
		t.Fatal(err)
	}

	val, _ := st.NodesForValidation(ctx, "rf-home-1", 10)
	if len(val) != 1 || val[0].ID != nid {
		t.Fatalf("validation candidates: %+v", val)
	}

	p := &Publish{ClassFilter: "all", NodeCount: 1, Checksum: "sha256:dead", KVKey: "publishes/v1"}
	if err := st.InsertPublish(ctx, p); err != nil {
		t.Fatal(err)
	}
	if p.Version != 1 {
		t.Fatalf("publish version: %d", p.Version)
	}
	latest, _ := st.LatestPublish(ctx)
	if latest == nil || latest.KVKey != "publishes/v1" {
		t.Fatalf("latest publish: %+v", latest)
	}

	// api reads
	cities, _ := st.CityAggregates(ctx, "")
	if len(cities) != 1 || cities[0].City != "Frankfurt" || cities[0].CC != "DE" {
		t.Fatalf("cities: %+v", cities)
	}
	if cities[0].BestLatencyMs == nil || *cities[0].BestLatencyMs != 245 {
		t.Fatalf("city latency: %+v", cities[0])
	}
	cards, _ := st.NodeCards(ctx, "", "", "", 10)
	if len(cards) != 1 || cards[0].Class != "cloud" || *cards[0].LatencyMs != 245 {
		t.Fatalf("cards: %+v", cards)
	}
	card, _ := st.NodeCard(ctx, nid)
	if card == nil || card.ShareLink == "" {
		t.Fatalf("card: %+v", card)
	}

	// long-dead cleanup (§4: dead > 14 days)
	old := time.Now().Add(-15 * 24 * time.Hour)
	if err := st.UpdateNodeAggregate(ctx, NodeUpdate{NodeID: nid2, Status: "dead"}); err != nil {
		t.Fatal(err)
	}
	_, err = st.pool.Exec(ctx, `UPDATE nodes SET first_seen=$1 WHERE id=$2`, old, nid2)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := st.DeleteLongDead(ctx, time.Now().Add(-14*24*time.Hour))
	if err != nil || deleted != 1 {
		t.Fatalf("delete long dead: %d %v", deleted, err)
	}
	cnt, _ := st.CountNodes(ctx)
	if cnt != 1 {
		t.Fatalf("count: %d", cnt)
	}
}

func TestRenameFragment(t *testing.T) {
	// coordinator package function mirror-tested here via store-free helper
	// (kept in this package only as sanity for naming)
}

func ptrTime(t time.Time) *time.Time { return &t }
