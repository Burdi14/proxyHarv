package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"proxyfarm/internal/model"
	"proxyfarm/internal/natsb"
)

func startDeps(t *testing.T) (store *model.PGStore, bus *natsb.Bus) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	const pgPort = 55433
	const natsPort = 4322
	pgName, natsName := "proxyfarm-api-test-pg", "proxyfarm-api-test-nats"
	cleanup := func() {
		_ = exec.Command("docker", "rm", "-f", pgName).Run()
		_ = exec.Command("docker", "rm", "-f", natsName).Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	pgDSN := fmt.Sprintf("postgres://proxyfarm:proxyfarm@127.0.0.1:%d/proxyfarm?sslmode=disable", pgPort)
	natsURL := fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
	sd := t.TempDir()
	if err := exec.Command("docker", "run", "-d", "--name", pgName,
		"-e", "POSTGRES_USER=proxyfarm", "-e", "POSTGRES_PASSWORD=proxyfarm",
		"-e", "POSTGRES_DB=proxyfarm", "-p", fmt.Sprintf("%d:5432", pgPort),
		"postgres:16-alpine").Run(); err != nil {
		t.Skipf("docker pg: %v", err)
	}
	if err := exec.Command("docker", "run", "-d", "--name", natsName,
		"-p", fmt.Sprintf("%d:4222", natsPort), "-v", sd+":/data",
		"nats:2.10-alpine", "-js", "-sd", "/data").Run(); err != nil {
		t.Skipf("docker nats: %v", err)
	}

	ctx := context.Background()
	var st *model.PGStore
	for i := 0; i < 60; i++ {
		if s, err := model.Open(ctx, pgDSN); err == nil {
			st = s
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if st == nil {
		t.Fatal("pg did not come up")
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	var nc *nats.Conn
	for i := 0; i < 60; i++ {
		if c, err := nats.Connect(natsURL); err == nil {
			nc = c
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if nc == nil {
		t.Fatal("nats did not come up")
	}
	t.Cleanup(func() { nc.Drain() })
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := natsb.EnsureAllTiny(ctx, js); err != nil {
		t.Fatal(err)
	}
	return st, natsb.NewBus(js)
}

func seed(t *testing.T, store *model.PGStore) (rfID, cloudID int64) {
	t.Helper()
	ctx := context.Background()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	de := "DE"
	fra := "Frankfurt"
	lat, lon := 50.11, 8.68
	ts := time.Now().UTC()

	mk := func(hash, uri string, class string) int64 {
		n := &model.Node{URIHash: hash, URI: uri, Protocol: "vless", Transport: "tcp",
			Host: "1.2.3.4", CC: &de, City: &fra, Lat: &lat, Lon: &lon,
			Status: "alive", BestClass: class, Score: 0.5}
		id, created, err := store.UpsertNode(ctx, n)
		must(err)
		if created {
			must(store.UpdateNodeGeo(ctx, id, "1.2.3.4", &de, &fra, &lat, &lon))
			must(store.UpdateNodeAggregate(ctx, model.NodeUpdate{NodeID: id, Status: "alive",
				BestClass: class, Score: 0.5, LastL1OKAt: &ts}))
		}
		latms := 94
		_, err = store.InsertCheck(ctx, &model.Check{NodeID: id, Vantage: "cloud-eu-1",
			Stage: "L1", OK: true, LatencyMs: &latms, TS: ts, TSBucket: ts.Unix() / 300})
		must(err)
		return id
	}
	rfID = mk("sha256:rf1", "vless://uuid-rf@1.2.3.4:443?security=none&type=tcp#x", "rf")
	cloudID = mk("sha256:cl1", "vless://uuid-cl@5.6.7.8:443?security=none&type=tcp#y", "cloud")
	must(store.InsertPublish(ctx, &model.Publish{ClassFilter: "all", NodeCount: 2,
		Checksum: "sha256:beef", KVKey: "publishes/v1"}))
	return rfID, cloudID
}

func TestAPIEndpoints(t *testing.T) {
	store, bus := startDeps(t)
	defer store.Close()
	rfID, _ := seed(t, store)

	app := &apiApp{store: store, bus: bus, adminToken: "sectest", subInterval: 6}
	srv := httptest.NewServer(app.routes("testdata-nope"))
	defer srv.Close()
	client := srv.Client()
	client.Timeout = 5 * time.Second

	get := func(path string) (*http.Response, string) {
		t.Helper()
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, string(b)
	}

	// /healthz
	resp, body := get("/healthz")
	if resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("healthz: %d %q", resp.StatusCode, body)
	}

	// /api/cities?class=rf (§14.7 shape)
	resp, body = get("/api/cities?class=rf")
	if resp.StatusCode != 200 {
		t.Fatalf("cities: %d", resp.StatusCode)
	}
	var cities struct {
		Cities []struct {
			City          string   `json:"city"`
			CC            string   `json:"cc"`
			Lat           float64  `json:"lat"`
			Lon           float64  `json:"lon"`
			Count         int      `json:"count"`
			BestLatencyMs *int     `json:"best_latency_ms"`
			Protocols     []string `json:"protocols"`
		} `json:"cities"`
	}
	if err := json.Unmarshal([]byte(body), &cities); err != nil {
		t.Fatalf("cities json: %v", err)
	}
	if len(cities.Cities) != 1 {
		t.Fatalf("cities: %s", body)
	}
	c := cities.Cities[0]
	if c.City != "Frankfurt" || c.CC != "DE" || c.Count != 1 ||
		c.BestLatencyMs == nil || *c.BestLatencyMs != 94 || len(c.Protocols) != 1 {
		t.Fatalf("city agg: %+v", c)
	}

	// bad class → 400
	resp, _ = get("/api/cities?class=nope")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad class: %d", resp.StatusCode)
	}

	// /api/nodes
	resp, body = get("/api/nodes?city=Frankfurt&class=rf")
	if resp.StatusCode != 200 || !strings.Contains(body, `"id"`) {
		t.Fatalf("nodes: %d %s", resp.StatusCode, body)
	}
	if strings.Contains(body, "vless://") {
		t.Fatalf("nodes list must not leak share links: %s", body)
	}

	// /api/node/{id} (§14.7 shape)
	resp, body = get(fmt.Sprintf("/api/node/%d", rfID))
	if resp.StatusCode != 200 {
		t.Fatalf("node: %d", resp.StatusCode)
	}
	var node map[string]any
	if err := json.Unmarshal([]byte(body), &node); err != nil {
		t.Fatal(err)
	}
	if node["share_link"] == nil || node["share_link"] == "" {
		t.Fatalf("node detail must include share_link: %s", body)
	}
	if ob, ok := node["outbound"].(map[string]any); !ok || ob["type"] != "vless" {
		t.Fatalf("node outbound: %v", node["outbound"])
	}
	resp, _ = get("/api/node/999999")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing node: %d", resp.StatusCode)
	}

	// /api/sub (§14.7): base64 body, Profile-Update-Interval: 6
	resp, body = get("/api/sub?class=rf")
	if resp.StatusCode != 200 {
		t.Fatalf("sub: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("sub content-type: %s", ct)
	}
	if resp.Header.Get("Profile-Update-Interval") != "6" {
		t.Fatalf("sub interval header: %q", resp.Header.Get("Profile-Update-Interval"))
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(body))
	if err != nil {
		t.Fatalf("sub not std base64: %v", err)
	}
	if !strings.Contains(string(decoded), "vless://") {
		t.Fatalf("sub body: %q", decoded)
	}
	if !strings.Contains(string(decoded), "DE-Frankfurt-94ms-rf") {
		t.Fatalf("sub names must be {CC}-{City}-{latency}ms-{class}: %q", decoded)
	}

	// /api/publishes
	resp, body = get("/api/publishes")
	if resp.StatusCode != 200 || !strings.Contains(body, `"kv_key": "publishes/v1"`) {
		t.Fatalf("publishes: %d %s", resp.StatusCode, body)
	}

	// admin: no token → 401; disabled when token empty
	resp, _ = get("/api/admin/sources")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin no token: %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/admin/sources", nil)
	req.Header.Set("X-Admin-Token", "sectest")
	resp2, err := client.Do(req)
	if err != nil || resp2.StatusCode != 200 {
		t.Fatalf("admin with token: %v %d", err, resp2.StatusCode)
	}
	resp2.Body.Close()

	// admin create/delete source
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/api/admin/sources", strings.NewReader(`{"url":"https://example.com/s"}`))
	req.Header.Set("X-Admin-Token", "sectest")
	resp2, err = client.Do(req)
	if err != nil || resp2.StatusCode != http.StatusCreated {
		t.Fatalf("admin post: %v %d", err, resp2.StatusCode)
	}
	resp2.Body.Close()
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/api/admin/sources/1", nil)
	req.Header.Set("X-Admin-Token", "sectest")
	resp2, err = client.Do(req)
	if err != nil || resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("admin delete: %v %d", err, resp2.StatusCode)
	}
	resp2.Body.Close()
}

func TestSSEPublishEvent(t *testing.T) {
	store, bus := startDeps(t)
	defer store.Close()
	ctx := context.Background()

	app := &apiApp{store: store, bus: bus, adminToken: "", subInterval: 6}
	srv := httptest.NewServer(app.routes("testdata-nope"))
	defer srv.Close()

	// open SSE stream
	resp, err := srv.Client().Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("sse content type: %s", ct)
	}

	// emit a publish → expect "event: publish"
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = bus.PutPublish(ctx, 7, []byte(`{"version":7,"node_count":1}`))
	}()

	scanner := bufio.NewScanner(resp.Body)
	got := make(chan string, 4)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "data:") {
				got <- line
			}
		}
	}()
	deadline := time.After(8 * time.Second)
	var event, data string
	for event == "" || data == "" {
		select {
		case line := <-got:
			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			}
			if strings.HasPrefix(line, "data:") {
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		case <-deadline:
			t.Fatalf("no publish event; got event=%q data=%q", event, data)
		}
	}
	if event != "publish" || !strings.Contains(data, `"version":7`) {
		t.Fatalf("sse payload: %q %q", event, data)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
