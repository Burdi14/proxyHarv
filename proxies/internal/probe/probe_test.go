package probe

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"proxyfarm/internal/parse"
)

func TestL0OpenPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	res := L0(context.Background(), "127.0.0.1", port, time.Second)
	if !res.OK {
		t.Fatalf("open port: %+v", res)
	}
}

func TestL0Refused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	time.Sleep(50 * time.Millisecond)
	res := L0(context.Background(), "127.0.0.1", port, time.Second)
	if res.OK || res.Code != CodeDialRefused {
		t.Fatalf("refused: %+v", res)
	}
}

func TestL0Timeout(t *testing.T) {
	// TEST-NET-3: guaranteed non-routable → timeout
	res := L0(context.Background(), "203.0.113.1", 81, 300*time.Millisecond)
	if res.OK || res.Code != CodeDialTimeout {
		t.Fatalf("timeout: %+v", res)
	}
}

func TestParseL1(t *testing.T) {
	r := parseL1("204 0.245", 0)
	if !r.OK || r.LatencyMs != 245 {
		t.Fatalf("204: %+v", r)
	}
	r = parseL1("200 0.100", 0)
	if r.OK || r.Code != CodeHTTPStatus {
		t.Fatalf("200: %+v", r)
	}
	r = parseL1("301 0.050", 0)
	if r.OK || r.Code != CodeHTTPStatus {
		t.Fatalf("301: %+v", r)
	}
	r = parseL1("", 28)
	if r.OK || r.Code != CodeDialTimeout {
		t.Fatalf("exit28: %+v", r)
	}
	r = parseL1("", 97)
	if r.OK || r.Code != CodeProtoHandshake {
		t.Fatalf("exit97: %+v", r)
	}
	r = parseL1("000 1.234", 35)
	if r.OK || r.Code != CodeTLS {
		t.Fatalf("exit35: %+v", r)
	}
	r = parseL1("garbage", 0)
	if r.OK || r.Code != CodeContentMismatch {
		t.Fatalf("garbage: %+v", r)
	}
}

func TestParseL2(t *testing.T) {
	// 3_125_000 bytes/sec = 25 mbps
	r := parseL2("3125000.000\n", 0, 25_000_000)
	if !r.OK || r.SpeedMbps < 24.99 || r.SpeedMbps > 25.01 {
		t.Fatalf("speed: %+v", r)
	}
	r = parseL2("", 28, 25_000_000)
	if r.OK || r.Code != CodeSlow {
		t.Fatalf("exit28 L2: %+v", r)
	}
	r = parseL2("", 56, 25_000_000)
	if r.OK || r.Code != CodeProtoHandshake {
		t.Fatalf("exit56 L2: %+v", r)
	}
	r = parseL2("0.000", 0, 25_000_000)
	if r.OK || r.Code != CodeContentMismatch {
		t.Fatalf("zero: %+v", r)
	}
	r = parseL2("", 7, 25_000_000)
	if r.OK || r.Code != CodeDialRefused {
		t.Fatalf("exit7: %+v", r)
	}
}

func TestBuildSingboxConfig(t *testing.T) {
	out := json.RawMessage(`{"type":"socks","server":"1.2.3.4","server_port":1080}`)
	cfg, err := buildSingboxConfig(out, 20555)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(cfg, &m); err != nil {
		t.Fatal(err)
	}
	if m["route"].(map[string]any)["final"] != "probe" {
		t.Fatal("route.final != probe")
	}
	inb := m["inbounds"].([]any)[0].(map[string]any)
	if inb["listen"] != "127.0.0.1" || int(inb["listen_port"].(float64)) != 20555 {
		t.Fatalf("inbound: %v", inb)
	}
	ob := m["outbounds"].([]any)[0].(map[string]any)
	if ob["tag"] != "probe" {
		t.Fatal("outbound tag not forced to probe")
	}
	if m["log"].(map[string]any)["level"] != "error" {
		t.Fatal("log level != error")
	}
}

// TestL1Integration probes one real uri through a real sing-box binary.
// Skipped unless PROXYFARM_SINGBOX is set (skip-on-CI per T-005 DoD).
func TestL1Integration(t *testing.T) {
	singbox := os.Getenv("PROXYFARM_SINGBOX")
	if singbox == "" {
		t.Skip("PROXYFARM_SINGBOX not set; skipping integration probe")
	}
	if _, err := exec.LookPath(singbox); err != nil {
		t.Skipf("sing-box not found at %s", singbox)
	}
	uri := os.Getenv("PROXYFARM_TEST_URI")
	if uri == "" {
		uri = "vless://eea1090b-71a9-42df-a617-c6c4ba5bc3d4@65.109.208.236:26795?security=none&type=tcp&packetEncoding=xudp&encryption=none#test"
	}
	node, err := parse.ParseLine(uri)
	if err != nil {
		t.Skipf("test uri unparsable: %v", err)
	}
	r := NewRunner(singbox, "curl", 2, DefaultProbeURLs(""), "", 25_000_000)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := r.ProbeL1(ctx, node.Outbound)
	t.Logf("L1 %+v", res)
}

func TestPortAllocator(t *testing.T) {
	pa := NewPortAllocator()
	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		p := pa.Acquire()
		if seen[p] {
			t.Fatalf("port %d reused", p)
		}
		if p < portMin || p > portMax {
			t.Fatalf("port %d out of range", p)
		}
		seen[p] = true
	}
	// drained allocator: releasing a port makes exactly it available again
	pa2 := NewPortAllocator()
	for i := 0; i < portMax-portMin+1; i++ {
		pa2.Acquire()
	}
	pa2.Release(20500)
	if got := pa2.Acquire(); got != 20500 {
		t.Fatalf("release/acquire mismatch: %d want 20500", got)
	}
}
