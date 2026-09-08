package parse

import (
	"encoding/json"
	"os"
	"testing"
)

type goldenCase struct {
	URI      string `json:"uri"`
	Canonical string `json:"canonical"`
	URIHash  string `json:"uri_hash"`
}

// TestGoldenURIHash runs the 22 golden fixtures from fixtures/golden_uri_hash.json
// (normative §14.2 / §19: canonical and hash must match character-for-character).
func TestGoldenURIHash(t *testing.T) {
	b, err := os.ReadFile("../../fixtures/golden_uri_hash.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g struct {
		Comment string       `json:"_comment"`
		Cases   []goldenCase `json:"cases"`
	}
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(g.Cases) != 22 {
		t.Fatalf("expected 22 golden cases, got %d", len(g.Cases))
	}
	for i, c := range g.Cases {
		canonical, hash, err := CanonicalHash(c.URI)
		if err != nil {
			t.Fatalf("case %d (%s…): %v", i+1, c.URI[:24], err)
		}
		if canonical != c.Canonical {
			t.Errorf("case %d canonical mismatch:\n got %q\nwant %q", i+1, canonical, c.Canonical)
		}
		if hash != c.URIHash {
			t.Errorf("case %d hash mismatch: got %s want %s", i+1, hash, c.URIHash)
		}
	}
}

// TestParseLines checks every sample uri parses into protocol/transport/host/port
// and produces a sing-box outbound with tag "probe".
func TestParseLines(t *testing.T) {
	perProtocol := map[string]int{}
	for uri, want := range map[string]struct{ proto, transport string }{
		"vless://47fcef29-ab4e-4aa6-932b-d95a18f28a4e@166.62.111.8:443?security=tls&type=ws&path=/&host=h.example.com&sni=h.example.com&fp=chrome#x": {"vless", "ws"},
		"ss://YWVzLTEyOC1nY206c2hhZG93c29ja3M=@37.19.198.160:443#x": {"ss", "tcp"},
		"trojan://pw@cf.0sm.com:443?sni=yks.pages.dev&type=grpc&serviceName=grpc-svc#x": {"trojan", "grpc"},
		"hysteria2://pw@94.20.88.13:8443?insecure=1&sni=s.example.com#x":               {"hysteria2", "quic"},
		"socks://u:p@1.2.3.4:1080#x":                                                     {"socks", "tcp"},
		"anytls://f93d0a97-c3a8-47af-b2be-ea4904c9918b@jjz.jjznodenode.top:17966?security=tls&insecure=0&allowInsecure=0&type=tcp&headerType=none#x": {"anytls", "tcp"},
		"vmess://eyJ2IjoiMiIsInBzIjoiIiwiYWRkIjoiMS4yLjMuNCIsInBvcnQiOiI0NDMiLCJpZCI6ImI1ZTk0ODBhLWI3YWEtNDBhNC1mOWE3LTUyOTliNWUzNjNiNCIsImFpZCI6IjAiLCJzY3kiOiJhdXRvIiwibmV0Ijoid3MiLCJ0eXBlIjoiIiwidGxzIjoiIiwicGF0aCI6Ii8iLCJob3N0IjoiMS4yLjMuNCJ9": {"vmess", "ws"},
	} {
		n, err := ParseLine(uri)
		if err != nil {
			t.Fatalf("ParseLine(%s…): %v", uri[:20], err)
		}
		if n.Protocol != want.proto {
			t.Errorf("%s…: protocol=%s want %s", uri[:20], n.Protocol, want.proto)
		}
		if n.Transport != want.transport {
			t.Errorf("%s…: transport=%s want %s", uri[:20], n.Transport, want.transport)
		}
		if n.Outbound == nil {
			t.Fatalf("%s…: nil outbound", uri[:20])
		}
		var out map[string]any
		if err := json.Unmarshal(n.Outbound, &out); err != nil {
			t.Fatalf("%s…: outbound json: %v", uri[:20], err)
		}
		if out["tag"] != "probe" {
			t.Errorf("%s…: outbound tag=%v want probe", uri[:20], out["tag"])
		}
		if n.Host == "" || n.Port == 0 {
			t.Errorf("%s…: host/port not set: %q %d", uri[:20], n.Host, n.Port)
		}
		perProtocol[want.proto]++
	}
	if len(perProtocol) != 7 {
		t.Errorf("expected 7 protocols exercised, got %d", len(perProtocol))
	}
}

func TestParseLineSkips(t *testing.T) {
	for _, line := range []string{"", "   "} {
		n, err := ParseLine(line)
		if n != nil || err != nil {
			t.Errorf("ParseLine(%q) = %v, %v; want nil, nil", line, n, err)
		}
	}
	for _, line := range []string{"hello world", "https://example.com/page"} {
		n, err := ParseLine(line)
		if n != nil || err != ErrNotShareLink {
			t.Errorf("ParseLine(%q) = %v, %v; want nil, ErrNotShareLink", line, n, err)
		}
	}
}

func TestRawJSONVmessParsesOutbound(t *testing.T) {
	uri := `vmess://{"v":"2","ps":"","add":"146.56.112.110","port":"8888","id":"e7c302f3-90d6-42dd-9d7d-94a3683a3707","aid":"0","scy":"auto","net":"raw","type":"","tls":""}#x`
	n, err := ParseLine(uri)
	if err != nil {
		t.Fatalf("raw json vmess: %v", err)
	}
	if n.Protocol != "vmess" || n.Transport != "tcp" || n.Port != 8888 || n.Host != "146.56.112.110" {
		t.Fatalf("bad node: %+v", n)
	}
}

func TestCanonicalVmessBase64Values(t *testing.T) {
	// base64 of {"v":"2","add":"EXample.COM","port":443,"id":"uuid-1","net":"ws","path":"/","host":"h","tls":"tls","sni":"s","alpn":"h2","scy":"auto","ps":"ignored","aid":"0"}
	b64 := "eyJ2IjoiMiIsImFkZCI6IkVYYW1wbGUuQ09NIiwicG9ydCI6NDQzLCJpZCI6InV1aWQtMSIsIm5ldCI6IndzIiwicGF0aCI6Ii8iLCJob3N0IjoiaCIsInRscyI6InRscyIsInNuaSI6InMiLCJhbHBuIjoiaDIiLCJzY3kiOiJhdXRvIiwicHMiOiJpZ25vcmVkIiwiYWlkIjoiMCJ9"
	canonical, err := Canonical("vmess://" + b64 + "#name")
	if err != nil {
		t.Fatal(err)
	}
	want := "vmess|2|example.com|443|uuid-1|ws|/|h|tls|s|h2|auto"
	if canonical != want {
		t.Fatalf("canonical=%q want %q", canonical, want)
	}
}

func TestCanonicalDefaultPortAndIPv6(t *testing.T) {
	c, err := Canonical("HTTPS://User@Example.COM:443/a/b?b=2&a=1#frag")
	if err != nil {
		t.Fatal(err)
	}
	if c != "https://User@example.com/a/b?a=1&b=2" {
		t.Fatalf("got %q", c)
	}
	c, err = Canonical("vless://uuid@[2001:db8::1]:443?type=tcp")
	if err != nil {
		t.Fatal(err)
	}
	if c != "vless://uuid@[2001:db8::1]:443?type=tcp" {
		t.Fatalf("ipv6: got %q", c)
	}
	// vless:443 is NOT a default port — must stay (§18.6)
	c, err = Canonical("socks://u@1.2.3.4:1080")
	if err != nil {
		t.Fatal(err)
	}
	if c != "socks://u@1.2.3.4" {
		t.Fatalf("socks default port: got %q", c)
	}
}

func TestParseManyBase64Sub(t *testing.T) {
	body := "dmxlc3M6Ly91dWlkQDEuMi4zLjQ6NDQzP3NlY3VyaXR5PW5vbmUmdHlwZT10Y3AjbmFtZQp0cm9qYW46Ly9wd0A1LjYuNy44OjQ0Mz9zYT1leGFtcGxlLmNvbSNuYW1lCg=="
	nodes := ParseMany([]byte(body))
	if len(nodes) != 2 {
		t.Fatalf("want 2 nodes, got %d", len(nodes))
	}
	if nodes[0].Protocol != "vless" || nodes[1].Protocol != "trojan" {
		t.Fatalf("protocols: %s %s", nodes[0].Protocol, nodes[1].Protocol)
	}
}
