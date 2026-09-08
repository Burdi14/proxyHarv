package geo

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestMMDBLookup(t *testing.T) {
	l, err := New("testdata/GeoLite2-City-Test.mmdb", time.Hour)
	if err != nil {
		t.Skipf("no test mmdb: %v", err)
	}
	defer l.Close()

	// Known MaxMind test record: 81.2.69.160 → GB / London.
	g := l.Geo(net.ParseIP("81.2.69.160"))
	if !g.OK || g.CC != "GB" {
		t.Fatalf("geo(81.2.69.160) = %+v, want GB", g)
	}
	if g.City != "London" {
		t.Errorf("city = %q, want London", g.City)
	}
	if g.Lat == 0 && g.Lon == 0 {
		t.Errorf("lat/lon empty")
	}

	// Private/reserved range → no record.
	if g := l.Geo(net.ParseIP("127.0.0.1")); g.OK {
		t.Errorf("loopback unexpectedly geolocated: %+v", g)
	}
}

func TestResolveCache(t *testing.T) {
	l, err := New("testdata/GeoLite2-City-Test.mmdb", 30*time.Millisecond)
	if err != nil {
		t.Skipf("no test mmdb: %v", err)
	}
	defer l.Close()

	var calls int32
	fakeIPs := []net.IP{net.ParseIP("81.2.69.160")}
	l.lookupIP = func(_ context.Context, _, _ string) ([]net.IP, error) {
		atomic.AddInt32(&calls, 1)
		return fakeIPs, nil
	}

	for i := 0; i < 3; i++ {
		if got := l.Resolve(context.Background(), "Example.COM"); len(got) != 1 {
			t.Fatalf("resolve %d: %v", i, got)
		}
	}
	if calls != 1 {
		t.Fatalf("dns called %d times, want 1 (cache)", calls)
	}

	time.Sleep(35 * time.Millisecond)
	l.Resolve(context.Background(), "example.com")
	if calls != 2 {
		t.Fatalf("dns called %d after ttl expiry, want 2", calls)
	}

	// literal IP: no dns call, returned as-is
	before := atomic.LoadInt32(&calls)
	if got := l.Resolve(context.Background(), "192.0.2.10"); len(got) != 1 || got[0].String() != "192.0.2.10" {
		t.Fatalf("literal ip: %v", got)
	}
	if atomic.LoadInt32(&calls) != before {
		t.Fatalf("literal ip should not hit dns")
	}
}

func TestLocate(t *testing.T) {
	l, err := New("testdata/GeoLite2-City-Test.mmdb", time.Hour)
	if err != nil {
		t.Skipf("no test mmdb: %v", err)
	}
	defer l.Close()
	ip, g := l.Locate(context.Background(), "81.2.69.160")
	if ip != "81.2.69.160" || g.CC != "GB" {
		t.Fatalf("locate = %q %+v", ip, g)
	}
	_, g = l.Locate(context.Background(), "nonexistent.invalid.")
	if g.OK {
		t.Fatalf("expected empty geo for bad host")
	}
}

func TestNoDBMode(t *testing.T) {
	l, err := New("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if g := l.Geo(net.ParseIP("81.2.69.160")); g.OK {
		t.Fatalf("nil db should not geolocate")
	}
	_ = fmt.Sprint(l.CacheLen())
}
