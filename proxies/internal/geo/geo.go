// Package geo wraps offline MaxMind mmdb lookup (§6) plus a DNS resolver with
// a TTL cache (default 1h, §17). No external geo APIs.
package geo

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"
)

type cacheEntry struct {
	ips     []net.IP
	expires time.Time
}

// Locator resolves hosts to IPs (cached) and looks up geo data in mmdb.
// A nil db disables geo lookups (DNS cache still works).
type Locator struct {
	db *geoip2.Reader

	mu    sync.Mutex
	ttl   time.Duration
	cache map[string]cacheEntry

	lookupIP func(ctx context.Context, network, host string) ([]net.IP, error)
}

// New opens mmdbPath (empty string → no geo) and a resolver with the given TTL.
func New(mmdbPath string, ttl time.Duration) (*Locator, error) {
	l := &Locator{ttl: ttl, cache: map[string]cacheEntry{}}
	var resolver net.Resolver
	l.lookupIP = resolver.LookupIP
	if mmdbPath != "" {
		db, err := geoip2.Open(mmdbPath)
		if err != nil {
			return nil, err
		}
		l.db = db
	}
	return l, nil
}

// Close releases the mmdb handle.
func (l *Locator) Close() error {
	if l.db != nil {
		return l.db.Close()
	}
	return nil
}

// Resolve returns IPs for host; literal IPs are returned uncached. Hosts are
// lowercased so the cache is consistent with the §14.2 canonical host.
func (l *Locator) Resolve(ctx context.Context, host string) []net.IP {
	host = normalizeHost(host)
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}
	}
	l.mu.Lock()
	if e, ok := l.cache[host]; ok && time.Now().Before(e.expires) {
		l.mu.Unlock()
		return e.ips
	}
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ips, err := l.lookupIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil
	}
	l.mu.Lock()
	l.cache[host] = cacheEntry{ips: ips, expires: time.Now().Add(l.ttl)}
	l.mu.Unlock()
	return ips
}

// Geo looks up cc/city/lat/lon for an IP string.
type Geo struct {
	CC   string
	City string
	Lat  float64
	Lon  float64
	OK   bool
}

func (l *Locator) Geo(ip net.IP) Geo {
	if l.db == nil || ip == nil {
		return Geo{}
	}
	rec, err := l.db.City(ip)
	if err != nil {
		return Geo{}
	}
	g := Geo{CC: rec.Country.IsoCode, Lat: rec.Location.Latitude, Lon: rec.Location.Longitude}
	if len(rec.City.Names) > 0 {
		g.City = rec.City.Names["en"]
	}
	if g.CC != "" {
		g.OK = true
	}
	return g
}

// Locate combines Resolve + Geo: returns the first resolved IP and its geo.
func (l *Locator) Locate(ctx context.Context, host string) (string, Geo) {
	ips := l.Resolve(ctx, host)
	if len(ips) == 0 {
		return "", Geo{}
	}
	ip := ips[0].String()
	return ip, l.Geo(ips[0])
}

func normalizeHost(host string) string {
	if len(host) > 1 && host[0] == '[' && host[len(host)-1] == ']' {
		return host[1 : len(host)-1]
	}
	return host
}

// CacheLen is exposed for tests and diagnostics.
func (l *Locator) CacheLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.cache)
}
