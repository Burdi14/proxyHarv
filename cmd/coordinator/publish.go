package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"proxyfarm/internal/model"
	"proxyfarm/internal/natsb"
)

// publishLoop publishes immutable list versions vN (§4): every 6h OR when the
// alive-set composition changed by >20% since the last publication.
func (c *coord) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute) // change detection tick
	period := c.cfg.publishEvery
	lastPublish := time.Time{}
	var lastSet map[int64]struct{}
	if p, err := c.store.LatestPublish(ctx); err == nil && p != nil {
		lastPublish = p.CreatedAt
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			nodes, err := c.store.ListNodesForPublish(ctx)
			if err != nil {
				log.Warn("publish list", "err", err.Error())
				continue
			}
			if len(nodes) == 0 {
				continue // nothing alive yet; wait for the pipeline
			}
			set := make(map[int64]struct{}, len(nodes))
			for _, n := range nodes {
				set[n.ID] = struct{}{}
			}
			periodDue := lastPublish.IsZero() || now.Sub(lastPublish) >= period
			changed := false
			if lastSet != nil && len(lastSet) > 0 {
				diff := 0
				for id := range set {
					if _, ok := lastSet[id]; !ok {
						diff++
					}
				}
				for id := range lastSet {
					if _, ok := set[id]; !ok {
						diff++
					}
				}
				changed = diff*100/len(lastSet) > 20 // Δ состава >20% (§4)
			}
			if !periodDue && !changed {
				continue
			}
			if err := c.publishVersion(ctx, nodes); err != nil {
				log.Error("publish", "err", err.Error())
				continue
			}
			lastPublish = now
			lastSet = set
		}
	}
}

type publishMeta struct {
	Version    int64          `json:"version"`
	CreatedAt  string         `json:"created_at"`
	NodeCount  int            `json:"node_count"`
	ClassCount map[string]int `json:"class_count"`
	Checksum   string         `json:"checksum"`
	Size       int            `json:"size"`
	Classes    []string       `json:"classes"`
}

func (c *coord) publishVersion(ctx context.Context, nodes []model.Node) error {
	classes := map[string]int{"rf": 0, "cloud": 0}
	latencyByNode := map[int64]int{}
	for _, n := range nodes {
		classes[n.BestClass]++
		if latest, err := c.store.LatestCheck(ctx, n.ID, []string{"L1"}); err == nil && latest != nil && latest.LatencyMs != nil {
			latencyByNode[n.ID] = *latest.LatencyMs
		}
	}

	// subscription body: share-links with readable names (§8)
	lines := make([]string, 0, len(nodes))
	for _, n := range nodes {
		cc := derefString(n.CC, "XX")
		city := derefString(n.City, "Unknown")
		lines = append(lines, renameFragment(n.URI, cc, city, latencyByNode[n.ID], n.BestClass))
	}
	// §18.12: join \n, base64 std (not urlsafe), no inner wrapping
	body := base64.StdEncoding.EncodeToString([]byte(strings.Join(lines, "\n")))
	checksum := sha256.Sum256([]byte(body))
	checksumHex := "sha256:" + hex.EncodeToString(checksum[:])

	pub := &model.Publish{
		ClassFilter: "all",
		NodeCount:   len(nodes),
		Checksum:    checksumHex,
		KVKey:       "", // filled after InsertPublish assigns version
	}
	if err := c.store.InsertPublish(ctx, pub); err != nil {
		return err
	}
	pub.KVKey = natsb.PublishKey(pub.Version)

	meta := publishMeta{
		Version:    pub.Version,
		CreatedAt:  pub.CreatedAt.UTC().Format(time.RFC3339),
		NodeCount:  len(nodes),
		ClassCount: classes,
		Checksum:   checksumHex,
		Size:       len(body),
		Classes:    []string{"rf", "cloud"},
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := c.bus.PutPublish(ctx, pub.Version, metaJSON); err != nil {
		return err
	}
	c.cfg.reg.Publish(float64(pub.CreatedAt.Unix()))
	log.Info("published", "version", pub.Version, "nodes", len(nodes),
		"rf", classes["rf"], "cloud", classes["cloud"], "checksum", checksumHex)
	return nil
}

// renameFragment swaps the uri fragment for the {CC}-{City}-{latency}ms-{class}
// subscription display name (§8).
func renameFragment(uri, cc, city string, latencyMs int, class string) string {
	base := uri
	if i := strings.IndexByte(uri, '#'); i >= 0 {
		base = uri[:i]
	}
	label := cc + "-" + city + "-" + itoa(latencyMs) + "ms-" + class
	return base + "#" + url.QueryEscape(label)
}

func itoa(v int) string {
	if v <= 0 {
		return "?" // no L1 latency recorded yet
	}
	return strconv.Itoa(v)
}

func derefString(p *string, def string) string {
	if p == nil || *p == "" {
		return def
	}
	return *p
}

// maintenanceLoop: monthly partitions, retention drops, long-dead cleanup (§4).
func (c *coord) maintenanceLoop(ctx context.Context) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			if err := c.store.EnsureCheckPartition(ctx, now.Add(35*24*time.Hour)); err != nil {
				log.Warn("partition ensure", "err", err.Error())
			}
			if now.Hour() == 3 { // nightly chores
				if err := c.store.DropOldCheckPartitions(ctx, 30); err != nil {
					log.Warn("partition drop", "err", err.Error())
				}
				if n, err := c.store.DeleteLongDead(ctx, now.Add(-14*24*time.Hour)); err == nil && n > 0 {
					log.Info("deleted long-dead nodes", "count", n)
				}
			}
		}
	}
}
