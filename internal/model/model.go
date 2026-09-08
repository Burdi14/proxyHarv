// Package model holds domain entities and the pgx-backed Store. The
// coordinator is the single writer; api is read-only; workers never touch pg.
package model

import (
	"context"
	"time"
)

type Source struct {
	ID          int64      `json:"id"`
	URL         string     `json:"url"`
	Kind        string     `json:"kind"`
	Enabled     bool       `json:"enabled"`
	LastFetchAt *time.Time `json:"last_fetch_at"`
	LastStatus  *string    `json:"last_status"`
	Note        *string    `json:"note"`
	CreatedAt   time.Time  `json:"created_at"`
}

type Node struct {
	ID         int64      `json:"id"`
	URIHash    string     `json:"uri_hash"`
	URI        string     `json:"uri"`
	Protocol   string     `json:"protocol"`
	Transport  string     `json:"transport"`
	Host       string     `json:"host"`
	HostIP     *string    `json:"host_ip"`
	CC         *string    `json:"cc"`
	City       *string    `json:"city"`
	Lat        *float64   `json:"lat"`
	Lon        *float64   `json:"lon"`
	Status     string     `json:"status"`     // unknown|alive|dead
	BestClass  string     `json:"best_class"` // none|cloud|rf
	Score      float64    `json:"score"`
	FirstSeen  time.Time  `json:"first_seen"`
	LastSeen   time.Time  `json:"last_seen"`
	LastL1OKAt *time.Time `json:"last_l1_ok_at"`
}

type Check struct {
	ID        int64     `json:"id"`
	NodeID    int64     `json:"node_id"`
	Vantage   string    `json:"vantage"`
	Stage     string    `json:"stage"` // L0|L1|L2
	OK        bool      `json:"ok"`
	LatencyMs *int      `json:"latency_ms"`
	SpeedMbps *float64  `json:"speed_mbps"`
	ErrorCode *string   `json:"error_code"`
	TS        time.Time `json:"ts"`
	TSBucket  int64     `json:"ts_bucket"`
}

type Vantage struct {
	ID            string     `json:"id"`
	Kind          string     `json:"kind"` // cloud|edge
	Region        *string    `json:"region"`
	Enabled       bool       `json:"enabled"`
	LastHeartbeat *time.Time `json:"last_heartbeat_at"`
}

type Publish struct {
	Version     int64     `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	ClassFilter string    `json:"class_filter"`
	NodeCount   int       `json:"node_count"`
	Checksum    string    `json:"checksum"`
	KVKey       string    `json:"kv_key"`
}

// CheckOutcome is what workers report (§14.3 RESULTS).
type CheckOutcome struct {
	NodeID    int64    `json:"node_id"`
	OK        bool     `json:"ok"`
	LatencyMs *int     `json:"latency_ms"`
	SpeedMbps *float64 `json:"speed_mbps"`
	ErrorCode *string  `json:"error_code"`
	TS        string   `json:"ts"`
}

// NodeUpdate is the aggregated view used for scoring recompute.
type NodeUpdate struct {
	NodeID     int64
	Status     string
	BestClass  string
	Score      float64
	LastL1OKAt *time.Time
}

// CityAgg powers GET /api/cities (§8).
type CityAgg struct {
	City          string   `json:"city"`
	CC            string   `json:"cc"`
	Lat           float64  `json:"lat"`
	Lon           float64  `json:"lon"`
	Count         int      `json:"count"`
	BestLatencyMs *int     `json:"best_latency_ms"`
	Protocols     []string `json:"protocols"`
}

// NodeCard powers GET /api/nodes and /api/node/{id}.
type NodeCard struct {
	ID        int64      `json:"id"`
	Protocol  string     `json:"protocol"`
	Transport string     `json:"transport"`
	CC        string     `json:"cc"`
	City      string     `json:"city"`
	Lat       float64    `json:"lat,omitempty"`
	Lon       float64    `json:"lon,omitempty"`
	Class     string     `json:"class"`
	LatencyMs *int       `json:"latency_ms"`
	SpeedMbps *float64   `json:"speed_mbps"`
	LastCheck *time.Time `json:"last_check"`
	ShareLink string     `json:"share_link,omitempty"`
}

// Store is the persistence boundary.
type Store interface {
	Close()

	Migrate(ctx context.Context) error
	SeedSQL(ctx context.Context, sql string) error

	EnsureCheckPartition(ctx context.Context, ts time.Time) error
	DropOldCheckPartitions(ctx context.Context, days int) error
	DeleteLongDead(ctx context.Context, olderThan time.Time) (int64, error)

	UpsertNode(ctx context.Context, n *Node) (id int64, created bool, err error)
	UpdateNodeGeo(ctx context.Context, id int64, hostIP string, cc, city *string, lat, lon *float64) error
	GetNode(ctx context.Context, id int64) (*Node, error)
	GetNodeByHash(ctx context.Context, hash string) (*Node, error)

	NodesDueL0(ctx context.Context, limit int) ([]Node, error)
	NodesDueL1(ctx context.Context, limit int) ([]Node, error)
	NodesForL2(ctx context.Context, topK int, notCheckedSince time.Duration, limit int) ([]Node, error)
	NodesForValidation(ctx context.Context, vantage string, topN int) ([]Node, error)
	ListNodesForPublish(ctx context.Context) ([]Node, error)

	InsertCheck(ctx context.Context, c *Check) (inserted bool, err error)
	RecentL1(ctx context.Context, nodeID int64, vantage string, limit int) ([]bool, error)
	LatestCheck(ctx context.Context, nodeID int64, stages []string) (*Check, error)
	LatestByVantage(ctx context.Context, nodeID int64) (map[string]Check, error)
	UpdateNodeAggregate(ctx context.Context, u NodeUpdate) error

	UpsertVantage(ctx context.Context, v *Vantage) error
	DeleteVantage(ctx context.Context, id string) error
	ListVantages(ctx context.Context, enabledOnly bool) ([]Vantage, error)

	ListSources(ctx context.Context, enabledOnly bool) ([]Source, error)
	UpsertSource(ctx context.Context, s *Source) (int64, error)
	DeleteSource(ctx context.Context, id int64) error
	UpdateSourceStatus(ctx context.Context, id int64, status string, at time.Time) error

	InsertPublish(ctx context.Context, p *Publish) error
	ListPublishes(ctx context.Context, limit int) ([]Publish, error)
	LatestPublish(ctx context.Context) (*Publish, error)

	CityAggregates(ctx context.Context, class string) ([]CityAgg, error)
	NodeCards(ctx context.Context, city, class, proto string, limit int) ([]NodeCard, error)
	NodeCard(ctx context.Context, id int64) (*NodeCard, error)

	AliveCount(ctx context.Context) (int, error)
	CountNodes(ctx context.Context) (int64, error)
}
