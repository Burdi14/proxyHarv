// Package natsb wires NATS JetStream: streams, KV buckets, durable pull
// consumers (§14.4), and the wire messages (§14.3).
package natsb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Subjects.
const (
	SubjectRaw     = "raw.import"
	SubjectJobsL0  = "jobs.l0.cloud"
	SubjectJobsL1  = "jobs.l1.cloud"
	SubjectJobsL2  = "jobs.l2.cloud"
	SubjectResults = "results.cloud-eu-1" // results.<vantage>
	SubjectDLQ     = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
)

// Stream names.
const (
	StreamRAW       = "RAW"
	StreamJOBSL0    = "JOBS_L0"
	StreamJOBSL1    = "JOBS_L1"
	StreamJOBSL2    = "JOBS_L2"
	StreamJOBSVAL   = "JOBS_VAL"
	StreamRESULTS   = "RESULTS"
	StreamDLQ       = "DLQ"
	BucketPUBLISHES = "PUBLISHES"
	BucketREGISTRY  = "REGISTRY"
)

func JobsSubjectFor(stage, vantage string) string {
	switch stage {
	case "L0":
		return SubjectJobsL0
	case "L1":
		return SubjectJobsL1
	case "L2":
		return SubjectJobsL2
	case "VAL":
		return fmt.Sprintf("jobs.validator.%s", vantage)
	}
	return ""
}

func ResultsSubjectFor(vantage string) string {
	return "results." + vantage
}

const gb = 1024 * 1024 * 1024

// EnsureAll creates/updates streams and KV buckets per the §14.4 table.
func EnsureAll(ctx context.Context, js jetstream.JetStream) error {
	return ensureAll(ctx, js, gb)
}

// EnsureAllTiny uses 1 MB stream limits — for tests on small disks.
func EnsureAllTiny(ctx context.Context, js jetstream.JetStream) error {
	return ensureAll(ctx, js, 1024*1024)
}

func ensureAll(ctx context.Context, js jetstream.JetStream, unit int64) error {
	streams := []jetstream.StreamConfig{
		{Name: StreamRAW, Subjects: []string{SubjectRaw},
			Retention: jetstream.InterestPolicy,
			MaxBytes:  1 * unit, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage},
		{Name: StreamJOBSL0, Subjects: []string{"jobs.l0.>"},
			Retention: jetstream.WorkQueuePolicy,
			MaxBytes:  5 * unit, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage},
		{Name: StreamJOBSL1, Subjects: []string{"jobs.l1.>"},
			Retention: jetstream.WorkQueuePolicy,
			MaxBytes:  5 * unit, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage},
		{Name: StreamJOBSL2, Subjects: []string{"jobs.l2.>"},
			Retention: jetstream.WorkQueuePolicy,
			MaxBytes:  1 * unit, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage},
		{Name: StreamJOBSVAL, Subjects: []string{"jobs.validator.>"},
			Retention: jetstream.WorkQueuePolicy,
			MaxBytes:  1 * unit, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage},
		{Name: StreamRESULTS, Subjects: []string{"results.>"},
			Retention: jetstream.InterestPolicy,
			MaxBytes:  5 * unit, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage},
		{Name: StreamDLQ, Subjects: []string{SubjectDLQ},
			Retention: jetstream.LimitsPolicy,
			MaxBytes:  1 * unit, Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage},
	}
	for i := range streams {
		cfg := streams[i]
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("stream %s: %w", cfg.Name, err)
		}
	}
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: BucketPUBLISHES, History: 10, Storage: jetstream.FileStorage}); err != nil {
		return fmt.Errorf("kv PUBLISHES: %w", err)
	}
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: BucketREGISTRY, History: 1, TTL: 2 * time.Hour, Storage: jetstream.FileStorage}); err != nil {
		return fmt.Errorf("kv REGISTRY: %w", err)
	}
	return nil
}

// Consumer groups.
const (
	DurableRaw       = "coordinator-raw"
	DurableResults   = "coordinator-results"
	DurableWorkerL0  = "workers-l0"
	DurableWorkerL1  = "workers-l1"
	DurableWorkerL2  = "workers-l2"
	durableValidator = "validator-%s"
)

// DurableForValidator returns the per-vantage durable name on JOBS_VAL.
func DurableForValidator(vantage string) string {
	return fmt.Sprintf(durableValidator, vantage)
}

// EnsureConsumer creates/updates a durable pull consumer.
func EnsureConsumer(ctx context.Context, js jetstream.JetStream, stream, durable, filter string) (jetstream.Consumer, error) {
	cfg := &jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,
		AckWait:       5 * time.Minute,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
	switch stream {
	case StreamJOBSL2:
		cfg.AckWait = 10 * time.Minute
	case StreamRAW, StreamRESULTS:
		cfg.AckWait = 2 * time.Minute
	}
	return js.CreateOrUpdateConsumer(ctx, stream, *cfg)
}

// ---- wire messages (§14.3) ----

// RawNode is one discovered share-link inside a RAW batch.
type RawNode struct {
	URI       string `json:"uri"`
	URIHash   string `json:"uri_hash"`
	Protocol  string `json:"protocol"`
	Transport string `json:"transport"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
}

// RawBatch is published by fetcher on raw.import (batches ≤100).
type RawBatch struct {
	Event     string    `json:"event"` // "nodes.found"
	SourceID  int64     `json:"source_id"`
	FetchedAt string    `json:"fetched_at"`
	Nodes     []RawNode `json:"nodes"`
}

// JobNode carries either a dial target (L0) or a ready outbound (L1/L2).
type JobNode struct {
	NodeID    int64           `json:"node_id"`
	Host      string          `json:"host,omitempty"`
	Port      int             `json:"port,omitempty"`
	Transport string          `json:"transport,omitempty"`
	Outbound  json.RawMessage `json:"outbound,omitempty"`
}

// JobBatch is one unit of work for a worker.
type JobBatch struct {
	JobID     string    `json:"job_id"`
	Stage     string    `json:"stage"` // L0|L1|L2|VAL
	CreatedAt string    `json:"created_at"`
	Vantage   string    `json:"vantage"`
	Nodes     []JobNode `json:"nodes"`
}

// ResultItem is one node outcome.
type ResultItem struct {
	NodeID    int64    `json:"node_id"`
	OK        bool     `json:"ok"`
	LatencyMs *int     `json:"latency_ms"`
	SpeedMbps *float64 `json:"speed_mbps"`
	ErrorCode *string  `json:"error_code"`
	TS        string   `json:"ts"`
}

// ResultsBatch is published by workers on results.<vantage>.
type ResultsBatch struct {
	JobID   string       `json:"job_id"`
	Vantage string       `json:"vantage"`
	Stage   string       `json:"stage"`
	Results []ResultItem `json:"results"`
}

// MsgIDForJobs / MsgIDForResults implement the §14.3 dedup rule.
func MsgIDForJobs(jobID string) string { return jobID }
func MsgIDForResults(jobID, vantage, stage string) string {
	return jobID + ":" + vantage + ":" + stage
}

// Connect opens a NATS connection with optional creds/nkey (§9.2).
func Connect(url, credsPath string) (*nats.Conn, error) {
	opts := []nats.Option{nats.Name("proxyfarm")}
	if credsPath != "" {
		opts = append(opts, nats.UserCredentials(credsPath))
	}
	return nats.Connect(url, opts...)
}
