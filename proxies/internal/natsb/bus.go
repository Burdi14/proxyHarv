package natsb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Bus wraps a JetStream context with proxyfarm publish/consume helpers.
type Bus struct {
	JS jetstream.JetStream
}

func NewBus(js jetstream.JetStream) *Bus { return &Bus{JS: js} }

// PublishRaw publishes a RAW batch with Nats-Msg-Id dedup.
func (b *Bus) PublishRaw(ctx context.Context, msgID string, batch RawBatch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	_, err = b.JS.Publish(ctx, SubjectRaw, data, jetstream.WithMsgID(msgID))
	return err
}

// PublishJobs publishes a job batch to the stage subject with dedup by job_id.
func (b *Bus) PublishJobs(ctx context.Context, stage, vantage string, batch JobBatch) error {
	subj := JobsSubjectFor(stage, vantage)
	if subj == "" {
		return fmt.Errorf("unknown stage %q", stage)
	}
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	_, err = b.JS.Publish(ctx, subj, data, jetstream.WithMsgID(MsgIDForJobs(batch.JobID)))
	return err
}

// PublishResults publishes a results batch (dedup job_id:vantage:stage).
// Publish is synchronous (waits for the server ack — §18.8).
func (b *Bus) PublishResults(ctx context.Context, batch ResultsBatch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	_, err = b.JS.Publish(ctx, ResultsSubjectFor(batch.Vantage), data,
		jetstream.WithMsgID(MsgIDForResults(batch.JobID, batch.Vantage, batch.Stage)))
	return err
}

// FetchMsgs pulls up to n messages (batch-pull, ack explicit §5) and decodes
// them; messages that fail to decode are NAKed.
func FetchMsgs[T any](cons jetstream.Consumer, n int, wait time.Duration) ([]jetstream.Msg, []T, error) {
	if n <= 0 {
		n = 20
	}
	if wait <= 0 {
		wait = 2 * time.Second
	}
	msgs, err := cons.Fetch(n, jetstream.FetchMaxWait(wait))
	if err != nil {
		return nil, nil, err
	}
	var raw []jetstream.Msg
	var out []T
	for m := range msgs.Messages() {
		var v T
		if err := json.Unmarshal(m.Data(), &v); err != nil {
			_ = m.Nak()
			continue
		}
		raw = append(raw, m)
		out = append(out, v)
	}
	return raw, out, msgs.Error()
}

// PutPublish stores an immutable publish version (KV publishes/vN).
func (b *Bus) PutPublish(ctx context.Context, version int64, meta []byte) error {
	kv, err := b.JS.KeyValue(ctx, BucketPUBLISHES)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, publishKey(version), meta)
	return err
}

func publishKey(version int64) string { return fmt.Sprintf("publishes/v%d", version) }

// PublishKey is exported for api consumers.
func PublishKey(version int64) string { return publishKey(version) }

// LatestPublish reads the newest version entry.
func (b *Bus) LatestPublish(ctx context.Context) (int64, []byte, error) {
	kv, err := b.JS.KeyValue(ctx, BucketPUBLISHES)
	if err != nil {
		return 0, nil, err
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer w.Stop()
	var bestVer int64
	var bestVal []byte
	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				if bestVer > 0 {
					return bestVer, bestVal, nil
				}
				continue
			}
			if entry.Operation() == jetstream.KeyValuePut {
				var v int64
				if _, err := fmt.Sscanf(entry.Key(), "publishes/v%d", &v); err == nil && v > bestVer {
					bestVer, bestVal = v, entry.Value()
				}
			}
		case <-ctx.Done():
			if bestVer > 0 {
				return bestVer, bestVal, nil
			}
			return 0, nil, ctx.Err()
		case <-time.After(2 * time.Second):
			if bestVer > 0 {
				return bestVer, bestVal, nil
			}
			return 0, nil, errors.New("no publishes found")
		}
	}
}

// WatchPublishes invokes fn on every publish update (api SSE).
func (b *Bus) WatchPublishes(ctx context.Context, fn func(version int64, value []byte)) error {
	kv, err := b.JS.KeyValue(ctx, BucketPUBLISHES)
	if err != nil {
		return err
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		return err
	}
	defer w.Stop()
	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok {
				return nil
			}
			if entry == nil || entry.Operation() != jetstream.KeyValuePut {
				continue
			}
			var v int64
			if _, err := fmt.Sscanf(entry.Key(), "publishes/v%d", &v); err == nil {
				fn(v, entry.Value())
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Heartbeat writes the vantage registry entry (REGISTRY KV, ttl 2h).
func (b *Bus) Heartbeat(ctx context.Context, vantage, role string) error {
	kv, err := b.JS.KeyValue(ctx, BucketREGISTRY)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]string{
		"vantage": vantage, "role": role,
		"ts": time.Now().UTC().Format(time.RFC3339),
	})
	_, err = kv.Put(ctx, "vantage."+vantage, body)
	return err
}

// TryAcquireLease implements the coordinator leader lease: a timestamped
// REGISTRY entry that must be renewed every renew interval; a lease whose
// holder missed its TTL can be taken over.
func (b *Bus) TryAcquireLease(ctx context.Context, instanceID string, ttl, renew time.Duration) (release func(), leader func() bool, err error) {
	kv, err := b.JS.KeyValue(ctx, BucketREGISTRY)
	if err != nil {
		return nil, nil, err
	}
	const key = "lease.coordinator"
	isLeader := false

	type leaseEntry struct {
		ID string `json:"id"`
		TS string `json:"ts"`
	}
	nowEntry := func() []byte {
		v, _ := json.Marshal(leaseEntry{ID: instanceID, TS: time.Now().UTC().Format(time.RFC3339)})
		return v
	}
	claim := func() bool {
		gctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if entry, err := kv.Get(gctx, key); err == nil {
			var prev leaseEntry
			_ = json.Unmarshal(entry.Value(), &prev)
			if ts, err := time.Parse(time.RFC3339, prev.TS); err == nil &&
				prev.ID != instanceID && time.Since(ts) < ttl {
				return false // healthy lease held by someone else
			}
		}
		_, err := kv.Put(gctx, key, nowEntry())
		return err == nil
	}

	isLeader = claim()
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(renew)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if isLeader {
					kctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, _ = kv.Put(kctx, key, nowEntry())
					cancel()
				} else {
					isLeader = claim()
				}
			}
		}
	}()
	return func() { close(stop) }, func() bool { return isLeader }, nil
}
