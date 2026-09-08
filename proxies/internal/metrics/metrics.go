// Package metrics implements the §16 Prometheus metric set without external
// dependencies, rendering the classic text exposition format:
//
//	proxyfarm_probe_requests_total{stage,vantage,code}
//	proxyfarm_probe_duration_seconds{stage}          (histogram)
//	proxyfarm_job_batches_total{stream,result}
//	proxyfarm_publish_timestamp                        (gauge)
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

var buckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 45}

type counterKey struct{ stage, vantage, code string }

type Registry struct {
	mu               sync.Mutex
	probeRequests    map[counterKey]uint64
	jobBatches       map[[2]string]uint64
	probeCount       map[string]uint64
	probeSum         map[string]float64
	probeHist        map[string][]uint64
	publishTimestamp float64
}

func NewRegistry() *Registry {
	r := &Registry{
		probeRequests: map[counterKey]uint64{},
		jobBatches:    map[[2]string]uint64{},
		probeCount:    map[string]uint64{},
		probeSum:      map[string]float64{},
		probeHist:     map[string][]uint64{},
	}
	for _, b := range buckets {
		_ = b
	}
	return r
}

func esc(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	return strings.ReplaceAll(v, `"`, `\"`)
}

func (r *Registry) ProbeRequest(stage, vantage, code string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probeRequests[counterKey{stage, vantage, code}]++
}

func (r *Registry) ProbeDuration(stage string, seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probeCount[stage]++
	r.probeSum[stage] += seconds
	h := r.probeHist[stage]
	if h == nil {
		h = make([]uint64, len(buckets)+1)
	}
	for i, b := range buckets {
		if seconds <= b {
			h[i]++
			r.probeHist[stage] = h
			return
		}
	}
	h[len(buckets)]++
	r.probeHist[stage] = h
}

func (r *Registry) JobBatch(stream, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobBatches[[2]string{stream, result}]++
}

func (r *Registry) Publish(ts float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publishTimestamp = ts
}

// Render emits the registry in Prometheus text format.
func (r *Registry) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder

	b.WriteString("# HELP proxyfarm_probe_requests_total Total probes finished.\n")
	b.WriteString("# TYPE proxyfarm_probe_requests_total counter\n")
	keys := make([]counterKey, 0, len(r.probeRequests))
	for k := range r.probeRequests {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].stage < keys[j].stage })
	for _, k := range keys {
		fmt.Fprintf(&b, "proxyfarm_probe_requests_total{stage=%q,vantage=%q,code=%q} %d\n",
			esc(k.stage), esc(k.vantage), esc(k.code), r.probeRequests[k])
	}

	b.WriteString("# HELP proxyfarm_probe_duration_seconds Probe wall time.\n")
	b.WriteString("# TYPE proxyfarm_probe_duration_seconds histogram\n")
	stages := make([]string, 0, len(r.probeCount))
	for s := range r.probeCount {
		stages = append(stages, s)
	}
	sort.Strings(stages)
	for _, s := range stages {
		h := r.probeHist[s]
		cum := uint64(0)
		for i, ub := range buckets {
			cum += h[i]
			fmt.Fprintf(&b, "proxyfarm_probe_duration_seconds_bucket{stage=%q,le=\"%v\"} %d\n", esc(s), ub, cum)
		}
		cum += h[len(buckets)]
		fmt.Fprintf(&b, "proxyfarm_probe_duration_seconds_bucket{stage=%q,le=\"+Inf\"} %d\n", esc(s), cum)
		fmt.Fprintf(&b, "proxyfarm_probe_duration_seconds_sum{stage=%q} %v\n", esc(s), r.probeSum[s])
		fmt.Fprintf(&b, "proxyfarm_probe_duration_seconds_count{stage=%q} %d\n", esc(s), r.probeCount[s])
	}

	b.WriteString("# HELP proxyfarm_job_batches_total Job batches consumed.\n")
	b.WriteString("# TYPE proxyfarm_job_batches_total counter\n")
	jk := make([][2]string, 0, len(r.jobBatches))
	for k := range r.jobBatches {
		jk = append(jk, k)
	}
	sort.Slice(jk, func(i, j int) bool { return jk[i][0] < jk[j][0] })
	for _, k := range jk {
		fmt.Fprintf(&b, "proxyfarm_job_batches_total{stream=%q,result=%q} %d\n",
			esc(k[0]), esc(k[1]), r.jobBatches[k])
	}

	b.WriteString("# HELP proxyfarm_publish_timestamp Unix time of the latest publish.\n")
	b.WriteString("# TYPE proxyfarm_publish_timestamp gauge\n")
	fmt.Fprintf(&b, "proxyfarm_publish_timestamp %v\n", r.publishTimestamp)
	return b.String()
}

// Handler serves /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(r.Render()))
	})
}
