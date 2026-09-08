// Package score aggregates check history into the node score (§7):
//
//	score = w1·recency + w2·success_rate + w3·latency + w4·speed + w5·rf
//
// Every component is normalized to [0,1]; default weights (§17):
// 0.2 / 0.3 / 0.2 / 0.2 / 0.1.
package score

import (
	"strconv"
	"strings"
	"time"
)

type Weights struct {
	Recency     float64
	SuccessRate float64
	Latency     float64
	Speed       float64
	RF          float64
}

// Default returns the §17 weights.
func Default() Weights {
	return Weights{0.2, 0.3, 0.2, 0.2, 0.1}
}

// ParseWeights accepts "0.2,0.3,0.2,0.2,0.1" (ConfigMap-friendly).
// Empty string yields the §17 defaults.
func ParseWeights(s string) (Weights, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Default(), true
	}
	parts := strings.Split(s, ",")
	if len(parts) != 5 {
		return Default(), false
	}
	var vals [5]float64
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return Default(), false
		}
		vals[i] = f
	}
	return Weights{vals[0], vals[1], vals[2], vals[3], vals[4]}, true
}

type Input struct {
	Now        time.Time
	LastL1OKAt time.Time // zero = never
	RecentL1   []bool    // most-recent-first, last ≤10 L1 results
	LatencyMs  int
	SpeedMbps  float64
	RFValidated bool
}

// Recency: 1.0 while fresh (≤6h), then linear decay to 0 at 48h.
func Recency(now, lastOK time.Time) float64 {
	if lastOK.IsZero() {
		return 0
	}
	age := now.Sub(lastOK)
	if age <= 6*time.Hour {
		return 1
	}
	decay := age - 6*time.Hour
	const span = 42 * time.Hour
	if decay >= span {
		return 0
	}
	return float64(span-decay) / float64(span)
}

// SuccessRate: share of ok among recent L1 checks.
func SuccessRate(recent []bool) float64 {
	if len(recent) == 0 {
		return 0
	}
	ok := 0
	for _, b := range recent {
		if b {
			ok++
		}
	}
	return float64(ok) / float64(len(recent))
}

// LatencyScore: 300 ms or faster → 1.0, linear to 0 at 3 s (0 beyond/missing).
func LatencyScore(ms int) float64 {
	switch {
	case ms <= 0:
		return 0
	case ms <= 300:
		return 1
	case ms >= 3000:
		return 0
	default:
		return float64(3000-ms) / 2700.0
	}
}

// SpeedScore: 100 Mbps or faster → 1.0, linear below.
func SpeedScore(mbps float64) float64 {
	if mbps <= 0 {
		return 0
	}
	if mbps >= 100 {
		return 1
	}
	return mbps / 100.0
}

// Compute returns the weighted score in [0, 1].
func Compute(in Input, w Weights) float64 {
	s := w.Recency*Recency(in.Now, in.LastL1OKAt) +
		w.SuccessRate*SuccessRate(in.RecentL1) +
		w.Latency*LatencyScore(in.LatencyMs) +
		w.Speed*SpeedScore(in.SpeedMbps)
	if in.RFValidated {
		s += w.RF
	}
	if s < 0 {
		return 0
	}
	if s > 1 {
		return 1
	}
	return s
}
