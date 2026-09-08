package score

import (
	"testing"
	"time"
)

func TestWeightsDefaults(t *testing.T) {
	w := Default()
	if w != (Weights{0.2, 0.3, 0.2, 0.2, 0.1}) {
		t.Fatalf("default weights: %+v", w)
	}
	if p, ok := ParseWeights("0.1,0.2,0.3,0.2,0.2"); !ok || p.Recency != 0.1 || p.RF != 0.2 {
		t.Fatalf("parse: %+v ok=%v", p, ok)
	}
	if _, ok := ParseWeights("0.2,0.3,0.2,0.2"); ok {
		t.Fatal("4 values should not parse")
	}
	if p, ok := ParseWeights(""); !ok || p != Default() {
		t.Fatalf("empty falls back to default: %+v %v", p, ok)
	}
}

func TestRecency(t *testing.T) {
	now := time.Now()
	if Recency(now, time.Time{}) != 0 {
		t.Fatal("never ok → 0")
	}
	if Recency(now, now.Add(-1*time.Hour)) != 1 {
		t.Fatal("1h old → 1")
	}
	if Recency(now, now.Add(-6*time.Hour)) != 1 {
		t.Fatal("6h old → 1 (boundary)")
	}
	mid := Recency(now, now.Add(-27*time.Hour)) // 21h into decay
	if mid <= 0 || mid >= 1 {
		t.Fatalf("27h → (0,1), got %v", mid)
	}
	if Recency(now, now.Add(-49*time.Hour)) != 0 {
		t.Fatal("49h → 0")
	}
}

func TestSuccessRate(t *testing.T) {
	if SuccessRate(nil) != 0 {
		t.Fatal("empty → 0")
	}
	if SuccessRate([]bool{true, true, false}) != 2.0/3.0 {
		t.Fatal("2/3")
	}
	if SuccessRate(make([]bool, 10)) != 0 {
		t.Fatal("all fails → 0")
	}
}

func TestLatencySpeed(t *testing.T) {
	if LatencyScore(0) != 0 || LatencyScore(-5) != 0 {
		t.Fatal("no latency → 0")
	}
	if LatencyScore(100) != 1 || LatencyScore(300) != 1 {
		t.Fatal("≤300ms → 1")
	}
	if LatencyScore(3000) != 0 || LatencyScore(5000) != 0 {
		t.Fatal("≥3s → 0")
	}
	if l := LatencyScore(1650); l < 0.49 || l > 0.51 {
		t.Fatalf("1650ms ≈ 0.5, got %v", l)
	}
	if SpeedScore(0) != 0 || SpeedScore(100) != 1 || SpeedScore(250) != 1 {
		t.Fatal("speed bounds")
	}
	if SpeedScore(50) != 0.5 {
		t.Fatal("50mbps → 0.5")
	}
}

func TestCompute(t *testing.T) {
	w := Default()
	now := time.Now()
	perfect := Input{
		Now: now, LastL1OKAt: now.Add(-time.Hour),
		RecentL1:   []bool{true, true, true, true, true, true, true, true, true, true},
		LatencyMs:  120, SpeedMbps: 100, RFValidated: true,
	}
	if got := Compute(perfect, w); got < 0.99 || got > 1.0 {
		t.Fatalf("perfect node score = %v, want ~1.0", got)
	}
	dead := Input{Now: now}
	if got := Compute(dead, w); got != 0 {
		t.Fatalf("dead node score = %v, want 0", got)
	}
	cloud := Input{
		Now: now, LastL1OKAt: now.Add(-time.Hour),
		RecentL1: []bool{true, false, true},
		LatencyMs: 500, SpeedMbps: 0, RFValidated: false,
	}
	if got := Compute(cloud, w); got <= 0 || got >= 1 {
		t.Fatalf("mid node out of range: %v", got)
	}
	// RF bonus must increase the score with everything else fixed
	withRF := cloud
	withRF.RFValidated = true
	if Compute(withRF, w) <= Compute(cloud, w) {
		t.Fatal("rf bonus must add score")
	}
	// monotonic in latency
	faster := cloud
	faster.LatencyMs = 150
	if Compute(faster, w) <= Compute(cloud, w) {
		t.Fatal("faster node must score higher")
	}
}
