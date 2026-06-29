package vp8channel

import "testing"

// TestRatePacerProbesUpOnLowDelay verifies the pacer additively raises the
// rate while the path delay stays near the baseline, up to the ceiling.
func TestRatePacerProbesUpOnLowDelay(t *testing.T) {
	rp := newRatePacer(30, defaultStartRate, defaultMinProbeRate, defaultMaxBytesPerSec)

	// First sample establishes the baseline (20ms). Subsequent equal samples
	// read as uncongested, so the rate climbs by rateProbeStepBytes each tick.
	rp.observe(20)
	start := rp.rate
	for range 5 {
		rp.observe(20)
	}
	if rp.rate <= start {
		t.Fatalf("rate did not climb on low delay: start=%d now=%d", start, rp.rate)
	}
	if rp.rate > rp.maxRate {
		t.Fatalf("rate %d exceeded ceiling %d", rp.rate, rp.maxRate)
	}
}

// TestRatePacerBacksOffOnHighDelay verifies a delay spike past the congestion
// mark triggers a multiplicative decrease.
func TestRatePacerBacksOffOnHighDelay(t *testing.T) {
	rp := newRatePacer(30, defaultStartRate, defaultMinProbeRate, defaultMaxBytesPerSec)
	rp.observe(20) // baseline 20ms
	before := rp.rate

	// 20ms baseline -> highMark = 20 + 10 + 25 = 55ms. A 120ms sample is well
	// past it and must shrink the rate.
	rp.observe(120)
	if rp.rate >= before {
		t.Fatalf("rate did not back off on high delay: before=%d after=%d", before, rp.rate)
	}
	want := before * rateBackoffNum / rateBackoffDen
	if rp.rate != want {
		t.Fatalf("backoff rate = %d, want %d", rp.rate, want)
	}
}

// TestRatePacerConvergesToKnee drives a synthetic policer: delay stays low
// until the rate crosses a hidden knee, then inflates. The pacer must settle
// in a band around that knee instead of running away or collapsing.
func TestRatePacerConvergesToKnee(t *testing.T) {
	const knee = 600_000
	rp := newRatePacer(30, defaultStartRate, defaultMinProbeRate, defaultMaxBytesPerSec)

	srtt := func(rate int) int32 {
		// Below the knee the path is idle (20ms). Above it the policer queues,
		// inflating delay in proportion to the overshoot.
		if rate <= knee {
			return 20
		}
		return 20 + int32(min((rate-knee)/10_000, 1_000)) //nolint:gosec // G115: bounded by min to 1000
	}

	for range 400 {
		rp.observe(srtt(rp.rate))
	}

	// After convergence the rate must sit in a sane band around the knee:
	// high enough to beat the old 400 KB/s floor, never pinned at the ceiling.
	if rp.rate < defaultStartRate {
		t.Fatalf("converged rate %d below start floor %d", rp.rate, defaultStartRate)
	}
	if rp.rate >= rp.maxRate {
		t.Fatalf("rate pinned at ceiling %d, did not detect knee", rp.rate)
	}
	if rp.rate > knee+4*rateProbeStepBytes {
		t.Fatalf("converged rate %d overshoots knee %d", rp.rate, knee)
	}
}

// TestRatePacerHoldsWithoutSample verifies a non-positive SRTT (no estimate
// yet) holds the rate steady rather than probing blind.
func TestRatePacerHoldsWithoutSample(t *testing.T) {
	rp := newRatePacer(30, defaultStartRate, defaultMinProbeRate, defaultMaxBytesPerSec)
	before := rp.rate
	if got := rp.observe(0); got != before/rp.fps {
		t.Fatalf("perTick on no-sample = %d, want %d", got, before/rp.fps)
	}
	if rp.rate != before {
		t.Fatalf("rate moved without a sample: before=%d after=%d", before, rp.rate)
	}
}

// TestRatePacerNeverBelowFloor verifies sustained congestion cannot starve the
// rate below the minimum that keeps the control plane and keepalives flowing.
func TestRatePacerNeverBelowFloor(t *testing.T) {
	rp := newRatePacer(30, defaultStartRate, defaultMinProbeRate, defaultMaxBytesPerSec)
	rp.observe(10)
	for range 100 {
		rp.observe(5000) // permanent heavy congestion
	}
	if rp.rate < rp.minRate {
		t.Fatalf("rate %d fell below floor %d", rp.rate, rp.minRate)
	}
}

// TestRatePacerPerTickFloor verifies the per-tick budget never drops below one
// epoch header so keepalives always fit even at the rate floor.
func TestRatePacerPerTickFloor(t *testing.T) {
	rp := newRatePacer(30, epochHdrLen, epochHdrLen, epochHdrLen)
	if got := rp.perTick(); got < epochHdrLen {
		t.Fatalf("perTick = %d, want >= %d", got, epochHdrLen)
	}
}
