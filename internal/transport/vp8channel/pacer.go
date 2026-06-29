package vp8channel

// ratePacer is a delay-based AIMD controller that auto-discovers the wire
// byte-rate a policed carrier (the SFU) tolerates, with no operator tuning.
//
// The carrier policer starts queueing before it starts dropping, so KCP's
// smoothed RTT inflates ahead of the throughput collapse seen in issues #95
// and #107. Each control tick the pacer folds one SRTT sample in: while the
// delay stays near the path baseline it probes the rate up additively, and
// once the delay inflates past the baseline it backs off multiplicatively.
// The rate settles just under the per-SFU policer knee on its own, which is
// the "real run" compromise the fixed 400 KB/s floor only approximated. The
// operator ceiling (vp8.max_bytes_per_sec) just bounds how high it may probe.
type ratePacer struct {
	fps     int
	minRate int
	maxRate int
	rate    int
	baseRTT int32
}

func newRatePacer(fps, startRate, minRate, maxRate int) *ratePacer {
	if fps <= 0 {
		fps = defaultFPS
	}
	if minRate <= 0 {
		minRate = defaultMinProbeRate
	}
	if maxRate < minRate {
		maxRate = minRate
	}
	return &ratePacer{
		fps:     fps,
		minRate: minRate,
		maxRate: maxRate,
		rate:    max(minRate, min(startRate, maxRate)),
	}
}

// perTick is the current per-frame byte budget. The ticker already paces at
// fps, so a per-tick cap bounds the rate without token bookkeeping. Floor at
// one epoch header so keepalives always fit.
func (rp *ratePacer) perTick() int {
	pt := rp.rate / rp.fps
	if pt < epochHdrLen {
		return epochHdrLen
	}
	return pt
}

// observe folds one smoothed-RTT sample (milliseconds, as KCP reports it) into
// the controller and returns the updated per-tick byte budget. A non-positive
// sample means KCP has no RTT estimate yet, so the rate is held steady.
func (rp *ratePacer) observe(srtt int32) int {
	if srtt <= 0 {
		return rp.perTick()
	}
	rp.trackBaseline(srtt)

	lowMark := rp.baseRTT + rp.baseRTT/4 + rateProbeSlackMs
	highMark := rp.baseRTT + rp.baseRTT/2 + rateCongestionSlackMs
	switch {
	case srtt >= highMark:
		rp.rate = max(rp.minRate, min(rp.rate*rateBackoffNum/rateBackoffDen, rp.maxRate))
	case srtt <= lowMark:
		rp.rate = max(rp.minRate, min(rp.rate+rateProbeStepBytes, rp.maxRate))
	}
	return rp.perTick()
}

// trackBaseline anchors baseRTT to the path's uncongested delay: it drops
// instantly to any new minimum and drifts up slowly otherwise, so a genuine
// path change cannot pin the baseline below the real floor forever while a
// transient queueing spike still reads as congestion.
func (rp *ratePacer) trackBaseline(srtt int32) {
	if rp.baseRTT == 0 || srtt < rp.baseRTT {
		rp.baseRTT = srtt
		return
	}
	rp.baseRTT += (srtt - rp.baseRTT) / baseRTTDriftDiv
}
