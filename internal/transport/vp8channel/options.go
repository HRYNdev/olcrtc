package vp8channel

import (
	"fmt"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

const (
	defaultFPS       = 30
	defaultBatchSize = 64
	// defaultMaxBytesPerSec is the upper bound the adaptive pacer may probe up
	// to when no explicit ceiling is configured. The pacer (see pacer.go)
	// auto-discovers each SFU's real policer knee at runtime from KCP's
	// smoothed RTT, so this is just a sanity cap, not a hand-tuned operating
	// point. 1 MiB/s matches the throughput target from issue #107 without
	// letting a misbehaving path probe unbounded.
	defaultMaxBytesPerSec = 1_000_000

	// defaultMinProbeRate floors the adaptive rate so a congested path can
	// always make forward progress and recover once delay drops. ~120 KB/s
	// stays below the worst observed Telemost knee while keeping the control
	// plane and keepalives flowing.
	defaultMinProbeRate = 120_000

	// defaultStartRate is where the pacer begins probing from on a fresh
	// session: the old conservative 400 KB/s floor, so behaviour on a healthy
	// path only ever ramps up from the previously shipped operating point.
	defaultStartRate = 400_000

	// Adaptive pacer tuning. Delay marks are derived from the measured path
	// baseline plus a fixed slack so a low-RTT LAN and a high-RTT WAN both get
	// a sane congestion threshold.
	rateProbeSlackMs      = 8  // extra ms below which we probe the rate up
	rateCongestionSlackMs = 25 // extra ms above which we back the rate off
	rateProbeStepBytes    = 50_000
	rateBackoffNum        = 4 // multiplicative decrease: rate *= 4/5
	rateBackoffDen        = 5
	baseRTTDriftDiv       = 64 // baseline rises by 1/64 of the gap per sample
)

// Options tunes the vp8channel transport. Zero values fall back to documented defaults.
type Options struct {
	FPS       int
	BatchSize int
	// MaxBytesPerSec caps the wire byte-rate fed to the video track. Zero
	// falls back to defaultMaxBytesPerSec.
	MaxBytesPerSec int
}

// TransportOptions marks Options as belonging to the transport options family.
func (Options) TransportOptions() {}

func optionsFrom(cfg transport.Config) (Options, error) {
	if cfg.Options == nil {
		return Options{}, nil
	}
	opts, ok := cfg.Options.(Options)
	if !ok {
		return Options{}, fmt.Errorf("%w: vp8channel: got %T", transport.ErrOptionsTypeMismatch, cfg.Options)
	}
	return opts, nil
}
