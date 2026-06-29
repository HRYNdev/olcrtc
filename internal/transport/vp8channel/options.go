package vp8channel

import (
	"fmt"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

const (
	defaultFPS       = 30
	defaultBatchSize = 64
	// defaultMaxBytesPerSec paces the wire byte-rate fed to the video track.
	// The earlier ~42s collapse at 1.2 MiB/s was the SFU dropping a track with
	// no decodable keyframe, not a raw rate ceiling; that keyframe starvation
	// is fixed separately (forceKeepalive, issue #95), so the pacer can run at
	// the 1 MB/s target from issue #107 instead of the old 400 KB/s stability
	// compromise. The policer knee still differs per SFU, so operators can tune
	// this via Options.MaxBytesPerSec (vp8.max_bytes_per_sec in YAML).
	defaultMaxBytesPerSec = 1_000_000
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
