package vp8channel

import (
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

const (
	defaultFPS       = 30
	defaultBatchSize = 64
)

// Options tunes the vp8channel transport. Zero values fall back to documented defaults.
type Options struct {
	FPS       int
	BatchSize int
}

// TransportOptions marks Options as belonging to the transport options family.
func (Options) TransportOptions() {}

// withDefaults fills unset Options fields with the package defaults. FPS in
// particular must never stay zero: the writer loop derives its ticker period
// from it and a zero divides by zero.
func (o Options) withDefaults() Options {
	if o.FPS <= 0 {
		o.FPS = defaultFPS
	}
	if o.BatchSize <= 0 {
		o.BatchSize = defaultBatchSize
	}
	return o
}

func optionsFrom(cfg transport.Config) (Options, error) {
	opts, err := transport.OptionsAs[Options](cfg, "vp8channel")
	if err != nil {
		return Options{}, err
	}
	return opts.withDefaults(), nil
}
