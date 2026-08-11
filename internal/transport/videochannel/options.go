package videochannel

import (
	"fmt"

	grtile "github.com/zarazaex69/gr/tile"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// Package defaults for unset Options fields. They mirror the session-level
// video defaults so a transport built straight from a zero Options behaves
// like one built from the documented config.
const (
	defaultFPS    = 30
	defaultWidth  = 1920
	defaultHeight = 1080
	codecTile     = "tile"
)

// Options tunes the videochannel transport. Zero values fall back to documented defaults.
type Options struct {
	Width      int
	Height     int
	FPS        int
	Bitrate    string
	HW         string
	QRSize     int
	QRRecovery string
	Codec      string
	TileModule int
	TileRS     int
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
	// The tile codec renders fixed-size frames, so its dimensions are not a
	// free choice - they must match the tile frame or the encoder rejects
	// every sample.
	if o.Codec == codecTile {
		if o.Width <= 0 {
			o.Width = grtile.FrameW
		}
		if o.Height <= 0 {
			o.Height = grtile.FrameH
		}
		return o
	}
	if o.Width <= 0 {
		o.Width = defaultWidth
	}
	if o.Height <= 0 {
		o.Height = defaultHeight
	}
	return o
}

func optionsFrom(cfg transport.Config) (Options, error) {
	if cfg.Options == nil {
		return Options{}.withDefaults(), nil
	}
	opts, ok := cfg.Options.(Options)
	if !ok {
		return Options{}, fmt.Errorf("%w: videochannel: got %T", transport.ErrOptionsTypeMismatch, cfg.Options)
	}
	return opts.withDefaults(), nil
}
