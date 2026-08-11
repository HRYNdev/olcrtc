package videochannel

import (
	"bytes"
	"testing"
)

func TestVisualRoundTrip(t *testing.T) {
	payload := []byte("hello over visual videochannel")
	frame, err := renderVisualFrame(payload, 320, 240, "qrcode", "low", 4, 20)
	if err != nil {
		t.Fatalf("renderVisualFrame failed: %v", err)
	}

	got, err := extractVisualPayload(frame, 320, 240, "qrcode", 4, 20)
	if err != nil {
		t.Fatalf("extractVisualPayload failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got=%q want=%q", got, payload)
	}
}

func TestIdleFrameIgnored(t *testing.T) {
	frame, err := renderVisualFrame(nil, 320, 240, "qrcode", "low", 4, 20)
	if err != nil {
		t.Fatalf("renderVisualFrame failed: %v", err)
	}

	got, err := extractVisualPayload(frame, 320, 240, "qrcode", 4, 20)
	if err == nil && len(got) != 0 {
		t.Fatalf("expected idle frame to be ignored, got=%q", got)
	}
}

func TestTileVisualRoundTrip(t *testing.T) {
	payload := []byte("hello over tile videochannel")
	frame, err := renderVisualFrame(payload, 1080, 1080, "tile", "", 4, 20)
	if err != nil {
		t.Fatalf("renderVisualFrame tile failed: %v", err)
	}

	got, err := extractVisualPayload(frame, 1080, 1080, "tile", 4, 20)
	if err != nil {
		t.Fatalf("extractVisualPayload tile failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got=%q want=%q", got, payload)
	}
}

func TestTileIdleFrameIgnored(t *testing.T) {
	frame, err := renderVisualFrame(nil, 1080, 1080, "tile", "", 4, 20)
	if err != nil {
		t.Fatalf("renderVisualFrame tile failed: %v", err)
	}

	got, err := extractVisualPayload(frame, 1080, 1080, "tile", 4, 20)
	if err == nil && len(got) != 0 {
		t.Fatalf("expected tile idle frame to be ignored, got=%q", got)
	}
}
