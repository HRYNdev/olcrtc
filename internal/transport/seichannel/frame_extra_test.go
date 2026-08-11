package seichannel

import (
	"bytes"
	"errors"
	"testing"
)

func TestSEIHelpersAndErrors(t *testing.T) {
	escaped := escapeRBSP([]byte{0, 0, 1, 0, 0, 2, 3})
	if !bytes.Equal(unescapeRBSP(escaped), []byte{0, 0, 1, 0, 0, 2, 3}) {
		t.Fatalf("unescapeRBSP(escapeRBSP()) = %v", unescapeRBSP(escaped))
	}

	value := appendSEIValue(nil, 300)
	got, next, err := consumeSEIValue(value, 0)
	if err != nil || got != 300 || next != len(value) {
		t.Fatalf("consumeSEIValue() = (%d, %d, %v), want 300", got, next, err)
	}
	if _, _, truncErr := consumeSEIValue([]byte{0xff}, 0); !errors.Is(truncErr, ErrSEIValueTruncated) {
		t.Fatalf("consumeSEIValue() error = %v, want %v", truncErr, ErrSEIValueTruncated)
	}

	rbsp := appendSEIValue(nil, 5)
	rbsp = append(rbsp, appendSEIValue(nil, len(videoSEIUUID)+5)...)
	rbsp = append(rbsp, videoSEIUUID[:]...)
	rbsp = append(rbsp, []byte{1, 2}...)
	if _, seiErr := extractTransportSEI(rbsp); !errors.Is(seiErr, ErrSEIPayloadTruncated) {
		t.Fatalf("extractTransportSEI() error = %v, want %v", seiErr, ErrSEIPayloadTruncated)
	}

	payloads, err := extractTransportSEI([]byte{4, 1, 0, 0x80})
	if err != nil {
		t.Fatalf("extractTransportSEI(non-transport) error = %v", err)
	}
	if len(payloads) != 0 {
		t.Fatalf("extractTransportSEI(non-transport) = %v, want none", payloads)
	}
}
