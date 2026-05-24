package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// errPionLoopbackNoRTP signals that the receiving PeerConnection never
// surfaced a single RTP packet during the proof-of-concept window.
var errPionLoopbackNoRTP = errors.New("pion loopback: no RTP delivered")

// vp8KeyframeStub is the smallest byte sequence pion accepts as a VP8
// keyframe sample. It is meaningless visually but valid enough that the
// depacketizer/sample builder on the receiving side accepts the RTP
// stream and OnTrack fires. We are not testing video quality here — only
// that two PeerConnections inside the same process can negotiate, gather
// loopback ICE candidates, and exchange RTP without an external bridge.
//
//nolint:gochecknoglobals // test-only constant byte literal
var vp8KeyframeStub = []byte{
	0x10, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x40, 0x01, 0xf0, 0x00,
}

// TestPionLoopbackPOC is a focused proof-of-concept: spin up two pion
// PeerConnections inside one process, wire them together with manual
// SDP+trickle-ICE exchange, publish a VP8 track on one side, and wait
// for OnTrack to surface real RTP on the other side. If this test
// passes, the same wiring can be embedded into memoryStream so that
// videochannel/seichannel/vp8channel transports complete their
// handshake under the in-memory carrier used by the e2e suite.
//
// The test is intentionally small and self-contained — no production
// code is touched, no e2e fixtures are involved.
//
//nolint:gocognit,cyclop // PoC is intentionally linear; splitting helpers hurts readability
func TestPionLoopbackPOC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	api := webrtc.NewAPI()

	pcOffer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("new offer pc: %v", err)
	}
	t.Cleanup(func() { _ = pcOffer.Close() })

	pcAnswer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("new answer pc: %v", err)
	}
	t.Cleanup(func() { _ = pcAnswer.Close() })

	// Trickle ICE: forward every gathered candidate straight to the peer.
	// A nil candidate terminates gathering and is intentionally ignored.
	pcOffer.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		if err := pcAnswer.AddICECandidate(c.ToJSON()); err != nil {
			t.Logf("answer AddICECandidate: %v", err)
		}
	})
	pcAnswer.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		if err := pcOffer.AddICECandidate(c.ToJSON()); err != nil {
			t.Logf("offer AddICECandidate: %v", err)
		}
	})

	gotRTP := make(chan struct{})
	pcAnswer.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		buf := make([]byte, 1500)
		// First read confirms RTP is actually flowing end-to-end.
		if _, _, err := track.Read(buf); err != nil {
			t.Logf("answer first Read: %v", err)
			return
		}
		select {
		case <-gotRTP:
		default:
			close(gotRTP)
		}
		// Keep draining so pion's per-track buffer doesn't back up.
		for {
			if _, _, err := track.Read(buf); err != nil {
				return
			}
		}
	})

	localTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"poc-stream",
		"poc-track",
	)
	if err != nil {
		t.Fatalf("new local track: %v", err)
	}
	if _, err := pcOffer.AddTrack(localTrack); err != nil {
		t.Fatalf("offer AddTrack: %v", err)
	}

	offer, err := pcOffer.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	if err := pcOffer.SetLocalDescription(offer); err != nil {
		t.Fatalf("offer SetLocalDescription: %v", err)
	}
	if err := pcAnswer.SetRemoteDescription(offer); err != nil {
		t.Fatalf("answer SetRemoteDescription: %v", err)
	}
	answer, err := pcAnswer.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("create answer: %v", err)
	}
	if err := pcAnswer.SetLocalDescription(answer); err != nil {
		t.Fatalf("answer SetLocalDescription: %v", err)
	}
	if err := pcOffer.SetRemoteDescription(answer); err != nil {
		t.Fatalf("offer SetRemoteDescription: %v", err)
	}

	// Pump tiny VP8 keyframe stubs at 20 fps until OnTrack fires or
	// the test times out. Real frame content is irrelevant — we only
	// care that RTP packets traverse the loopback ICE/DTLS/SRTP stack.
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = localTrack.WriteSample(media.Sample{
					Data:     vp8KeyframeStub,
					Duration: 50 * time.Millisecond,
				})
			}
		}
	}()

	select {
	case <-gotRTP:
		t.Logf("pion loopback delivered RTP successfully")
	case <-ctx.Done():
		t.Fatalf("%v: timed out after %s", errPionLoopbackNoRTP, 15*time.Second)
	}
}
