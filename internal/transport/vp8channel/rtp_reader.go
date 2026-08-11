package vp8channel

import (
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// reorderWindow bounds how many out-of-order RTP packets the reorder buffer
// holds while waiting for a gap to fill. Real SFUs reorder within a handful of
// packets; once this many newer packets pile up behind a hole, the missing
// sequence is treated as genuinely lost and we advance, so a truly dropped
// packet cannot stall delivery indefinitely.
const reorderWindow = 256

// seqLess reports whether RTP sequence a precedes b using wrap-around aware
// comparison (RFC 1982 serial arithmetic on uint16).
func seqLess(a, b uint16) bool {
	// bit15 of the wrap-around difference is the serial sign bit: set means a
	// precedes b. Avoids a signed conversion gosec flags as overflow.
	return (a-b)&0x8000 != 0
}

// reorderBuffer restores RTP sequence order before frame assembly. The SFU may
// deliver packets out of order or drop them; feeding that stream straight into
// the strict contiguity check in processRTPPacket made every reorder look like
// loss and discarded whole frames (issue #95: ~80-90% of VP8 frames dropped on
// a live SFU). Buffering by sequence number and draining in order means only
// genuine loss produces a gap.
type reorderBuffer struct {
	pkts    map[uint16]*rtp.Packet
	nextSeq uint16
	started bool
}

func newReorderBuffer() *reorderBuffer {
	return &reorderBuffer{pkts: make(map[uint16]*rtp.Packet, reorderWindow)}
}

// push adds pkt and returns any packets now deliverable in strict sequence
// order. The caller reuses its read buffer across packets, so the payload is
// copied before buffering.
func (b *reorderBuffer) push(pkt *rtp.Packet) []*rtp.Packet {
	if !b.started {
		b.started = true
		b.nextSeq = pkt.SequenceNumber
	}
	// Drop packets older than our current position: already delivered, or
	// skipped past as lost.
	if seqLess(pkt.SequenceNumber, b.nextSeq) {
		return nil
	}
	cp := &rtp.Packet{Header: pkt.Header}
	cp.Payload = append([]byte(nil), pkt.Payload...)
	b.pkts[pkt.SequenceNumber] = cp

	// Holding a full window behind a hole means the head sequence is
	// genuinely lost: skip forward to the oldest buffered packet.
	if len(b.pkts) > reorderWindow {
		b.skipToOldest()
	}
	return b.drain()
}

// drain pops contiguous packets starting at nextSeq.
func (b *reorderBuffer) drain() []*rtp.Packet {
	var out []*rtp.Packet
	for {
		pkt, ok := b.pkts[b.nextSeq]
		if !ok {
			return out
		}
		out = append(out, pkt)
		delete(b.pkts, b.nextSeq)
		b.nextSeq++
	}
}

// skipToOldest advances nextSeq to the lowest buffered sequence, abandoning a
// lost packet so drain can make progress.
func (b *reorderBuffer) skipToOldest() {
	first := true
	var oldest uint16
	for seq := range b.pkts {
		if first || seqLess(seq, oldest) {
			oldest = seq
			first = false
		}
	}
	b.nextSeq = oldest
}

type vp8FrameState struct {
	vp8Pkt      codecs.VP8Packet
	frameBuf    []byte
	lastSeq     uint16
	haveLastSeq bool
	frameValid  bool
}

// processRTPPacket returns a complete VP8 frame payload when fully assembled,
// nil otherwise. Detects packet loss/reordering to avoid silently corrupting
// fragmented VP8 frames.
func (s *vp8FrameState) processRTPPacket(pkt *rtp.Packet) []byte {
	if s.haveLastSeq && pkt.SequenceNumber != s.lastSeq+1 {
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
	}
	s.lastSeq = pkt.SequenceNumber
	s.haveLastSeq = true

	vp8Payload, err := s.vp8Pkt.Unmarshal(pkt.Payload)
	if err != nil {
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
		return nil
	}

	if s.vp8Pkt.S == 1 {
		s.frameBuf = s.frameBuf[:0]
		s.frameValid = true
	}

	if !s.frameValid {
		return nil
	}

	s.frameBuf = append(s.frameBuf, vp8Payload...)

	if !pkt.Marker {
		return nil
	}

	defer func() {
		s.frameBuf = s.frameBuf[:0]
		s.frameValid = false
	}()

	if len(s.frameBuf) >= epochHdrLen {
		frame := make([]byte, len(s.frameBuf))
		copy(frame, s.frameBuf)
		return frame
	}
	return nil
}

func (p *streamTransport) handleRemoteTrack(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	if track.Codec().MimeType != webrtc.MimeTypeVP8 {
		go p.drainTrack(track)
		return
	}

	// We don't reset KCP here. Peer restarts are detected by the epoch
	// header on incoming frames, which works even when the SFU keeps
	// forwarding the same track across our restarts.
	go p.readVP8Track(track)
}

func (p *streamTransport) drainTrack(track *webrtc.TrackRemote) {
	buf := make([]byte, rtpBufSize)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
		}
	}
}

func (p *streamTransport) readVP8Track(track *webrtc.TrackRemote) {
	var state vp8FrameState
	reorder := newReorderBuffer()
	buf := make([]byte, rtpBufSize)
	var rtpCount, frameCount int

	for {
		n, _, err := track.Read(buf)
		if err != nil {
			logger.Infof("vp8channel: readVP8Track closed track=%s rtp=%d frames=%d err=%v",
				track.ID(), rtpCount, frameCount, err)
			return
		}
		rtpCount++

		pkt := &rtp.Packet{}
		if pkt.Unmarshal(buf[:n]) != nil {
			continue
		}

		// Restore sequence order before assembly so SFU reordering is not
		// mistaken for loss.
		for _, ordered := range reorder.push(pkt) {
			frame := state.processRTPPacket(ordered)
			if frame == nil {
				continue
			}
			frameCount++
			p.handleIncomingFrame(frame)
		}
	}
}
