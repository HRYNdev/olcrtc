package livekit

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	lksdk "github.com/owenewans/owenlivekit/v2"
	"github.com/pion/webrtc/v4"
)

// MemoryHub is an in-process stand-in for a LiveKit room, used by tests and
// local benchmarks to run the real LiveKit engine without an SFU. It
// reproduces the parts of LiveKit that olcrtc relies on:
//
//   - every participant gets a unique identity;
//   - reliable delivery, ordered per sender→receiver pair, with the sender
//     identity and topic on receive; the sender never receives its own data;
//   - DestinationIdentities addressing, where an empty list means everyone
//     else in the room;
//   - participant-left notifications to the remaining participants.
//
// Receivers are decoupled from senders by an unbounded per-receiver queue, as
// the SFU buffers per subscriber: a slow receiver does not stall the sender's
// traffic to anybody else.
type MemoryHub struct {
	// SubscribeDelay drops everything sent to a participant during this long
	// after it joined, like a LiveKit subscriber whose data channel is not
	// live yet. Zero disables it.
	SubscribeDelay time.Duration
	// UplinkBitsPerSec caps each participant's publish rate, like its single
	// data channel to the SFU; publishing blocks while over budget, as the SDK
	// does on a full SCTP buffer. Zero means unlimited.
	UplinkBitsPerSec int64

	mu    sync.Mutex
	parts map[string]*memParticipant
	next  int
}

// NewMemoryHub creates an empty in-memory room.
func NewMemoryHub() *MemoryHub {
	return &MemoryHub{parts: make(map[string]*memParticipant)}
}

// NewSession creates a LiveKit engine session whose room lives in this hub.
// URL/Token/Refresh are ignored.
func (h *MemoryHub) NewSession(cfg engine.Config) engine.Session {
	return newSession(cfg, h.connect, nil)
}

// Participants returns the identities currently in the room, sorted.
func (h *MemoryHub) Participants() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.parts))
	for id := range h.parts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Kick removes a participant as the SFU would on eviction: the others get
// participant-left and the kicked session sees a disconnect.
func (h *MemoryHub) Kick(identity string) {
	h.mu.Lock()
	p := h.parts[identity]
	h.mu.Unlock()
	if p == nil {
		return
	}
	p.disconnect()
	if cb := p.cb; cb != nil && cb.OnDisconnected != nil {
		go cb.OnDisconnected()
	}
}

// Bounce emulates a participant that drops out of the room and rejoins under
// the same identity without its olcrtc session noticing (a LiveKit SDK
// restart, or a quick reconnect with a per-account identity): the others get
// participant-left, and everything sent to it during away is lost. Its own
// session keeps running and keeps sending.
func (h *MemoryHub) Bounce(identity string, away time.Duration) {
	h.mu.Lock()
	p := h.parts[identity]
	others := make([]*memParticipant, 0, len(h.parts))
	for id, o := range h.parts {
		if id != identity {
			others = append(others, o)
		}
	}
	h.mu.Unlock()
	if p == nil {
		return
	}
	p.qmu.Lock()
	p.awayUntil = time.Now().Add(away)
	p.queue = nil
	p.qmu.Unlock()
	for _, o := range others {
		if sink := o.leftSink.Load(); sink != nil {
			go (*sink)(identity)
		}
	}
}

// Publish sends one packet into the room as a participant that is not an
// olcrtc session (a browser in the call): topic and destinations as in
// LiveKit, empty destinations meaning everyone.
func (h *MemoryHub) Publish(sender, topic string, data []byte, destinations ...string) {
	payload := append([]byte(nil), data...)
	for _, t := range h.targets(sender, destinations) {
		t.enqueue(memPacket{sender: sender, topic: topic, data: payload})
	}
}

func (h *MemoryHub) connect(
	_, _ string, cb *lksdk.RoomCallback, _ ...lksdk.ConnectOption,
) (roomHandle, error) {
	h.mu.Lock()
	h.next++
	p := &memParticipant{
		hub:      h,
		identity: fmt.Sprintf("PA_mem%d", h.next),
		cb:       cb,
		joined:   time.Now(),
	}
	p.cond = sync.NewCond(&p.qmu)
	h.parts[p.identity] = p
	h.mu.Unlock()
	go p.deliverLoop()
	return p, nil
}

func (h *MemoryHub) targets(sender string, destinations []string) []*memParticipant {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(destinations) == 0 {
		out := make([]*memParticipant, 0, len(h.parts))
		for id, p := range h.parts {
			if id != sender {
				out = append(out, p)
			}
		}
		return out
	}
	out := make([]*memParticipant, 0, len(destinations))
	for _, id := range destinations {
		if p := h.parts[id]; p != nil && id != sender {
			out = append(out, p)
		}
	}
	return out
}

type memPacket struct {
	sender string
	topic  string
	data   []byte
}

type memParticipant struct {
	hub      *MemoryHub
	identity string
	cb       *lksdk.RoomCallback
	joined   time.Time

	qmu    sync.Mutex
	cond   *sync.Cond
	queue     []memPacket
	closed    bool
	awayUntil time.Time

	leftSink atomic.Pointer[func(string)]

	rateMu   sync.Mutex
	nextFree time.Time
}

// pace blocks long enough to keep this participant's uplink under
// MemoryHub.UplinkBitsPerSec. Idle time earns no burst credit.
func (p *memParticipant) pace(n int) {
	rate := p.hub.UplinkBitsPerSec
	if rate <= 0 {
		return
	}
	cost := time.Duration(int64(n) * 8 * int64(time.Second) / rate)
	p.rateMu.Lock()
	now := time.Now()
	if p.nextFree.Before(now) {
		p.nextFree = now
	}
	p.nextFree = p.nextFree.Add(cost)
	wait := p.nextFree.Sub(now)
	p.rateMu.Unlock()
	if wait > 2*time.Millisecond {
		time.Sleep(wait)
	}
}

func (p *memParticipant) publishData(data []byte, topic string, destinations []string) error {
	p.qmu.Lock()
	closed := p.closed
	p.qmu.Unlock()
	if closed {
		return ErrRoomNotConnected
	}
	p.pace(len(data))
	// The SDK serialises the payload into a protobuf, so the caller may reuse
	// its buffer after the call returns.
	payload := append([]byte(nil), data...)
	for _, t := range p.hub.targets(p.identity, destinations) {
		t.enqueue(memPacket{sender: p.identity, topic: topic, data: payload})
	}
	return nil
}

func (p *memParticipant) enqueue(pkt memPacket) {
	if d := p.hub.SubscribeDelay; d > 0 && time.Since(p.joined) < d {
		return
	}
	p.qmu.Lock()
	if !p.closed && !time.Now().Before(p.awayUntil) {
		p.queue = append(p.queue, pkt)
		p.cond.Signal()
	}
	p.qmu.Unlock()
}

func (p *memParticipant) deliverLoop() {
	var batch []memPacket
	for {
		p.qmu.Lock()
		for len(p.queue) == 0 && !p.closed {
			p.cond.Wait()
		}
		if p.closed {
			p.qmu.Unlock()
			return
		}
		batch, p.queue = p.queue, batch[:0]
		p.qmu.Unlock()

		for i := range batch {
			if p.cb != nil && p.cb.OnDataReceived != nil {
				p.cb.OnDataReceived(batch[i].data, lksdk.DataReceiveParams{
					SenderIdentity: batch[i].sender,
					Topic:          batch[i].topic,
				})
			}
			batch[i] = memPacket{}
		}
	}
}

func (p *memParticipant) disconnect() {
	p.hub.mu.Lock()
	if p.hub.parts[p.identity] != p {
		p.hub.mu.Unlock()
		return
	}
	delete(p.hub.parts, p.identity)
	others := make([]*memParticipant, 0, len(p.hub.parts))
	for _, o := range p.hub.parts {
		others = append(others, o)
	}
	p.hub.mu.Unlock()

	p.qmu.Lock()
	p.closed = true
	p.queue = nil
	p.cond.Broadcast()
	p.qmu.Unlock()

	for _, o := range others {
		if sink := o.leftSink.Load(); sink != nil {
			go (*sink)(p.identity)
		}
	}
}

func (p *memParticipant) setPeerLeftSink(fn func(identity string)) { p.leftSink.Store(&fn) }

func (p *memParticipant) publishTrack(webrtc.TrackLocal) error { return nil }

func (p *memParticipant) unpublishLocalTracks() {}

func (p *memParticipant) connectionState() lksdk.ConnectionState {
	p.qmu.Lock()
	defer p.qmu.Unlock()
	if p.closed {
		return lksdk.ConnectionStateDisconnected
	}
	return lksdk.ConnectionStateConnected
}

func (p *memParticipant) localIdentity() string { return p.identity }
