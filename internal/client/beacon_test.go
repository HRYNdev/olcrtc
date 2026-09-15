package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

const beaconClientKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

type dirLinkStub struct {
	closerLinkStub
	mu     sync.Mutex
	pinned []string
}

func (l *dirLinkStub) SupportsRoomDirectory() bool                   { return true }
func (l *dirLinkStub) LocalPeerID() string                           { return "PA_client" }
func (l *dirLinkStub) Announce([]byte) error                         { return nil }
func (l *dirLinkStub) SetAnnounceHandler(func(string, []byte))       { return }
func (l *dirLinkStub) SetPeerLeftHandler(func(string))               {}
func (l *dirLinkStub) PinPeer(peerID string) {
	l.mu.Lock()
	l.pinned = append(l.pinned, peerID)
	l.mu.Unlock()
}

func (l *dirLinkStub) pins() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.pinned...)
}

var _ transport.RoomDirectory = (*dirLinkStub)(nil)

func newBeaconTestClient(t *testing.T) (*Client, *dirLinkStub) {
	t.Helper()
	cipher, err := setupCipher(beaconClientKey)
	if err != nil {
		t.Fatal(err)
	}
	link := &dirLinkStub{}
	c := &Client{cipher: cipher, ln: link, roomDir: link}
	c.beaconSeen.Store(make(chan struct{}, 1))
	c.inboundReady.Store(make(chan struct{}, 1))
	return c, link
}

func TestServerBeaconPinsServerParticipant(t *testing.T) {
	c, link := newBeaconTestClient(t)
	beacon, err := runtime.EncodeServerBeacon(c.cipher, "PA_server")
	if err != nil {
		t.Fatal(err)
	}
	c.onServerBeacon(context.Background(), Config{}, func() {}, "PA_server", beacon)
	c.onServerBeacon(context.Background(), Config{}, func() {}, "PA_server", beacon)

	if got := c.ServerPeer(); got != "PA_server" {
		t.Fatalf("ServerPeer() = %q, want PA_server", got)
	}
	if got := link.pins(); len(got) != 1 || got[0] != "PA_server" {
		t.Fatalf("pins = %v, want a single pin to PA_server", got)
	}
	if !c.waitServerBeacon(context.Background()) {
		t.Fatal("waitServerBeacon() = false after a beacon")
	}
}

func TestServerBeaconIgnoresForgedAndForeign(t *testing.T) {
	c, link := newBeaconTestClient(t)
	valid, _ := runtime.EncodeServerBeacon(c.cipher, "PA_server")
	otherCipher, _ := setupCipher("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100")
	foreign, _ := runtime.EncodeServerBeacon(otherCipher, "PA_foreign_server")

	c.onServerBeacon(context.Background(), Config{}, func() {}, "PA_other_client", valid) // replayed under another name
	c.onServerBeacon(context.Background(), Config{}, func() {}, "PA_foreign_server", foreign)
	c.onServerBeacon(context.Background(), Config{}, func() {}, "PA_browser", []byte("chat message"))

	if got := c.ServerPeer(); got != "" {
		t.Fatalf("ServerPeer() = %q, want none", got)
	}
	if got := link.pins(); len(got) != 0 {
		t.Fatalf("pins = %v, want none", got)
	}
}

func TestWaitServerBeaconFallsBackForLegacyServer(t *testing.T) {
	old := legacyBeaconGrace
	legacyBeaconGrace = 100 * time.Millisecond
	defer func() { legacyBeaconGrace = old }()

	c, link := newBeaconTestClient(t)
	c.onData(nil) // a packet (old server keepalive) proves the downstream is live
	start := time.Now()
	if c.waitServerBeacon(context.Background()) {
		t.Fatal("waitServerBeacon() = true without a beacon")
	}
	if waited := time.Since(start); waited < legacyBeaconGrace || waited > 5*time.Second {
		t.Fatalf("waited %s, want about the %s grace", waited, legacyBeaconGrace)
	}
	if got := link.pins(); len(got) != 0 {
		t.Fatalf("legacy fallback pinned %v", got)
	}
}

func TestWaitServerBeaconReturnsWhenBeaconArrives(t *testing.T) {
	c, _ := newBeaconTestClient(t)
	beacon, _ := runtime.EncodeServerBeacon(c.cipher, "PA_server")
	go func() {
		time.Sleep(50 * time.Millisecond)
		c.onData(nil)
		time.Sleep(20 * time.Millisecond)
		c.onServerBeacon(context.Background(), Config{}, func() {}, "PA_server", beacon)
	}()
	if !c.waitServerBeacon(context.Background()) {
		t.Fatal("waitServerBeacon() = false, want addressed mode")
	}
	if c.ServerPeer() != "PA_server" {
		t.Fatalf("ServerPeer() = %q", c.ServerPeer())
	}
}
