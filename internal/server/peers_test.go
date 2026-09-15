package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/xtaci/smux"
)

const peersTestKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

// fakeRoomLink is the server side of an emulated LiveKit room: SendTo routes
// to one participant, Send counts broadcasts, leave() fires participant-left.
type fakeRoomLink struct {
	mu        sync.Mutex
	localID   string
	routes    map[string]func([]byte)
	leftCB    func(string)
	announces [][]byte
	broadcast int
}

func newFakeRoomLink() *fakeRoomLink {
	return &fakeRoomLink{localID: "PA_server", routes: make(map[string]func([]byte))}
}

func (l *fakeRoomLink) Connect(context.Context) error { return nil }
func (l *fakeRoomLink) Send([]byte) error {
	l.mu.Lock()
	l.broadcast++
	l.mu.Unlock()
	return nil
}

func (l *fakeRoomLink) SendTo(peerID string, data []byte) error {
	l.mu.Lock()
	route := l.routes[peerID]
	l.mu.Unlock()
	if route != nil {
		route(append([]byte(nil), data...))
	}
	return nil
}
func (l *fakeRoomLink) SupportsPeerRouting() bool       { return true }
func (l *fakeRoomLink) Close() error                    { return nil }
func (l *fakeRoomLink) SetReconnectCallback(func())     {}
func (l *fakeRoomLink) SetShouldReconnect(func() bool)  {}
func (l *fakeRoomLink) SetEndedCallback(func(string))   {}
func (l *fakeRoomLink) WatchConnection(context.Context) {}
func (l *fakeRoomLink) CanSend() bool                   { return true }
func (l *fakeRoomLink) Reconnect(string)                {}
func (l *fakeRoomLink) Features() transport.Features {
	return transport.Features{Reliable: true, Ordered: true, MessageOriented: true, MaxPayloadSize: 12 * 1024}
}
func (l *fakeRoomLink) SupportsRoomDirectory() bool                   { return true }
func (l *fakeRoomLink) LocalPeerID() string                           { return l.localID }
func (l *fakeRoomLink) SetAnnounceHandler(func(string, []byte))       {}
func (l *fakeRoomLink) PinPeer(string)                                {}
func (l *fakeRoomLink) SetPeerLeftHandler(cb func(peerID string))     { l.mu.Lock(); l.leftCB = cb; l.mu.Unlock() }
func (l *fakeRoomLink) Announce(data []byte) error {
	l.mu.Lock()
	l.announces = append(l.announces, append([]byte(nil), data...))
	l.mu.Unlock()
	return nil
}

func (l *fakeRoomLink) leave(peerID string) {
	l.mu.Lock()
	delete(l.routes, peerID)
	cb := l.leftCB
	l.mu.Unlock()
	if cb != nil {
		cb(peerID)
	}
}

// clientLink is one participant's uplink: every Send arrives at the server
// as data from that participant.
type clientLink struct {
	id  string
	srv *Server
}

func (l *clientLink) Send(data []byte) error {
	l.srv.onPeerData(l.id, append([]byte(nil), data...))
	return nil
}
func (l *clientLink) Connect(context.Context) error   { return nil }
func (l *clientLink) Close() error                    { return nil }
func (l *clientLink) SetReconnectCallback(func())     {}
func (l *clientLink) SetShouldReconnect(func() bool)  {}
func (l *clientLink) SetEndedCallback(func(string))   {}
func (l *clientLink) WatchConnection(context.Context) {}
func (l *clientLink) CanSend() bool                   { return true }
func (l *clientLink) Reconnect(string)                {}
func (l *clientLink) Features() transport.Features {
	return transport.Features{Reliable: true, Ordered: true, MessageOriented: true, MaxPayloadSize: 12 * 1024}
}

type closeEvent struct{ sid, reason string }

type peerTestServer struct {
	*Server
	link   *fakeRoomLink
	opens  chan string
	closes chan closeEvent
}

func newPeerTestServer(t *testing.T) *peerTestServer {
	t.Helper()
	cipher, err := setupCipher(peersTestKey)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	link := newFakeRoomLink()
	pts := &peerTestServer{
		link:   link,
		opens:  make(chan string, 128),
		closes: make(chan closeEvent, 128),
	}
	s := &Server{
		baseCtx:      ctx,
		ln:           link,
		peerLn:       link,
		roomDir:      link,
		cipher:       cipher,
		authHook:     defaultAuthHook,
		onOpen:       func(sid, _ string, _ map[string]any) { pts.opens <- sid },
		onClose:      func(sid, reason string) { pts.closes <- closeEvent{sid, reason} },
		onTraffic:    func(string, string, uint64, uint64) {},
		health:       runtime.NewHealthTracker(nil),
		peerSessions: make(map[string]*peerSession),
		peerStats:    make(map[string]peerStat),
		done:         make(chan struct{}),
	}
	link.SetPeerLeftHandler(s.onPeerLeft)
	pts.Server = s
	t.Cleanup(func() {
		cancel()
		s.shutdown()
		s.wg.Wait()
	})
	return pts
}

type testPeer struct {
	id   string
	sid  string
	sess *smux.Session
}

func (pts *peerTestServer) join(t *testing.T, id string) *testPeer {
	t.Helper()
	cipher, err := setupCipher(peersTestKey)
	if err != nil {
		t.Fatal(err)
	}
	conn := muxconn.New(&clientLink{id: id, srv: pts.Server}, cipher)
	pts.link.mu.Lock()
	pts.link.routes[id] = conn.Push
	pts.link.mu.Unlock()
	sess, err := smux.Client(conn, runtime.SmuxConfigFor(pts.link))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	sid, err := handshake.Client(stream, "device-"+id, nil)
	if err != nil {
		t.Fatalf("handshake %s: %v", id, err)
	}
	_ = stream.SetDeadline(time.Time{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = control.Run(ctx, stream, control.Config{}) }()
	t.Cleanup(func() {
		cancel()
		_ = sess.Close()
		_ = conn.Close()
	})
	return &testPeer{id: id, sid: sid, sess: sess}
}

func startPeersEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

func (p *testPeer) echo(addr string, payload []byte) error {
	host, portText, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portText)
	stream, err := p.sess.OpenStream()
	if err != nil {
		return fmt.Errorf("%s open: %w", p.id, err)
	}
	defer func() { _ = stream.Close() }()
	req, _ := json.Marshal(ConnectRequest{Cmd: connectCommand, Addr: host, Port: port})
	if _, err := stream.Write(req); err != nil {
		return fmt.Errorf("%s connect: %w", p.id, err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(15 * time.Second))
	ack := make([]byte, 1)
	if _, err := io.ReadFull(stream, ack); err != nil || ack[0] != 0 {
		return fmt.Errorf("%s ack: %v %v", p.id, err, ack)
	}
	werr := make(chan error, 1)
	go func() {
		_, err := stream.Write(payload)
		werr <- err
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil {
		return fmt.Errorf("%s read echo: %w", p.id, err)
	}
	if err := <-werr; err != nil {
		return fmt.Errorf("%s write: %w", p.id, err)
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("%s: echo does not match its own payload", p.id)
	}
	return nil
}

func uniquePayload(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPeerSessionsIsolatedFourPeers(t *testing.T) {
	pts := newPeerTestServer(t)
	echoAddr := startPeersEcho(t)

	peers := make([]*testPeer, 4)
	var wg sync.WaitGroup
	for i := range peers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			peers[i] = pts.join(t, fmt.Sprintf("PA_client%d", i))
		}(i)
	}
	wg.Wait()
	if n := pts.peerCount(); n != 4 {
		t.Fatalf("peer sessions = %d, want 4", n)
	}

	errs := make(chan error, len(peers))
	for _, p := range peers {
		go func(p *testPeer) {
			var err error
			for round := 0; round < 3 && err == nil; round++ {
				err = p.echo(echoAddr, uniquePayload(t, 256<<10))
			}
			errs <- err
		}(p)
	}
	for range peers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	pts.link.mu.Lock()
	broadcasts := pts.link.broadcast
	pts.link.mu.Unlock()
	if broadcasts != 0 {
		t.Fatalf("server broadcast %d data frame(s) in peer mode, want 0", broadcasts)
	}
	select {
	case ev := <-pts.closes:
		t.Fatalf("unexpected session close %+v", ev)
	default:
	}
}

func TestPeerLeftClosesOnlyThatPeerAndFifthJoins(t *testing.T) {
	pts := newPeerTestServer(t)
	echoAddr := startPeersEcho(t)

	peers := make([]*testPeer, 4)
	for i := range peers {
		peers[i] = pts.join(t, fmt.Sprintf("PA_client%d", i))
	}

	// Keep the other three busy while one participant leaves.
	stop := make(chan struct{})
	errs := make(chan error, 3)
	for _, p := range []*testPeer{peers[0], peers[2], peers[3]} {
		go func(p *testPeer) {
			for {
				select {
				case <-stop:
					errs <- nil
					return
				default:
				}
				if err := p.echo(echoAddr, uniquePayload(t, 64<<10)); err != nil {
					errs <- err
					return
				}
			}
		}(p)
	}

	time.Sleep(200 * time.Millisecond)
	pts.link.leave(peers[1].id)

	select {
	case ev := <-pts.closes:
		if ev.sid != peers[1].sid || ev.reason != "left" {
			t.Fatalf("close event %+v, want sid %s reason left", ev, peers[1].sid)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session of the departed participant was not closed")
	}
	if n := pts.peerCount(); n != 3 {
		t.Fatalf("peer sessions after leave = %d, want 3", n)
	}

	fifth := pts.join(t, "PA_client4")
	if err := fifth.echo(echoAddr, uniquePayload(t, 128<<10)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatalf("remaining peer disturbed by leave/join: %v", err)
		}
	}
	if n := pts.peerCount(); n != 4 {
		t.Fatalf("peer sessions after fifth join = %d, want 4", n)
	}
	select {
	case ev := <-pts.closes:
		t.Fatalf("unexpected extra close %+v", ev)
	default:
	}
}

func TestUndecryptableParticipantGetsNoSession(t *testing.T) {
	pts := newPeerTestServer(t)
	for range 50 {
		pts.onPeerData("PA_browser", uniquePayload(t, 200))
	}
	other, _ := setupCipher("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100")
	foreign, _ := other.Encrypt([]byte{2, 0, 0, 0, 1, 0, 0, 0})
	pts.onPeerData("PA_other_olcrtc", foreign)
	if n := pts.peerCount(); n != 0 {
		t.Fatalf("peer sessions = %d, want 0", n)
	}
}

func TestPeerLimit(t *testing.T) {
	pts := newPeerTestServer(t)
	nop, _ := pts.cipher.Encrypt([]byte{2, 3, 0, 0, 0, 0, 0, 0}) // smux v2 NOP
	for i := range maxPeerSessions + 5 {
		pts.onPeerData(fmt.Sprintf("PA_%d", i), nop)
	}
	if n := pts.peerCount(); n != maxPeerSessions {
		t.Fatalf("peer sessions = %d, want cap %d", n, maxPeerSessions)
	}
}

func TestStaleRemovalKeepsNewerSession(t *testing.T) {
	pts := newPeerTestServer(t)
	nop, _ := pts.cipher.Encrypt([]byte{2, 3, 0, 0, 0, 0, 0, 0})
	pts.onPeerData("PA_a", nop)
	pts.sessMu.RLock()
	old := pts.peerSessions["PA_a"]
	pts.sessMu.RUnlock()
	if old == nil {
		t.Fatal("no session created")
	}
	pts.onPeerLeft("PA_a")
	pts.onPeerData("PA_a", nop) // participant rejoined with the same identity
	pts.removePeerSessionIf(old, "closed")

	pts.sessMu.RLock()
	cur := pts.peerSessions["PA_a"]
	pts.sessMu.RUnlock()
	if cur == nil || cur == old {
		t.Fatalf("newer session removed by a goroutine of the old one (cur=%p old=%p)", cur, old)
	}
}

func TestCarrierReconnectInPeerModeClosesPeersOnly(t *testing.T) {
	pts := newPeerTestServer(t)
	pts.join(t, "PA_client0")
	pts.join(t, "PA_client1")
	pts.handleReconnect()
	// Frames the clients still send after the close (smux FIN/keepalive) may
	// open fresh sessions, but none of them may carry a handshake: the old
	// sessions are gone and a client has to re-handshake.
	pts.sessMu.RLock()
	for id, ps := range pts.peerSessions {
		if ps.sessionID != "" {
			pts.sessMu.RUnlock()
			t.Fatalf("peer %s kept an established session across carrier reconnect", id)
		}
	}
	stray := pts.session
	pts.sessMu.RUnlock()
	if stray != nil {
		t.Fatal("singleton smux session built in peer mode (would broadcast keepalives to the room)")
	}
	for range 2 {
		select {
		case ev := <-pts.closes:
			if ev.reason != "reconnect" {
				t.Fatalf("close reason %q, want reconnect", ev.reason)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("peer session close not reported")
		}
	}
}

func TestServerAnnouncesVerifiableBeacon(t *testing.T) {
	pts := newPeerTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pts.announceLoop(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pts.link.mu.Lock()
		n := len(pts.link.announces)
		var first []byte
		if n > 0 {
			first = pts.link.announces[0]
		}
		pts.link.mu.Unlock()
		if n > 0 {
			if !runtime.VerifyServerBeacon(pts.cipher, "PA_server", first) {
				t.Fatal("server beacon does not verify for its own identity")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no beacon announced")
}
