package livekit

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	lksdk "github.com/owenewans/owenlivekit/v2"
)

type recordedData struct {
	peer string
	data []byte
}

type dataRecorder struct {
	mu   sync.Mutex
	got  []recordedData
	wake chan struct{}
}

func newDataRecorder() *dataRecorder { return &dataRecorder{wake: make(chan struct{}, 1024)} }

func (r *dataRecorder) onPeerData(peer string, data []byte) {
	r.mu.Lock()
	r.got = append(r.got, recordedData{peer: peer, data: append([]byte(nil), data...)})
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *dataRecorder) onData(data []byte) { r.onPeerData("", data) }

func (r *dataRecorder) snapshot() []recordedData {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedData(nil), r.got...)
}

func connectFakeSession(t *testing.T, cfg engine.Config) (*Session, *fakeConnector) {
	t.Helper()
	cfg.URL, cfg.Token = testOldURL, testOldToken
	sess, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	s, ok := sess.(*Session)
	if !ok {
		t.Fatalf("New() type = %T, want *Session", sess)
	}
	connector := newFakeConnector()
	s.connectRoom = connector.connect
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, connector
}

func (r *fakeRoom) packet(i int) ([]byte, string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.published[i], r.topics[i], r.destinations[i]
}

func (r *fakeRoom) publishedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.published)
}

func TestSendToAddressesOneParticipant(t *testing.T) {
	s, connector := connectFakeSession(t, engine.Config{})
	if err := s.SendTo("PA_client2", []byte("hello")); err != nil {
		t.Fatalf("SendTo() error = %v", err)
	}
	room := connector.room(0)
	waitFor(t, func() bool { return room.publishedCount() == 1 })
	data, topic, dest := room.packet(0)
	if !bytes.Equal(data, []byte("hello")) || topic != dataPublishTopic {
		t.Fatalf("packet = %q topic %q, want hello on %q", data, topic, dataPublishTopic)
	}
	if len(dest) != 1 || dest[0] != "PA_client2" {
		t.Fatalf("destinations = %v, want [PA_client2]", dest)
	}
}

func TestSendFollowsPinnedPeer(t *testing.T) {
	s, connector := connectFakeSession(t, engine.Config{})
	room := connector.room(0)

	send := func(want []string) {
		t.Helper()
		before := room.publishedCount()
		if err := s.Send([]byte("x")); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
		waitFor(t, func() bool { return room.publishedCount() == before+1 })
		_, _, dest := room.packet(before)
		if len(dest) != len(want) || (len(want) == 1 && dest[0] != want[0]) {
			t.Fatalf("destinations = %v, want %v", dest, want)
		}
	}

	send(nil) // unpinned: broadcast, as before addressed mode
	s.PinPeer("PA_server")
	send([]string{"PA_server"})
	s.PinPeer("")
	send(nil)
}

func TestAnnounceIsBroadcastOnBeaconTopic(t *testing.T) {
	s, connector := connectFakeSession(t, engine.Config{})
	s.PinPeer("PA_server") // pinning must not address announces
	if err := s.Announce([]byte("beacon")); err != nil {
		t.Fatalf("Announce() error = %v", err)
	}
	room := connector.room(0)
	waitFor(t, func() bool { return room.publishedCount() == 1 })
	_, topic, dest := room.packet(0)
	if topic != announceTopic || len(dest) != 0 {
		t.Fatalf("announce topic=%q dest=%v, want %q broadcast", topic, dest, announceTopic)
	}
}

func TestReceiveReportsSenderIdentity(t *testing.T) {
	rec := newDataRecorder()
	legacy := newDataRecorder()
	_, connector := connectFakeSession(t, engine.Config{OnPeerData: rec.onPeerData, OnData: legacy.onData})
	connector.callback(0).OnDataReceived([]byte("frame"), lksdk.DataReceiveParams{
		SenderIdentity: "PA_client7", Topic: dataPublishTopic,
	})
	got := rec.snapshot()
	if len(got) != 1 || got[0].peer != "PA_client7" || string(got[0].data) != "frame" {
		t.Fatalf("OnPeerData got %+v, want one frame from PA_client7", got)
	}
	if n := len(legacy.snapshot()); n != 0 {
		t.Fatalf("OnData called %d times alongside OnPeerData", n)
	}
}

func TestPinnedPeerDropsOtherSenders(t *testing.T) {
	rec := newDataRecorder()
	s, connector := connectFakeSession(t, engine.Config{OnData: rec.onData})
	cb := connector.callback(0)

	// Unpinned: accept from anyone (legacy behaviour).
	cb.OnDataReceived([]byte("a"), lksdk.DataReceiveParams{SenderIdentity: "PA_other", Topic: dataPublishTopic})
	s.PinPeer("PA_server")
	cb.OnDataReceived([]byte("b"), lksdk.DataReceiveParams{SenderIdentity: "PA_other", Topic: dataPublishTopic})
	cb.OnDataReceived([]byte("c"), lksdk.DataReceiveParams{SenderIdentity: "PA_server", Topic: dataPublishTopic})
	cb.OnDataReceived([]byte("d"), lksdk.DataReceiveParams{Topic: dataPublishTopic}) // no identity

	got := rec.snapshot()
	if len(got) != 2 || string(got[0].data) != "a" || string(got[1].data) != "c" {
		t.Fatalf("delivered %+v, want a (before pin) and c (from pinned server)", got)
	}
	if n := s.foreignDropped.Load(); n != 2 {
		t.Fatalf("foreignDropped = %d, want 2", n)
	}
}

func TestAnnounceTopicNeverReachesDataCallbacks(t *testing.T) {
	rec := newDataRecorder()
	announces := newDataRecorder()
	s, connector := connectFakeSession(t, engine.Config{OnData: rec.onData})
	s.SetAnnounceHandler(announces.onPeerData)
	s.PinPeer("PA_server")
	cb := connector.callback(0)
	// Announces from any participant reach the handler, pinned or not.
	cb.OnDataReceived([]byte("b1"), lksdk.DataReceiveParams{SenderIdentity: "PA_new_server", Topic: announceTopic})

	if n := len(rec.snapshot()); n != 0 {
		t.Fatalf("announce leaked into OnData (%d packets)", n)
	}
	got := announces.snapshot()
	if len(got) != 1 || got[0].peer != "PA_new_server" {
		t.Fatalf("announce handler got %+v, want one from PA_new_server", got)
	}
}

func connectHubSession(t *testing.T, hub *MemoryHub, cfg engine.Config) *Session {
	t.Helper()
	sess, ok := hub.NewSession(cfg).(*Session)
	if !ok {
		t.Fatal("hub session is not *Session")
	}
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess
}

func waitRecorded(t *testing.T, r *dataRecorder, n int) []recordedData {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := r.snapshot(); len(got) >= n {
			return got
		}
		select {
		case <-r.wake:
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("recorded %d packets, want %d", len(r.snapshot()), n)
	return nil
}

func TestMemoryHubAddressingAndBroadcast(t *testing.T) {
	hub := NewMemoryHub()
	recA, recB, recC := newDataRecorder(), newDataRecorder(), newDataRecorder()
	a := connectHubSession(t, hub, engine.Config{OnPeerData: recA.onPeerData})
	b := connectHubSession(t, hub, engine.Config{OnPeerData: recB.onPeerData})
	connectHubSession(t, hub, engine.Config{OnPeerData: recC.onPeerData})

	if err := a.SendTo(b.LocalPeerID(), []byte("to-b")); err != nil {
		t.Fatal(err)
	}
	if err := a.Send([]byte("to-all")); err != nil {
		t.Fatal(err)
	}
	gotB := waitRecorded(t, recB, 2)
	gotC := waitRecorded(t, recC, 1)
	time.Sleep(50 * time.Millisecond)

	if string(gotB[0].data) != "to-b" || string(gotB[1].data) != "to-all" || gotB[0].peer != a.LocalPeerID() {
		t.Fatalf("B got %+v", gotB)
	}
	if got := recC.snapshot(); len(got) != 1 || string(gotC[0].data) != "to-all" {
		t.Fatalf("C got %+v, want only the broadcast", got)
	}
	if got := recA.snapshot(); len(got) != 0 {
		t.Fatalf("sender received its own data: %+v", got)
	}
}

func TestMemoryHubReportsParticipantLeft(t *testing.T) {
	hub := NewMemoryHub()
	left := make(chan string, 4)
	a := connectHubSession(t, hub, engine.Config{})
	a.SetPeerLeftHandler(func(id string) { left <- id })
	b := connectHubSession(t, hub, engine.Config{})
	bID := b.LocalPeerID()

	_ = b.Close()
	select {
	case id := <-left:
		if id != bID {
			t.Fatalf("left = %q, want %q", id, bID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("participant-left not reported")
	}
	if got := hub.Participants(); len(got) != 1 || got[0] != a.LocalPeerID() {
		t.Fatalf("participants = %v, want only %s", got, a.LocalPeerID())
	}
}
