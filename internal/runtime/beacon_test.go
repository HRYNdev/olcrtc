package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/xtaci/smux"
)

const (
	beaconTestKey  = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	beaconOtherKey = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
)

func TestServerBeaconVerify(t *testing.T) {
	c, err := SetupCipher(beaconTestKey)
	if err != nil {
		t.Fatal(err)
	}
	other, err := SetupCipher(beaconOtherKey)
	if err != nil {
		t.Fatal(err)
	}
	beacon, err := EncodeServerBeacon(c, "PA_server")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyServerBeacon(c, "PA_server", beacon) {
		t.Fatal("valid beacon rejected")
	}
	if VerifyServerBeacon(c, "PA_replayer", beacon) {
		t.Fatal("beacon accepted from a participant it was not issued for")
	}
	if VerifyServerBeacon(other, "PA_server", beacon) {
		t.Fatal("beacon accepted under another key")
	}
	if VerifyServerBeacon(c, "", beacon) {
		t.Fatal("beacon accepted without sender identity")
	}
	tampered := append([]byte(nil), beacon...)
	tampered[len(tampered)-1] ^= 1
	if VerifyServerBeacon(c, "PA_server", tampered) {
		t.Fatal("tampered beacon accepted")
	}
	// An ordinary data record under the same key is not a beacon.
	data, _ := c.Encrypt([]byte{2, 2, 5, 0, 1, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'})
	if VerifyServerBeacon(c, "PA_server", data) {
		t.Fatal("data record accepted as beacon")
	}
}

// pipeLink is a message link between two muxconns in one process.
type pipeLink struct {
	mu   sync.Mutex
	peer *muxconn.Conn
}

func (l *pipeLink) Send(data []byte) error {
	l.mu.Lock()
	peer := l.peer
	l.mu.Unlock()
	peer.Push(append([]byte(nil), data...))
	return nil
}
func (l *pipeLink) Connect(context.Context) error   { return nil }
func (l *pipeLink) Close() error                    { return nil }
func (l *pipeLink) SetReconnectCallback(func())     {}
func (l *pipeLink) SetShouldReconnect(func() bool)  {}
func (l *pipeLink) SetEndedCallback(func(string))   {}
func (l *pipeLink) WatchConnection(context.Context) {}
func (l *pipeLink) CanSend() bool                   { return true }
func (l *pipeLink) Reconnect(string)                {}
func (l *pipeLink) Features() transport.Features {
	return transport.Features{Reliable: true, Ordered: true, MessageOriented: true, MaxPayloadSize: 12 * 1024}
}

// TestServerBeaconIsHarmlessForLegacySmux is the compatibility guarantee for
// clients built before addressed mode: they push every packet, beacons
// included, into their smux session. The beacon must decrypt into a frame
// smux silently ignores, so a transfer running while beacons arrive stays
// intact.
func TestServerBeaconIsHarmlessForLegacySmux(t *testing.T) {
	serverCipher, _ := SetupCipher(beaconTestKey)
	clientCipher, _ := SetupCipher(beaconTestKey)
	toClient, toServer := &pipeLink{}, &pipeLink{}
	serverConn := muxconn.New(toClient, serverCipher)
	clientConn := muxconn.New(toServer, clientCipher)
	toClient.peer = clientConn
	toServer.peer = serverConn

	srv, err := smux.Server(serverConn, SmuxConfigFor(toClient))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close() }()
	cli, err := smux.Client(clientConn, SmuxConfigFor(toServer))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	stop := make(chan struct{})
	var beacons sync.WaitGroup
	beacons.Add(1)
	go func() {
		defer beacons.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			b, err := EncodeServerBeacon(serverCipher, "PA_server")
			if err != nil {
				t.Error(err)
				return
			}
			clientConn.Push(b)
		}
	}()

	go func() {
		st, err := srv.AcceptStream()
		if err != nil {
			return
		}
		_, _ = io.Copy(st, st)
	}()

	payload := make([]byte, 2<<20)
	_, _ = rand.Read(payload)
	st, err := cli.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write(payload) }()
	got := make([]byte, len(payload))
	_ = st.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(st, got); err != nil {
		t.Fatalf("echo read: %v (client session closed=%v)", err, cli.IsClosed())
	}
	close(stop)
	beacons.Wait()
	if !bytes.Equal(got, payload) {
		t.Fatal("echo mismatch while beacons were injected")
	}
	if cli.IsClosed() {
		t.Fatal("legacy smux session closed by beacons")
	}
}
