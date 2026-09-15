package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/xtaci/smux"
)

// recordingLink captures every record a muxconn sends.
type recordingLink struct {
	pipeLink
	mu   sync.Mutex
	sent [][]byte
}

func (l *recordingLink) Send(data []byte) error {
	l.mu.Lock()
	l.sent = append(l.sent, append([]byte(nil), data...))
	l.mu.Unlock()
	return nil
}

func (l *recordingLink) Features() transport.Features { return l.pipeLink.Features() }

// The server admits a new peer session only on the frame a real smux client
// sends first. Pin that frame against the smux version in go.mod, and check
// that later frames (a tunnel stream) are not mistaken for it.
func TestSmuxSessionStartMatchesRealClient(t *testing.T) {
	c, err := SetupCipher(beaconTestKey)
	if err != nil {
		t.Fatal(err)
	}
	link := &recordingLink{}
	conn := muxconn.New(link, c)
	sess, err := smux.Client(conn, SmuxConfigFor(link))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	records := func() [][]byte {
		link.mu.Lock()
		defer link.mu.Unlock()
		return append([][]byte(nil), link.sent...)
	}
	waitRecords := func(n int) [][]byte {
		for ctx.Err() == nil {
			if got := records(); len(got) >= n {
				return got
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("smux sent %d record(s), want %d", len(records()), n)
		return nil
	}

	control, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.Decrypt(waitRecords(1)[0])
	if err != nil {
		t.Fatal(err)
	}
	if !IsSmuxSessionStart(first) {
		t.Fatalf("first record of a smux client % x is not recognised as a session start", first)
	}
	if control.ID() != smuxControlSID {
		t.Fatalf("first client stream id = %d, want %d", control.ID(), smuxControlSID)
	}

	if _, err := sess.OpenStream(); err != nil {
		t.Fatal(err)
	}
	tunnel, err := c.Decrypt(waitRecords(2)[1])
	if err != nil {
		t.Fatal(err)
	}
	if IsSmuxSessionStart(tunnel) {
		t.Fatalf("tunnel stream SYN % x accepted as a session start", tunnel)
	}
	if IsSmuxSessionStart(SmuxControlFIN()) {
		t.Fatal("FIN accepted as a session start")
	}
}
