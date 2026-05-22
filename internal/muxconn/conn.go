// Package muxconn adapts a link.Link into an io.ReadWriteCloser suitable for
// driving a smux session. The wrapper applies AEAD on every wire-bound write
// and inverts it on every received message before exposing the bytes as a
// byte stream.
//
// Link semantics are message-oriented: each Send produces exactly one OnData
// on the peer. smux operates on a pure byte stream (header + payload may be
// glued or split across reads). We bridge by:
//
//   - Treating each Push as an opaque chunk handed off via a channel that
//     Read drains in arbitrary slices, retaining any tail bytes that did
//     not fit the caller's buffer for the next Read.
//   - Letting smux's sendLoop call Write once per frame; we encrypt and hand
//     the whole buffer to the link as a single message. Length boundaries
//     are preserved end-to-end by the transport (KCP length-prefix framing
//     in vp8channel, native message boundaries in datachannel).
package muxconn

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// ErrClosed is returned from Read/Write after the conn has been closed.
var ErrClosed = errors.New("muxconn: closed")

// inboundQueue is the buffered capacity of the Push -> Read pipeline.
// It absorbs short Read stalls without applying back-pressure to the
// transport callback. Frames are typically smux-sized (well under
// defaultMaxPayloadSize == 12 KiB), so 256 amounts to a few MiB of
// in-flight data, which is enough for sustained throughput on every
// transport we have without unbounded growth on a stuck reader.
const inboundQueue = 256

// Conn is an io.ReadWriteCloser over a [transport.Transport] with optional AEAD wrapping.
//
// Push produces decrypted plaintext frames into an internal channel; Read
// drains the channel and slices each frame across as many caller buffers
// as needed. The hot path is lock-free: a single producer (the transport
// callback) and a single consumer (smux's read loop) communicate via a
// buffered channel without any cond/mutex ping-pong.
type Conn struct {
	ln     transport.Transport
	send   func([]byte) error
	cipher *crypto.Cipher

	in        chan []byte
	closeOnce sync.Once
	closeCh   chan struct{}
	closed    atomic.Bool

	// leftover holds the unread tail of the most recent frame popped
	// from `in`. It is touched only by Read and so needs no
	// synchronization.
	leftover []byte
}

// New wires a Conn over the given transport. Push must be set as the
// transport's OnData callback before this conn is used.
func New(ln transport.Transport, cipher *crypto.Cipher) *Conn {
	return &Conn{
		ln:      ln,
		send:    ln.Send,
		cipher:  cipher,
		in:      make(chan []byte, inboundQueue),
		closeCh: make(chan struct{}),
	}
}

// NewPeer wires a Conn whose writes are addressed to a specific transport peer.
func NewPeer(ln transport.PeerTransport, cipher *crypto.Cipher, peerID string) *Conn {
	return &Conn{
		ln: ln,
		send: func(data []byte) error {
			return ln.SendTo(peerID, data)
		},
		cipher:  cipher,
		in:      make(chan []byte, inboundQueue),
		closeCh: make(chan struct{}),
	}
}

// Push hands an encrypted wire payload (one OnData event) to the conn.
//
// On the producer side: decrypt, then either deliver via the inbound
// channel or, if the caller has Close'd or back-pressure can't drain in
// time, drop the frame. Blocking forever here would wedge the transport
// callback and trip its watchdog, so we cap waiting on closeCh.
func (c *Conn) Push(ciphertext []byte) {
	pt, err := c.cipher.Decrypt(ciphertext)
	if err != nil {
		logger.Debugf("muxconn: decrypt failed, dropping frame: %v", err)
		return
	}
	if c.closed.Load() {
		return
	}
	select {
	case c.in <- pt:
	case <-c.closeCh:
	}
}

// Read implements io.Reader. Blocks until at least one byte is available;
// after that, drains additional ready frames non-blockingly to fill p, so
// a single Read can absorb several queued frames in one go. This matches
// the prior cond/append-based implementation's concatenation behaviour
// and lets smux's bufio reader pull large chunks at a time.
func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(c.leftover) == 0 {
		select {
		case data, ok := <-c.in:
			if !ok {
				return 0, io.EOF
			}
			c.leftover = data
		case <-c.closeCh:
			// Drain any bytes that landed before close so a peer that
			// shut us down right after a final write doesn't lose data.
			select {
			case data := <-c.in:
				c.leftover = data
			default:
				return 0, io.EOF
			}
		}
	}
	n := copy(p, c.leftover)
	c.leftover = c.leftover[n:]

	// Greedily pull additional frames already sitting in the queue,
	// without blocking. This keeps the channel from accumulating a
	// backlog when the consumer asks for a large buffer.
	for n < len(p) && len(c.leftover) == 0 {
		select {
		case data, ok := <-c.in:
			if !ok {
				return n, nil
			}
			m := copy(p[n:], data)
			n += m
			if m < len(data) {
				c.leftover = data[m:]
			}
		default:
			return n, nil
		}
	}
	return n, nil
}

// Write encrypts p and ships it to the link as a single message. Blocks while
// the link signals back-pressure.
func (c *Conn) Write(p []byte) (int, error) {
	// Spin briefly first - on a healthy link CanSend usually clears within
	// well under a millisecond, so a 10ms sleep adds visible per-frame
	// latency to interactive request/response traffic. Fall back to a
	// modest sleep only if the link is truly congested.
	const (
		fastSpinAttempts = 200
		slowPollDelay    = 2 * time.Millisecond
	)
	for attempt := 0; ; attempt++ {
		if c.closed.Load() {
			return 0, ErrClosed
		}
		if c.ln.CanSend() {
			break
		}
		if attempt < fastSpinAttempts {
			runtime.Gosched()
			continue
		}
		time.Sleep(slowPollDelay)
	}

	enc, err := c.cipher.Encrypt(p)
	if err != nil {
		return 0, fmt.Errorf("encrypt: %w", err)
	}
	if err := c.send(enc); err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}
	return len(p), nil
}

// Close unblocks any pending Read with io.EOF.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)
	})
	return nil
}
