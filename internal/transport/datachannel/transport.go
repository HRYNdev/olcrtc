// Package datachannel provides a transport backed by a carrier's data channel.
package datachannel

import (
	"context"
	"errors"
	"fmt"

	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/transport/common"
)

const defaultMaxPayloadSize = 12 * 1024

// ErrByteStreamUnsupported is returned when a carrier engine cannot expose a byte stream.
var ErrByteStreamUnsupported = errors.New("engine does not support byte stream")

// PeerResetter is satisfied so upper layers can clear the peer binding.
var _ transport.PeerResetter = (*streamTransport)(nil)

type streamTransport struct {
	common.Lifecycle

	session engine.Session
	shaper  *transport.Shaper
}

// New creates a datachannel transport backed by a carrier engine.
func New(ctx context.Context, cfg transport.Config) (transport.Transport, error) {
	sess, err := cfg.OpenEngine(ctx)
	if err != nil {
		return nil, err
	}

	if !sess.Capabilities().ByteStream {
		_ = sess.Close()
		return nil, ErrByteStreamUnsupported
	}

	tr := &streamTransport{Lifecycle: common.NewLifecycle(sess), session: sess}
	tr.shaper = transport.NewShaper(cfg.Traffic, tr.Features())

	return tr, nil
}

// Connect starts the transport connection.
func (p *streamTransport) Connect(ctx context.Context) error {
	if err := p.session.Connect(ctx); err != nil {
		return fmt.Errorf("session connect: %w", err)
	}
	return nil
}

// Send transmits data through the transport.
func (p *streamTransport) Send(data []byte) error {
	return p.shaper.Send(p.send, data)
}

func (p *streamTransport) send(data []byte) error {
	if err := p.session.Send(data); err != nil {
		return fmt.Errorf("session send: %w", err)
	}
	return nil
}

// SendTo transmits data to a specific remote endpoint when the engine supports it.
func (p *streamTransport) SendTo(peerID string, data []byte) error {
	return p.shaper.Send(func(payload []byte) error {
		return p.sendTo(peerID, payload)
	}, data)
}

func (p *streamTransport) sendTo(peerID string, data []byte) error {
	peer, ok := p.session.(engine.PeerSession)
	if !ok {
		return p.send(data)
	}
	if err := peer.SendTo(peerID, data); err != nil {
		return fmt.Errorf("session send to peer: %w", err)
	}
	return nil
}

// SupportsPeerRouting reports whether this transport can address individual peers.
func (p *streamTransport) SupportsPeerRouting() bool {
	_, ok := p.session.(engine.PeerSession)
	return ok
}

// Close terminates the transport.
func (p *streamTransport) Close() error {
	if err := p.session.Close(); err != nil {
		return fmt.Errorf("session close: %w", err)
	}
	return nil
}

// ResetPeer clears peer binding on engines that expose it.
func (p *streamTransport) ResetPeer() {
	if resetter, ok := p.session.(interface{ ResetPeer() }); ok {
		resetter.ResetPeer()
	}
}

// SetReconnectCallback registers reconnect handling.
func (p *streamTransport) SetReconnectCallback(cb func()) {
	p.session.SetReconnectCallback(func(*webrtc.DataChannel) {
		if cb != nil {
			cb()
		}
	})
}

// CanSend reports whether transport is ready for sending.
func (p *streamTransport) CanSend() bool {
	return p.session.CanSend()
}

// WaitForPeer blocks until the remote peer is confirmed ready, or ctx expires.
// Implements transport.PeerReadyTransport.
func (p *streamTransport) WaitForPeer(ctx context.Context) error {
	waiter, ok := p.session.(engine.PeerReadySession)
	if !ok {
		return nil
	}
	if err := waiter.WaitForPeer(ctx); err != nil {
		return fmt.Errorf("wait for peer: %w", err)
	}
	return nil
}

// Features describes the current datachannel transport semantics.
func (p *streamTransport) Features() transport.Features {
	return p.shaper.Features(transport.Features{MaxPayloadSize: defaultMaxPayloadSize})
}
