package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/framing"
	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

type peerStat struct {
	deviceID string
	openedAt time.Time
}

// peerSession holds one client's independently synchronized peer-routing state.
type peerSession struct {
	peerID       string
	sessionReady chan struct{}
	mu           sync.Mutex
	conn         *muxconn.Conn
	session      *smux.Session
	controlConn  *muxconn.Conn
	controlSess  *smux.Session
	controlStrm  *smux.Stream
	controlStop  context.CancelFunc
	sessionID    string
	deviceID     string
}

func (ps *peerSession) sid() string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.sessionID
}

func (ps *peerSession) setHandshake(result handshakeResult) {
	ps.mu.Lock()
	ps.sessionID = result.sessionID
	ps.deviceID = result.deviceID
	ps.mu.Unlock()
}

func (ps *peerSession) attachData(conn *muxconn.Conn, session *smux.Session) {
	ps.mu.Lock()
	ps.conn = conn
	ps.session = session
	ps.mu.Unlock()
}

func (ps *peerSession) dataConn() *muxconn.Conn {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.conn
}

func (ps *peerSession) dataSession() *smux.Session {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.session
}

func (ps *peerSession) controlPlane() (*muxconn.Conn, *smux.Session) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.controlConn, ps.controlSess
}

func (ps *peerSession) setControl(stream *smux.Stream, stop context.CancelFunc) {
	ps.mu.Lock()
	ps.controlStrm = stream
	ps.controlStop = stop
	ps.mu.Unlock()
}

type teardown struct {
	conn        *muxconn.Conn
	session     *smux.Session
	controlConn *muxconn.Conn
	controlSess *smux.Session
	controlStrm *smux.Stream
	controlStop context.CancelFunc
	sessionID   string
}

func (ps *peerSession) snapshot() teardown {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return teardown{
		conn: ps.conn, session: ps.session, controlConn: ps.controlConn,
		controlSess: ps.controlSess, controlStrm: ps.controlStrm,
		controlStop: ps.controlStop, sessionID: ps.sessionID,
	}
}

func (s *Server) installPeerControlPlane(control transport.PeerControlPlane) {
	control.SetControlOnPeerData(s.onPeerControlData)
}

func (s *Server) onPeerControlData(peerID string, data []byte) {
	peer := s.getOrCreatePeerControlSession(peerID)
	if peer == nil {
		return
	}
	if conn, _ := peer.controlPlane(); conn != nil {
		conn.Push(data)
	}
}

func (s *Server) getOrCreatePeerControlSession(peerID string) *peerSession {
	s.sessMu.Lock()
	peer := s.peerSessions[peerID]
	if peer != nil {
		s.sessMu.Unlock()
		return peer
	}
	if _, ok := s.ln.(transport.PeerControlPlane); !ok {
		s.sessMu.Unlock()
		return nil
	}
	conn := muxconn.NewPeerControlUnbound(s.ln, s.cipher, peerID)
	if conn == nil {
		s.sessMu.Unlock()
		return nil
	}
	session, err := tunnelcore.NewSession(
		conn, tunnelcore.ServerRole, runtime.ControlSmuxConfig(runtime.MaxPayload(s.ln)),
	)
	if err != nil {
		logger.Warnf("control smux init failed for peer %s: %v", peerID, err)
		_ = conn.Close()
		s.sessMu.Unlock()
		return nil
	}
	peer = &peerSession{
		peerID: peerID, sessionReady: make(chan struct{}),
		controlConn: conn, controlSess: session,
	}
	s.peerSessions[peerID] = peer
	s.sessMu.Unlock()
	logger.Infof("server: peer control session created peerID=%s", peerID)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptPeerHandshake(s.baseCtx, peer)
	}()
	return peer
}

func (s *Server) onPeerData(peerID string, data []byte) {
	peer := s.getPeerSession(peerID)
	if peer == nil {
		s.onData(data)
		return
	}
	tunnelcore.PushData(peer.dataConn(), data)
}

func (s *Server) getPeerSession(peerID string) *peerSession {
	if peerID == "" || s.peerLn == nil {
		return nil
	}
	s.sessMu.Lock()
	peer := s.peerSessions[peerID]
	if peer != nil && peer.dataConn() != nil {
		s.sessMu.Unlock()
		return peer
	}
	conn := muxconn.NewPeer(s.peerLn, s.cipher, peerID)
	session, err := tunnelcore.NewSession(conn, tunnelcore.ServerRole, runtime.SmuxConfigFor(s.ln))
	if err != nil {
		s.sessMu.Unlock()
		logger.Warnf("smux server init failed for peer %s: %v", peerID, err)
		_ = conn.Close()
		return nil
	}
	if peer == nil {
		peer = &peerSession{peerID: peerID}
		s.peerSessions[peerID] = peer
	}
	peer.attachData(conn, session)
	s.sessMu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.servePeer(peer)
	}()
	return peer
}

func (s *Server) acceptPeerHandshake(ctx context.Context, peer *peerSession) {
	const maxStaleRetries = 3
	_, session := peer.controlPlane()
	if session == nil {
		return
	}
	for retry := 0; retry <= maxStaleRetries; retry++ {
		stream, err := session.AcceptStream()
		if err != nil {
			if ctx.Err() == nil {
				logger.Infof("server: AcceptStream(peer control=%s) error: %v", peer.peerID, err)
				s.removePeerSession(peer.peerID, "handshake failed")
			}
			return
		}
		_ = stream.SetDeadline(time.Now().Add(handshake.DefaultTimeout))
		hello, sessionID, err := handshake.Server(stream, s.authHook)
		_ = stream.SetDeadline(time.Time{})
		if err != nil {
			_ = stream.Close()
			if errors.Is(err, framing.ErrFrameTooLarge) && retry < maxStaleRetries {
				logger.Debugf("handshake peer=%s: stale stream retry %d: %v", peer.peerID, retry+1, err)
				continue
			}
			logger.Warnf("handshake peer=%s failed: %v", peer.peerID, err)
			s.removePeerSession(peer.peerID, "handshake failed")
			return
		}
		peer.setHandshake(handshakeResult{sessionID: sessionID, deviceID: hello.DeviceID})
		if peer.sessionReady != nil {
			close(peer.sessionReady)
		}
		s.health.RecordSession(sessionID)
		s.onOpen(sessionID, hello.DeviceID, hello.Claims)
		s.trackPeerOpen(sessionID, hello.DeviceID)
		logger.Infof("peer session %s opened (peer=%s device=%s)", sessionID, peer.peerID, hello.DeviceID)
		s.startPeerControlLoop(ctx, peer, stream)
		return
	}
}

func (s *Server) startPeerControlLoop(ctx context.Context, peer *peerSession, stream *smux.Stream) {
	controlCtx, stop := context.WithCancel(ctx)
	peer.setControl(stream, stop)
	runner := tunnelcore.ControlRunner{
		Transport: s.ln, Config: s.liveness, Health: s.health,
		LogFields: func() string { return "role=server peer=" + peer.peerID },
		OnDeath:   func(error) { s.removePeerSession(peer.peerID, "liveness") },
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { _ = stream.Close() }()
		runner.Run(controlCtx, stream)
	}()
}

func (s *Server) servePeer(peer *peerSession) {
	if peer.sid() == "" && !s.establishPeerSession(peer) {
		return
	}
	session := peer.dataSession()
	if session == nil {
		return
	}
	ctx := s.streamContext()
	sessionID := peer.sid()
	for {
		if s.stopping() {
			return
		}
		stream, err := session.AcceptStream()
		if err != nil {
			if !s.stopping() {
				logger.Infof("server: AcceptStream(peer=%s) error - closing peer session: %v", peer.peerID, err)
				s.removePeerSession(peer.peerID, "closed")
			}
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleStream(ctx, stream, sessionID)
		}()
	}
}

func (s *Server) streamContext() context.Context {
	if s.baseCtx != nil {
		return s.baseCtx
	}
	return context.Background()
}

func (s *Server) establishPeerSession(peer *peerSession) bool {
	if peer.sessionReady != nil {
		return s.waitPeerHandshake(peer)
	}
	session := peer.dataSession()
	if session == nil {
		return false
	}
	ctx := s.streamContext()
	stream, result, ok := s.acceptHandshake(ctx, session)
	if !ok {
		s.removePeerSession(peer.peerID, "handshake failed")
		return false
	}
	peer.setHandshake(result)
	s.startPeerControlLoop(ctx, peer, stream)
	return true
}

func (s *Server) waitPeerHandshake(peer *peerSession) bool {
	if peer.sessionReady == nil {
		return false
	}
	select {
	case <-peer.sessionReady:
		return peer.sid() != ""
	case <-s.done:
		s.removePeerSession(peer.peerID, "closed")
		return false
	}
}

func (s *Server) removePeerSession(peerID, reason string) {
	s.sessMu.Lock()
	peer := s.peerSessions[peerID]
	delete(s.peerSessions, peerID)
	s.sessMu.Unlock()
	if peer != nil {
		s.closePeerSession(peer, reason)
	}
}

func (s *Server) closePeerSession(peer *peerSession, reason string) {
	teardown := peer.snapshot()
	tunnelcore.NotifyControlClose(teardown.controlStrm)
	if teardown.controlStop != nil {
		teardown.controlStop()
	}
	if teardown.controlStrm != nil {
		_ = teardown.controlStrm.Close()
	}
	if teardown.controlSess != nil {
		_ = teardown.controlSess.Close()
	}
	if teardown.controlConn != nil {
		_ = teardown.controlConn.Close()
	}
	if teardown.session != nil {
		_ = teardown.session.Close()
	}
	if teardown.conn != nil {
		_ = teardown.conn.Close()
	}
	if teardown.sessionID != "" {
		s.onClose(teardown.sessionID, reason)
		s.trackPeerClose(teardown.sessionID, reason)
	}
}

func (s *Server) trackPeerOpen(sessionID, deviceID string) {
	s.peersMu.Lock()
	s.peerStats[sessionID] = peerStat{deviceID: deviceID, openedAt: time.Now()}
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("peer connected: device=%s session=%s", deviceID, sessionID)
	logger.Infof("%s", line)
}

func (s *Server) trackPeerClose(sessionID, reason string) {
	s.peersMu.Lock()
	stat, ok := s.peerStats[sessionID]
	if !ok {
		s.peersMu.Unlock()
		return
	}
	delete(s.peerStats, sessionID)
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("peer disconnected: device=%s session=%s reason=%s duration=%s",
		stat.deviceID, sessionID, reason, time.Since(stat.openedAt).Round(time.Second))
	logger.Infof("%s", line)
}

func (s *Server) peersLineLocked() string {
	devices := make([]string, 0, len(s.peerStats))
	for _, stat := range s.peerStats {
		devices = append(devices, stat.deviceID)
	}
	sort.Strings(devices)
	return fmt.Sprintf("Current peers count: %d, Devices: [%s]", len(s.peerStats), strings.Join(devices, ", "))
}

func (s *Server) logPeersLine() {
	s.peersMu.Lock()
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("%s", line)
}

func (s *Server) stopping() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}
