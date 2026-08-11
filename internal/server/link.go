package server

import (
	"context"
	"fmt"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

func (s *Server) bringUpLink(ctx context.Context, cfg Config, cancel context.CancelFunc) error {
	s.baseCtx = ctx
	linkCfg := tunnelcore.BuildTransportConfig(tunnelcore.LinkConfig{
		Provider: cfg.Provider, RoomURL: cfg.RoomURL, Engine: cfg.Engine,
		URL: cfg.URL, Token: cfg.Token, ProviderToken: cfg.ProviderToken,
		ChannelID: cfg.ChannelID, DNSServer: s.dnsServer,
		Options: cfg.TransportOptions, Traffic: cfg.Traffic,
	}, tunnelcore.LinkRoleConfig{
		OnData: s.onData, OnPeerData: s.onPeerData, Resolver: s.resolver,
		ProxyAddr: s.socksProxyAddr, ProxyPort: s.socksProxyPort,
	})
	ln, err := transport.New(ctx, cfg.Transport, linkCfg)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	s.ln = ln
	if peerLn, ok := ln.(transport.PeerTransport); ok && peerLn.SupportsPeerRouting() {
		s.peerLn = peerLn
	}
	ln.SetEndedCallback(func(reason string) {
		logger.Infof("Server link reported conference end: %s", reason)
		cancel()
	})
	ln.SetShouldReconnect(func() bool { return ctx.Err() == nil })
	ln.SetReconnectCallback(func() {
		if ctx.Err() == nil {
			s.handleReconnect()
		}
	})
	logger.Infof("Connecting transport=%s provider=%s ...", cfg.Transport, cfg.Provider)
	if s.peerLn == nil {
		s.installSession()
	} else {
		s.installControlSession()
	}
	if err := ln.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect link: %w", err)
	}
	logger.Infof("Link connected")
	s.logPeersLine()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ln.WatchConnection(ctx)
	}()
	return nil
}

func (s *Server) installSession() {
	pair, err := tunnelcore.NewSessionPair(s.ln, s.keys, tunnelcore.ServerRole)
	if pair == nil {
		logger.Warnf("smux server init failed: %v", err)
		return
	}
	if err != nil {
		logger.Warnf("control smux server init failed: %v", err)
	}
	s.sessMu.Lock()
	s.installPairLocked(pair)
	s.sessMu.Unlock()
	s.state.broadcast()
	if pair.HasIsolatedControl() {
		go s.acceptSingletonHandshake(s.baseCtx, pair.ControlSession)
	}
}

func (s *Server) installControlSession() {
	if peerControl, ok := s.ln.(transport.PeerControlPlane); ok {
		s.installPeerControlPlane(peerControl)
		return
	}
	conn, session, err := tunnelcore.NewControlSession(s.ln, s.keys, tunnelcore.ServerRole)
	if err != nil {
		logger.Warnf("control smux server init failed (peer-routing): %v", err)
		return
	}
	if conn == nil {
		return
	}
	s.sessMu.Lock()
	s.controlConn = conn
	s.controlSess = session
	s.sessMu.Unlock()
	go s.acceptSingletonHandshake(s.baseCtx, session)
}

func (s *Server) handleReconnect() {
	s.health.RecordReconnect()
	logger.Infof("server reconnect reason=provider - tearing down smux session")
	s.sessMu.RLock()
	current := s.session
	s.sessMu.RUnlock()
	s.reinstallSession(current)
}

func (s *Server) reinstallSession(dead *smux.Session) {
	s.reinstallMu.Lock()
	defer s.reinstallMu.Unlock()
	s.sessMu.RLock()
	if s.pair != nil {
		_ = s.pair.CloseConns()
	} else {
		if s.conn != nil {
			_ = s.conn.Close()
		}
		if s.controlConn != nil {
			_ = s.controlConn.Close()
		}
	}
	s.sessMu.RUnlock()
	replacement, err := tunnelcore.NewSessionPair(s.ln, s.keys, tunnelcore.ServerRole)
	if replacement == nil {
		logger.Warnf("smux server re-init failed: %v", err)
		return
	}
	if err != nil {
		logger.Warnf("control smux server re-init failed: %v", err)
	}
	if !s.swapSession(dead, replacement) {
		return
	}
	if replacement.HasIsolatedControl() {
		go s.acceptSingletonHandshake(s.baseCtx, replacement.ControlSession)
	}
}

func (s *Server) staleReinstall(dead *smux.Session) bool {
	return dead != nil && dead != s.session && dead != s.controlSess
}

func (s *Server) swapSession(dead *smux.Session, replacement *tunnelcore.SessionPair) bool {
	s.sessMu.Lock()
	if s.staleReinstall(dead) {
		s.sessMu.Unlock()
		_ = replacement.Close()
		return false
	}
	oldPair := s.pair
	oldSession := s.session
	oldControlSession := s.controlSess
	oldControl := s.controlStrm
	oldControlStop := s.controlStop
	oldSessionID := s.sessionID
	s.installPairLocked(replacement)
	s.controlStrm = nil
	s.controlStop = nil
	s.sessionID = ""
	s.deviceID = ""
	s.sessMu.Unlock()
	s.state.broadcast()
	if oldControlStop != nil {
		oldControlStop()
	}
	closeServerPair(oldPair, oldSession, oldControlSession)
	if oldControl != nil {
		_ = oldControl.Close()
	}
	if oldSessionID != "" {
		s.onClose(oldSessionID, "reconnect")
		s.trackPeerClose(oldSessionID, "reconnect")
	}
	return true
}

func closeServerPair(pair *tunnelcore.SessionPair, session, controlSession *smux.Session) {
	if pair != nil {
		_ = pair.Close()
		return
	}
	if session != nil {
		_ = session.Close()
	}
	if controlSession != nil && controlSession != session {
		_ = controlSession.Close()
	}
}

func (s *Server) installPairLocked(pair *tunnelcore.SessionPair) {
	s.pair = pair
	s.conn = pair.DataConn
	s.session = pair.DataSession
	s.controlConn = pair.ControlConn
	if pair.HasIsolatedControl() {
		s.controlSess = pair.ControlSession
		return
	}
	s.controlSess = nil
}

func (s *Server) closeSession() {
	s.sessMu.Lock()
	pair := s.pair
	session := s.session
	controlSession := s.controlSess
	conn := s.conn
	controlConn := s.controlConn
	control := s.controlStrm
	controlStop := s.controlStop
	peers := s.peerSessions
	oldSessionID := s.sessionID
	s.peerSessions = make(map[string]*peerSession)
	s.pair, s.session, s.controlSess = nil, nil, nil
	s.conn, s.controlConn = nil, nil
	s.controlStrm, s.controlStop = nil, nil
	s.sessionID, s.deviceID = "", ""
	s.sessMu.Unlock()
	s.state.broadcast()
	if controlStop != nil {
		controlStop()
	}
	tunnelcore.NotifyControlClose(control)
	if pair != nil {
		_ = pair.Close()
	} else {
		closeServerPair(nil, session, controlSession)
		if conn != nil {
			_ = conn.Close()
		}
		if controlConn != nil {
			_ = controlConn.Close()
		}
	}
	if oldSessionID != "" {
		s.onClose(oldSessionID, "closed")
		s.trackPeerClose(oldSessionID, "closed")
	}
	for _, peer := range peers {
		s.closePeerSession(peer, "closed")
	}
}

func (s *Server) onData(data []byte) {
	s.sessMu.RLock()
	conn := s.conn
	s.sessMu.RUnlock()
	tunnelcore.PushData(conn, data)
}

func (s *Server) shutdown() {
	if s.done != nil {
		s.doneOnce.Do(func() { close(s.done) })
	}
	s.closeSession()
	if s.ln != nil {
		_ = s.ln.Close()
	}
}
