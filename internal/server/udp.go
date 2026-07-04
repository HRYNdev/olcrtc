package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/udpwire"
)

const (
	udpRelayBufferSize = 64 * 1024
	udpFlowIdleTimeout = 2 * time.Minute
)

var (
	errBlockedUDPTarget = errors.New("blocked udp target")
	errNoUDPRecords     = errors.New("resolve udp target: no A records")
)

type serverUDPKey struct {
	peerID string
	flowID uint64
}

type serverUDPFlow struct {
	key       serverUDPKey
	conn      net.Conn
	endpoint  udpwire.Endpoint
	sessionID string
	bytesIn   atomic.Uint64
	bytesOut  atomic.Uint64
	closeOnce sync.Once
}

func (s *Server) onDatagram(ciphertext []byte) {
	s.handleDatagram("", ciphertext)
}

func (s *Server) onPeerDatagram(peerID string, ciphertext []byte) {
	s.handleDatagram(peerID, ciphertext)
}

func (s *Server) handleDatagram(peerID string, ciphertext []byte) {
	wire, err := s.cipher.Decrypt(ciphertext)
	if err != nil {
		logger.Debugf("drop udp datagram decrypt failed: %v", err)
		return
	}
	frame, err := udpwire.Decode(wire)
	if err != nil {
		logger.Debugf("drop udp datagram decode failed: %v", err)
		return
	}

	key := serverUDPKey{peerID: peerID, flowID: frame.FlowID}
	if frame.Type == udpwire.FrameTypeClose {
		s.closeUDPFlow(key)
		return
	}
	sessionID := s.udpSessionID(peerID)
	if sessionID == "" {
		logger.Debugf("drop udp datagram without authenticated session")
		return
	}
	flow, err := s.getOrCreateUDPFlow(key, frame.Endpoint, sessionID)
	if err != nil {
		logger.Debugf("drop udp flow create failed target=%s:%d err=%v",
			frame.Endpoint.Host, frame.Endpoint.Port, err)
		return
	}
	n, err := flow.conn.Write(frame.Payload)
	if n > 0 {
		flow.bytesIn.Add(uint64(n))
		flow.touch(time.Now())
	}
	if err != nil {
		logger.Debugf("udp relay write failed target=%s:%d err=%v",
			frame.Endpoint.Host, frame.Endpoint.Port, err)
		s.closeUDPFlow(key)
	}
}

func (s *Server) getOrCreateUDPFlow(
	key serverUDPKey,
	endpoint udpwire.Endpoint,
	sessionID string,
) (*serverUDPFlow, error) {
	s.udpMu.Lock()
	if flow := s.udpFlows[key]; flow != nil {
		s.udpMu.Unlock()
		return flow, nil
	}
	s.udpMu.Unlock()

	dialHost, err := s.resolveUDPTarget(endpoint)
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(dialHost, strconv.Itoa(int(endpoint.Port)))
	dialer := &net.Dialer{
		Timeout:  10 * time.Second,
		Resolver: s.resolver,
	}
	conn, err := dialer.DialContext(s.baseCtx, "udp4", addr)
	if err != nil {
		return nil, fmt.Errorf("udp dial failed: %w", err)
	}

	flow := &serverUDPFlow{
		key:       key,
		conn:      conn,
		endpoint:  endpoint,
		sessionID: sessionID,
	}
	flow.touch(time.Now())
	s.udpMu.Lock()
	if existing := s.udpFlows[key]; existing != nil {
		s.udpMu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	s.udpFlows[key] = flow
	s.udpMu.Unlock()

	go s.readUDPFlow(flow)
	return flow, nil
}

func (s *Server) readUDPFlow(flow *serverUDPFlow) {
	buf := make([]byte, udpRelayBufferSize)
	for {
		n, err := flow.conn.Read(buf)
		if err != nil {
			var netErr net.Error
			switch {
			case errors.Is(err, net.ErrClosed):
			case errors.As(err, &netErr) && netErr.Timeout():
				logger.Debugf("udp relay idle timeout target=%s:%d",
					flow.endpoint.Host, flow.endpoint.Port)
			default:
				logger.Debugf("udp relay read ended target=%s:%d err=%v",
					flow.endpoint.Host, flow.endpoint.Port, err)
			}
			s.closeUDPFlow(flow.key)
			return
		}
		if n <= 0 {
			continue
		}
		flow.bytesOut.Add(uint64(n))
		flow.touch(time.Now())
		frame := udpwire.Frame{
			Type:     udpwire.FrameTypePacket,
			FlowID:   flow.key.flowID,
			Endpoint: flow.endpoint,
			Payload:  append([]byte(nil), buf[:n]...),
		}
		s.sendUDPFrame(flow.key.peerID, frame)
	}
}

func (flow *serverUDPFlow) touch(now time.Time) {
	_ = flow.conn.SetReadDeadline(now.Add(udpFlowIdleTimeout))
}

func (s *Server) sendUDPFrame(peerID string, frame udpwire.Frame) {
	wire, err := udpwire.Encode(frame)
	if err != nil {
		logger.Debugf("udp relay encode response failed: %v", err)
		return
	}
	enc, err := s.cipher.Encrypt(wire)
	if err != nil {
		logger.Debugf("udp relay encrypt response failed: %v", err)
		return
	}
	if peerID != "" {
		if pdg, ok := s.ln.(transport.PeerDatagramTransport); ok {
			if err := pdg.SendDatagramTo(peerID, enc); err != nil {
				logger.Debugf("udp relay peer send failed: %v", err)
			}
			return
		}
	}
	dg, ok := s.ln.(transport.DatagramTransport)
	if !ok {
		return
	}
	if err := dg.SendDatagram(enc); err != nil {
		logger.Debugf("udp relay send failed: %v", err)
	}
}

func (s *Server) closeUDPFlow(key serverUDPKey) {
	s.udpMu.Lock()
	flow := s.udpFlows[key]
	delete(s.udpFlows, key)
	s.udpMu.Unlock()
	if flow != nil {
		s.finishUDPFlow(flow)
	}
}

func (s *Server) closeAllUDPFlows() {
	s.udpMu.Lock()
	flows := s.udpFlows
	s.udpFlows = make(map[serverUDPKey]*serverUDPFlow)
	s.udpMu.Unlock()
	for _, flow := range flows {
		if flow != nil {
			s.finishUDPFlow(flow)
		}
	}
}

func (s *Server) finishUDPFlow(flow *serverUDPFlow) {
	flow.closeOnce.Do(func() {
		_ = flow.conn.Close()
		bytesIn := flow.bytesIn.Load()
		bytesOut := flow.bytesOut.Load()
		if flow.sessionID == "" || (bytesIn == 0 && bytesOut == 0) || s.onTraffic == nil {
			return
		}
		addr := net.JoinHostPort(flow.endpoint.Host, strconv.Itoa(int(flow.endpoint.Port)))
		s.onTraffic(flow.sessionID, addr, bytesIn, bytesOut)
	})
}

func (s *Server) udpSessionID(peerID string) string {
	if peerID == "" {
		return s.currentSessionID()
	}
	s.sessMu.RLock()
	defer s.sessMu.RUnlock()
	ps := s.peerSessions[peerID]
	if ps == nil {
		return ""
	}
	return ps.sessionID
}

func (s *Server) resolveUDPTarget(endpoint udpwire.Endpoint) (string, error) {
	if endpoint.Port == 0 || endpoint.Host == "" {
		return "", udpwire.ErrInvalidEndpoint
	}
	if addr, err := netip.ParseAddr(endpoint.Host); err == nil {
		return s.validateResolvedUDPAddr(addr)
	}
	addrs, err := s.lookupUDPTarget(endpoint.Host)
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if _, err := s.validateResolvedUDPAddr(addr); err != nil {
			return "", err
		}
	}
	return addrs[0].String(), nil
}

func (s *Server) lookupUDPTarget(host string) ([]netip.Addr, error) {
	baseCtx := s.baseCtx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 5*time.Second)
	defer cancel()
	addrs, err := s.resolver.LookupNetIP(ctx, "ip4", host)
	if err != nil {
		return nil, fmt.Errorf("resolve udp target: %w", err)
	}
	if len(addrs) == 0 {
		return nil, errNoUDPRecords
	}
	return addrs, nil
}

func (s *Server) validateResolvedUDPAddr(addr netip.Addr) (string, error) {
	if !s.unsafeAllowPrivateUDPTargets && blockedUDPAddr(addr) {
		return "", errBlockedUDPTarget
	}
	return addr.String(), nil
}

func blockedUDPAddr(addr netip.Addr) bool {
	return addr.IsUnspecified() ||
		addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsMulticast()
}
