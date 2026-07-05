package server

import (
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/udpwire"
)

const (
	testUDPPeerID        = "peer-1"
	testUDPSessionID     = "session-1"
	testUDPDNSGoogle     = "8.8.8.8"
	testUDPDNSCloudflare = "1.1.1.1"
)

func TestCloseUDPFlowReportsTrafficOnce(t *testing.T) {
	left, right := net.Pipe()
	defer func() { _ = right.Close() }()

	key := serverUDPKey{peerID: testUDPPeerID, flowID: 42}
	flow := &serverUDPFlow{
		key:       key,
		conn:      left,
		endpoint:  udpwire.Endpoint{Host: testUDPDNSGoogle, Port: 53},
		sessionID: testUDPSessionID,
	}
	flow.bytesIn.Store(11)
	flow.bytesOut.Store(17)

	var calls int
	s := &Server{
		udpFlows: map[serverUDPKey]*serverUDPFlow{key: flow},
		onTraffic: func(sessionID, addr string, bytesIn, bytesOut uint64) {
			calls++
			if sessionID != testUDPSessionID {
				t.Fatalf("sessionID = %q, want %s", sessionID, testUDPSessionID)
			}
			if addr != "8.8.8.8:53" {
				t.Fatalf("addr = %q, want %s:53", addr, testUDPDNSGoogle)
			}
			if bytesIn != 11 || bytesOut != 17 {
				t.Fatalf("traffic = %d/%d, want 11/17", bytesIn, bytesOut)
			}
		},
	}

	s.closeUDPFlow(key)
	s.closeUDPFlow(key)
	s.finishUDPFlow(flow)

	if calls != 1 {
		t.Fatalf("onTraffic calls = %d, want 1", calls)
	}
	if _, ok := s.udpFlows[key]; ok {
		t.Fatal("flow still present after close")
	}
}

func TestGetOrCreateUDPFlowRejectsNewFlowAtLimit(t *testing.T) {
	key := serverUDPKey{peerID: testUDPPeerID, flowID: 1}
	s := &Server{
		maxUDPFlows: 1,
		udpFlows: map[serverUDPKey]*serverUDPFlow{
			key: {
				key:       key,
				conn:      noopConn{},
				endpoint:  udpwire.Endpoint{Host: testUDPDNSGoogle, Port: 53},
				sessionID: testUDPSessionID,
			},
		},
	}

	_, err := s.getOrCreateUDPFlow(
		serverUDPKey{peerID: testUDPPeerID, flowID: 2},
		udpwire.Endpoint{Host: testUDPDNSCloudflare, Port: 53},
		testUDPSessionID,
	)
	if !errors.Is(err, errTooManyUDPFlows) {
		t.Fatalf("getOrCreateUDPFlow() error = %v, want %v", err, errTooManyUDPFlows)
	}
}

func TestGetOrCreateUDPFlowRejectsPendingAtLimit(t *testing.T) {
	s := &Server{
		maxUDPFlows:     1,
		udpFlows:        map[serverUDPKey]*serverUDPFlow{},
		udpPendingFlows: 1,
	}

	_, err := s.getOrCreateUDPFlow(
		serverUDPKey{peerID: testUDPPeerID, flowID: 2},
		udpwire.Endpoint{Host: testUDPDNSCloudflare, Port: 53},
		testUDPSessionID,
	)
	if !errors.Is(err, errTooManyUDPFlows) {
		t.Fatalf("getOrCreateUDPFlow() error = %v, want %v", err, errTooManyUDPFlows)
	}
}

func TestGetOrCreateUDPFlowRejectsUpstreamProxy(t *testing.T) {
	s := &Server{
		socksProxyAddr: "127.0.0.1",
		udpFlows:       map[serverUDPKey]*serverUDPFlow{},
	}

	_, err := s.getOrCreateUDPFlow(
		serverUDPKey{peerID: testUDPPeerID, flowID: 2},
		udpwire.Endpoint{Host: testUDPDNSCloudflare, Port: 53},
		testUDPSessionID,
	)
	if !errors.Is(err, errUDPProxyRequired) {
		t.Fatalf("getOrCreateUDPFlow() error = %v, want %v", err, errUDPProxyRequired)
	}
}

func TestGetOrCreateUDPFlowReusesExistingWhenAtLimit(t *testing.T) {
	key := serverUDPKey{peerID: testUDPPeerID, flowID: 1}
	flow := &serverUDPFlow{
		key:       key,
		conn:      noopConn{},
		endpoint:  udpwire.Endpoint{Host: testUDPDNSGoogle, Port: 53},
		sessionID: testUDPSessionID,
	}
	s := &Server{
		maxUDPFlows: 1,
		udpFlows:    map[serverUDPKey]*serverUDPFlow{key: flow},
	}

	got, err := s.getOrCreateUDPFlow(key, flow.endpoint, testUDPSessionID)
	if err != nil {
		t.Fatalf("getOrCreateUDPFlow() error = %v", err)
	}
	if got != flow {
		t.Fatal("getOrCreateUDPFlow() did not reuse existing flow")
	}
}

func TestValidateResolvedUDPAddrBlocksSpecialTargets(t *testing.T) {
	s := &Server{}
	tests := []string{
		"127.0.0.1",
		"10.0.0.1",
		"169.254.1.1",
		"224.0.0.1",
		"255.255.255.255",
		"::1",
		"fc00::1",
		"::ffff:127.0.0.1",
		"::ffff:10.0.0.1",
	}
	for _, tc := range tests {
		t.Run(tc, func(t *testing.T) {
			addr, err := netip.ParseAddr(tc)
			if err != nil {
				t.Fatalf("parse addr: %v", err)
			}
			if _, err := s.validateResolvedUDPAddr(addr); !errors.Is(err, errBlockedUDPTarget) {
				t.Fatalf("validateResolvedUDPAddr(%s) error = %v, want %v", tc, err, errBlockedUDPTarget)
			}
		})
	}
}

func TestValidateResolvedUDPAddrSelectsNetwork(t *testing.T) {
	s := &Server{}
	tests := []struct {
		addr        string
		wantNetwork string
	}{
		{addr: testUDPDNSGoogle, wantNetwork: "udp4"},
		{addr: "2001:4860:4860::8888", wantNetwork: "udp6"},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			addr, err := netip.ParseAddr(tt.addr)
			if err != nil {
				t.Fatalf("parse addr: %v", err)
			}
			got, err := s.validateResolvedUDPAddr(addr)
			if err != nil {
				t.Fatalf("validateResolvedUDPAddr() error = %v", err)
			}
			if got.network != tt.wantNetwork {
				t.Fatalf("network = %q, want %q", got.network, tt.wantNetwork)
			}
		})
	}
}

type noopConn struct{}

func (noopConn) Read(_ []byte) (int, error)         { return 0, net.ErrClosed }
func (noopConn) Write(p []byte) (int, error)        { return len(p), nil }
func (noopConn) Close() error                       { return nil }
func (noopConn) LocalAddr() net.Addr                { return nil }
func (noopConn) RemoteAddr() net.Addr               { return nil }
func (noopConn) SetDeadline(_ time.Time) error      { return nil }
func (noopConn) SetReadDeadline(_ time.Time) error  { return nil }
func (noopConn) SetWriteDeadline(_ time.Time) error { return nil }
