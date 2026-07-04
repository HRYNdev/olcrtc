package server

import (
	"net"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/udpwire"
)

func TestCloseUDPFlowReportsTrafficOnce(t *testing.T) {
	left, right := net.Pipe()
	defer func() { _ = right.Close() }()

	key := serverUDPKey{peerID: "peer-1", flowID: 42}
	flow := &serverUDPFlow{
		key:       key,
		conn:      left,
		endpoint:  udpwire.Endpoint{Host: "8.8.8.8", Port: 53},
		sessionID: "session-1",
	}
	flow.bytesIn.Store(11)
	flow.bytesOut.Store(17)

	var calls int
	s := &Server{
		udpFlows: map[serverUDPKey]*serverUDPFlow{key: flow},
		onTraffic: func(sessionID, addr string, bytesIn, bytesOut uint64) {
			calls++
			if sessionID != "session-1" {
				t.Fatalf("sessionID = %q, want session-1", sessionID)
			}
			if addr != "8.8.8.8:53" {
				t.Fatalf("addr = %q, want 8.8.8.8:53", addr)
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
