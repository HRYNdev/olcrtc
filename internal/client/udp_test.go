package client

import (
	"net"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/udpwire"
)

const clientTestDNSCloudflare = "1.1.1.1"

func TestRemoveIdleUDPFlowsForConn(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer func() { _ = conn.Close() }()
	otherConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen other udp: %v", err)
	}
	defer func() { _ = otherConn.Close() }()

	now := time.Now()
	c := &Client{
		udpFlows: map[uint64]clientUDPFlow{
			1: {
				conn:       conn,
				clientAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10001},
				target:     udpwire.Endpoint{Host: clientTestDNSCloudflare, Port: 53},
				lastSeen:   now.Add(-udpFlowIdleTimeout - time.Second),
			},
			2: {
				conn:       conn,
				clientAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10002},
				target:     udpwire.Endpoint{Host: "8.8.8.8", Port: 53},
				lastSeen:   now,
			},
			3: {
				conn:       otherConn,
				clientAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10003},
				target:     udpwire.Endpoint{Host: "9.9.9.9", Port: 53},
				lastSeen:   now.Add(-udpFlowIdleTimeout - time.Second),
			},
		},
	}

	c.removeIdleUDPFlowsForConn(conn, now)

	if _, ok := c.udpFlows[1]; ok {
		t.Fatal("idle flow for conn was not removed")
	}
	if _, ok := c.udpFlows[2]; !ok {
		t.Fatal("active flow for conn was removed")
	}
	if _, ok := c.udpFlows[3]; !ok {
		t.Fatal("idle flow for another conn was removed")
	}
}

func TestUDPFlowIDReusesExistingWhenAtLimit(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer func() { _ = conn.Close() }()

	src := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10001}
	target := udpwire.Endpoint{Host: clientTestDNSCloudflare, Port: 53}
	c := &Client{
		maxUDPFlows: 1,
		udpFlows: map[uint64]clientUDPFlow{
			7: {
				conn:       conn,
				clientAddr: src,
				target:     target,
				lastSeen:   time.Now().Add(-time.Second),
			},
		},
	}

	id, ok := c.udpFlowID(conn, src, target)
	if !ok {
		t.Fatal("udpFlowID rejected existing flow at limit")
	}
	if id != 7 {
		t.Fatalf("udpFlowID = %d, want 7", id)
	}
}

func TestUDPFlowIDRejectsNewFlowAtLimit(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := &Client{
		maxUDPFlows: 1,
		udpFlows: map[uint64]clientUDPFlow{
			7: {
				conn:       conn,
				clientAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10001},
				target:     udpwire.Endpoint{Host: clientTestDNSCloudflare, Port: 53},
				lastSeen:   time.Now(),
			},
		},
	}

	_, ok := c.udpFlowID(
		conn,
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10002},
		udpwire.Endpoint{Host: "8.8.8.8", Port: 53},
	)
	if ok {
		t.Fatal("udpFlowID accepted new flow at limit")
	}
}
