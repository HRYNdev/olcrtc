package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

const (
	socksVersion            = 5
	socksAddrIPv4           = 1
	socksAddrDomain         = 3
	socksAddrIPv6           = 4
	socksRepSuccess         = 0
	socksRepHostUnreachable = 4
)

func (c *Client) acceptLoop(ctx context.Context, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Warnf("Accept error: %v", err)
			continue
		}
		go c.handleSocks5(ctx, conn)
	}
}

func (c *Client) handleSocks5(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	if err := c.socks5Handshake(conn); err != nil {
		return
	}
	targetAddr, targetPort, err := c.socks5Request(conn)
	if err != nil {
		return
	}
	const sessionReadyTimeout = 60 * time.Second
	readyCtx, cancel := context.WithTimeout(ctx, sessionReadyTimeout)
	defer cancel()
	for {
		c.sessMu.RLock()
		session := c.session
		sessionID := c.sessionID
		c.sessMu.RUnlock()
		if session != nil && !session.IsClosed() && sessionID != "" {
			c.tunnel(ctx, conn, session, targetAddr, targetPort)
			return
		}
		select {
		case <-readyCtx.Done():
			_, _ = conn.Write(replyHostUnreachable(targetAddr))
			return
		case <-c.readyChannel():
		}
	}
}

func (c *Client) socks5Handshake(conn net.Conn) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read socks5 header: %w", err)
	}
	if header[0] != socksVersion {
		return fmt.Errorf("%w: %d", ErrInvalidSOCKSVersion, header[0])
	}
	methods := make([]byte, header[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return fmt.Errorf("read socks5 methods: %w", err)
	}
	if c.socksUser != "" {
		if _, err := conn.Write([]byte{socksVersion, 2}); err != nil {
			return fmt.Errorf("write socks5 auth method: %w", err)
		}
		return c.socks5UserPassAuth(conn)
	}
	if _, err := conn.Write([]byte{socksVersion, 0}); err != nil {
		return fmt.Errorf("write socks5 auth: %w", err)
	}
	return nil
}

func (c *Client) socks5UserPassAuth(conn net.Conn) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read socks5 auth header: %w", err)
	}
	if header[0] != 1 {
		return fmt.Errorf("%w: expected auth version 1, got %d", ErrInvalidSOCKSVersion, header[0])
	}
	user := make([]byte, header[1])
	if _, err := io.ReadFull(conn, user); err != nil {
		return fmt.Errorf("read socks5 username: %w", err)
	}
	passwordLength := make([]byte, 1)
	if _, err := io.ReadFull(conn, passwordLength); err != nil {
		return fmt.Errorf("read socks5 plen: %w", err)
	}
	password := make([]byte, passwordLength[0])
	if _, err := io.ReadFull(conn, password); err != nil {
		return fmt.Errorf("read socks5 password: %w", err)
	}
	if string(user) != c.socksUser || string(password) != c.socksPass {
		_, _ = conn.Write([]byte{1, 1})
		return ErrSOCKSAuthFailed
	}
	if _, err := conn.Write([]byte{1, 0}); err != nil {
		return fmt.Errorf("write socks5 auth success: %w", err)
	}
	return nil
}

func (c *Client) socks5Request(conn net.Conn) (string, int, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", 0, fmt.Errorf("read socks5 request: %w", err)
	}
	if header[1] != 1 {
		return "", 0, fmt.Errorf("%w: %d", ErrUnsupportedSOCKSCommand, header[1])
	}
	addr, err := c.readSocks5Addr(conn, header[3])
	if err != nil {
		return "", 0, err
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return "", 0, fmt.Errorf("read socks5 port: %w", err)
	}
	return addr, int(binary.BigEndian.Uint16(portBytes)), nil
}

func (c *Client) readSocks5Addr(conn net.Conn, addrType byte) (string, error) {
	switch addrType {
	case socksAddrIPv4:
		return readIP(conn, net.IPv4len, "ipv4")
	case socksAddrIPv6:
		return readIP(conn, net.IPv6len, "ipv6")
	case socksAddrDomain:
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return "", fmt.Errorf("read socks5 domain len: %w", err)
		}
		buffer := make([]byte, length[0])
		if _, err := io.ReadFull(conn, buffer); err != nil {
			return "", fmt.Errorf("read socks5 domain: %w", err)
		}
		return string(buffer), nil
	default:
		return "", fmt.Errorf("%w: %d", ErrUnsupportedAddressType, addrType)
	}
}

func readIP(conn net.Conn, length int, label string) (string, error) {
	buffer := make([]byte, length)
	if _, err := io.ReadFull(conn, buffer); err != nil {
		return "", fmt.Errorf("read socks5 %s: %w", label, err)
	}
	return net.IP(buffer).String(), nil
}

func socks5Reply(rep byte, target string) []byte {
	addrLen := net.IPv4len
	addrType := byte(socksAddrIPv4)
	if ip := net.ParseIP(target); ip != nil && ip.To4() == nil {
		addrLen = net.IPv6len
		addrType = socksAddrIPv6
	}
	reply := make([]byte, 4+addrLen+2)
	reply[0], reply[1], reply[3] = socksVersion, rep, addrType
	return reply
}

func replySuccess(target string) []byte {
	return socks5Reply(socksRepSuccess, target)
}

func replyHostUnreachable(target string) []byte {
	return socks5Reply(socksRepHostUnreachable, target)
}
