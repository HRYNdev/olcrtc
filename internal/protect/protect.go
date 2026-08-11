// Package protect provides functions to protect sockets from VPN routing.
package protect

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultDialTimeout       = 10 * time.Second
	defaultKeepAlive         = 30 * time.Second
	defaultIdleConnTimeout   = 30 * time.Second
	defaultTLSHandshake      = 10 * time.Second
	defaultResponseHeader    = 10 * time.Second
	defaultWebSocketTimeout  = 10 * time.Second
	defaultHTTPClientTimeout = 30 * time.Second
	defaultStatusBodyLimit   = 1024
)

var (
	sensitiveFieldRE = regexp.MustCompile(
		`(?i)((?:access[_-]?token|room[_-]?token|token|credentials)"?\s*[:=]\s*"?)` +
			`[^",\s}]+`,
	)
	sensitiveBearerRE = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`)
)

// Protector is called with a socket file descriptor before connect.
// On Android, this calls VpnService.protect(fd) to bypass VPN routing.
var Protector func(fd int) bool //nolint:gochecknoglobals // package-level state intentional

var customResolver atomic.Pointer[net.Resolver] //nolint:gochecknoglobals // process integration setting

// SetResolver sets the resolver used by newly created protected dialers.
// ai-generated: added thread-safe custom resolver injection.
func SetResolver(resolver *net.Resolver) {
	customResolver.Store(resolver)
}

// Resolver returns the resolver used by newly created protected dialers.
// ai-generated: added access to the current custom resolver.
func Resolver() *net.Resolver {
	return customResolver.Load()
}

func controlFunc(network, _ string, c syscall.RawConn) error {
	if Protector == nil {
		return nil
	}
	var err error
	controlErr := c.Control(func(fd uintptr) {
		if !Protector(int(fd)) {
			err = &net.OpError{Op: "protect", Net: network, Err: net.ErrClosed}
		}
	})
	if controlErr != nil {
		return fmt.Errorf("control failed: %w", controlErr)
	}
	return err
}

// NewDialer returns a net.Dialer that calls Protector on each new socket.
func NewDialer() *net.Dialer {
	return NewDialerWithResolver(Resolver())
}

// NewDialerWithResolver returns a protected dialer using resolver for DNS.
// ai-generated: added resolver-aware protected dialing.
func NewDialerWithResolver(resolver *net.Resolver) *net.Dialer {
	return &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: defaultKeepAlive,
		Control:   controlFunc,
		Resolver:  resolver,
	}
}

// NewResolver returns a local Go resolver that sends queries to dnsServer.
// ai-generated: added local DNS server resolver construction.
func NewResolver(dnsServer string) *net.Resolver {
	if dnsServer == "" {
		return nil
	}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: 3 * time.Second, Control: controlFunc}
			return dialer.DialContext(ctx, network, dnsServer)
		},
	}
}

// NewTLSConfig returns the shared TLS policy for provider HTTP/WebSocket clients.
func NewTLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

// NewHTTPTransport returns an HTTP transport using protected sockets and sane timeouts.
func NewHTTPTransport(resolvers ...*net.Resolver) *http.Transport {
	dialer := NewDialerWithResolver(firstResolver(resolvers))
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSClientConfig:       NewTLSConfig(),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10,
		IdleConnTimeout:       defaultIdleConnTimeout,
		TLSHandshakeTimeout:   defaultTLSHandshake,
		ResponseHeaderTimeout: defaultResponseHeader,
	}
}

// NewHTTPClient returns an http.Client using protected sockets with DNS retry.
func NewHTTPClient(resolvers ...*net.Resolver) *http.Client {
	return &http.Client{
		Transport: &retryTransport{base: NewHTTPTransport(resolvers...)},
		Timeout:   defaultHTTPClientTimeout,
	}
}

// retryTransport retries requests on transient DNS/dial errors.
type retryTransport struct {
	base http.RoundTripper
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	const maxRetries = 3
	var resp *http.Response
	var err error
	for i := range maxRetries {
		if i > 0 {
			time.Sleep(time.Duration(i) * 500 * time.Millisecond)
		}
		resp, err = t.base.RoundTrip(req)
		if err == nil || !isRetriableError(err) {
			if err != nil {
				return resp, fmt.Errorf("round trip: %w", err)
			}
			return resp, nil
		}
	}
	return resp, fmt.Errorf("round trip after %d retries: %w", maxRetries, err)
}

func isRetriableError(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Timeout() || strings.Contains(opErr.Error(), "connection refused")
	}
	s := err.Error()
	return strings.Contains(s, "no such host") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "i/o timeout")
}

// NewWebSocketDialer returns a WebSocket dialer using protected sockets and shared TLS policy.
func NewWebSocketDialer(handshakeTimeout time.Duration, resolvers ...*net.Resolver) websocket.Dialer {
	if handshakeTimeout <= 0 {
		handshakeTimeout = defaultWebSocketTimeout
	}
	return websocket.Dialer{
		NetDialContext:   NewDialerWithResolver(firstResolver(resolvers)).DialContext,
		Proxy:            http.ProxyFromEnvironment,
		TLSClientConfig:  NewTLSConfig(),
		HandshakeTimeout: handshakeTimeout,
	}
}

// StatusError formats an upstream HTTP error while bounding and redacting the body.
func StatusError(base error, resp *http.Response, limit int64) error {
	if limit <= 0 {
		limit = defaultStatusBodyLimit
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, limit))
	bodyText := RedactSensitive(strings.TrimSpace(string(body)))
	if bodyText == "" {
		return fmt.Errorf("%w: status %d", base, resp.StatusCode)
	}
	return fmt.Errorf("%w: status %d: %s", base, resp.StatusCode, bodyText)
}

// RedactSensitive removes common token-like values from provider error text.
func RedactSensitive(text string) string {
	text = sensitiveBearerRE.ReplaceAllString(text, "${1}<redacted>")
	return sensitiveFieldRE.ReplaceAllString(text, "${1}<redacted>")
}

// DialContext dials using a protected socket.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := NewDialer().DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}
	return conn, nil
}

// ProxyDialer implements golang.org/x/net/proxy.Dialer for pion ICE.
type ProxyDialer struct {
	resolver *net.Resolver
}

// Dial connects to the address on the named network using a protected socket.
func (d *ProxyDialer) Dial(network, addr string) (net.Conn, error) {
	conn, err := NewDialerWithResolver(d.resolver).Dial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}
	return conn, nil
}

// NewProxyDialer returns a proxy.Dialer that protects ICE sockets.
// ai-generated: added optional resolver injection to the ICE proxy dialer.
func NewProxyDialer(resolvers ...*net.Resolver) *ProxyDialer {
	return &ProxyDialer{resolver: firstResolver(resolvers)}
}

// ai-generated: selects the optional resolver without changing legacy calls.
func firstResolver(resolvers []*net.Resolver) *net.Resolver {
	if len(resolvers) == 0 {
		return Resolver()
	}
	return resolvers[0]
}
