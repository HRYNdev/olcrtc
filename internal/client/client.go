// Package client implements the local SOCKS5 client side of the olcrtc tunnel.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

var (
	ErrConnectFailed           = errors.New("tunnel connection failed")
	ErrProxyAuth               = errors.New("SOCKS proxy auth failed")
	ErrKeySize                 = runtime.ErrKeySize
	ErrInvalidSOCKSVersion     = errors.New("invalid socks version")
	ErrUnsupportedSOCKSCommand = errors.New("unsupported socks command")
	ErrUnsupportedAddressType  = errors.New("unsupported address type")
	ErrRemoteNotReady          = errors.New("remote not ready")
	ErrSOCKSAuthFailed         = errors.New("SOCKS5 authentication failed")
	ErrSOCKSCredTooLong        = errors.New("socks5 user/pass exceeds 255 bytes")
)

const (
	reconnectCarrier  = "carrier"
	reconnectLiveness = "liveness"
	reconnectFallback = "liveness-fallback"
)

const (
	defaultLivenessFallback = 30 * time.Second
	defaultShutdownGrace    = 5 * time.Second
)

// Client handles local SOCKS5 connections and tunnels them to the server.
type Client struct {
	ln          transport.Transport
	keys        *crypto.KeySet
	pair        *tunnelcore.SessionPair
	conn        *muxconn.Conn
	controlConn *muxconn.Conn
	session     *smux.Session
	controlSess *smux.Session
	controlStrm *smux.Stream
	controlStop context.CancelFunc
	sessMu      sync.RWMutex
	reconnectMu sync.Mutex
	health      *runtime.HealthTracker

	// controlLastPong is independent corroboration for the transport's fast
	// peer-restart heuristic, not a second session reconnect detector.
	controlLastPong  atomic.Value // time.Time
	deviceID         string
	sessionID        string
	claims           map[string]any
	dnsServer        string
	socksUser        string
	socksPass        string
	sessionReady     chan struct{}
	wg               sync.WaitGroup
	livenessFallback time.Duration
	shutdownGrace    time.Duration
	fallbackPending  atomic.Bool
}

// HealthFunc is called when the client control health snapshot changes.
type HealthFunc func(control.Status)

// Config holds runtime configuration for [Run] and [RunWithReady].
type Config struct {
	Transport        string
	Carrier          string
	RoomURL          string
	ChannelID        string
	KeyHex           string
	LocalAddr        string
	DNSServer        string
	Resolver         *net.Resolver
	SOCKSUser        string
	SOCKSPass        string
	TransportOptions transport.Options
	Engine           string
	URL              string
	Token            string
	AuthToken        string
	Liveness         control.Config
	Traffic          transport.TrafficConfig
	DeviceID         string
	DeviceIDPath     string
	Claims           map[string]any
	OnHealth         HealthFunc
}

// Run starts the client with the given configuration.
func Run(ctx context.Context, cfg Config) error {
	return RunWithReady(ctx, cfg, nil)
}

// RunWithReady starts the client and invokes onReady after the SOCKS listener opens.
func RunWithReady(ctx context.Context, cfg Config, onReady func()) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	keys, err := tunnelcore.SetupKeySet(cfg.KeyHex, crypto.Client)
	if err != nil {
		return fmt.Errorf("setup key set: %w", err)
	}
	deviceID, err := resolveDeviceID(cfg.DeviceID, cfg.DeviceIDPath)
	if err != nil {
		return fmt.Errorf("resolve device id: %w", err)
	}
	client := &Client{
		keys: keys, deviceID: deviceID, claims: cfg.Claims, dnsServer: cfg.DNSServer,
		socksUser: cfg.SOCKSUser, socksPass: cfg.SOCKSPass,
		health: runtime.NewHealthTracker(cfg.OnHealth), sessionReady: make(chan struct{}),
	}
	defer func() {
		cancel()
		client.shutdown()
	}()
	if bringUpErr := client.bringUpLink(runCtx, cfg, cancel); bringUpErr != nil {
		return bringUpErr
	}
	listener, err := (&net.ListenConfig{}).Listen(runCtx, "tcp4", cfg.LocalAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", cfg.LocalAddr, err)
	}
	defer func() { _ = listener.Close() }()
	logger.Infof("SOCKS5 server listening on %s", cfg.LocalAddr)
	if onReady != nil {
		onReady()
	}
	client.goTracked(func() { client.acceptLoop(runCtx, listener) })
	<-runCtx.Done()
	return nil
}

func (c *Client) goTracked(fn func()) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		fn()
	}()
}

func resolveDeviceID(deviceID, path string) (string, error) {
	if deviceID != "" {
		return deviceID, nil
	}
	if path == "" {
		return uuid.NewString(), nil
	}
	data, err := os.ReadFile(path) // #nosec G304 - path is explicit user configuration
	if err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read device id %s: %w", path, err)
	}
	id := uuid.NewString()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("mkdir device id dir: %w", err)
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write device id %s: %w", path, err)
	}
	return id, nil
}
