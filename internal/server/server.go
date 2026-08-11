// Package server implements the olcrtc tunnel server logic.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/framing"
	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/names"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

const connectCommand = "connect"

// Tunnel CONNECT reply bytes. dispatch writes exactly one of these on the
// tunnel stream before any payload flows: ConnectAckOK once the target
// connection is established, a non-zero code when it could not be. The codes
// are SOCKS5 REP values (RFC 1928 section 6) so the client can hand them
// straight back to the local application. This block is the single definition
// of the protocol - internal/client reads these constants rather than
// duplicating the literals, so the two ends cannot drift apart.
const (
	// ConnectAckOK signals the target connection is up and the stream now
	// carries payload.
	ConnectAckOK byte = 0x00
	// ConnectAckHostUnreachable signals the server failed to reach the
	// requested target (SOCKS5 REP 0x04).
	ConnectAckHostUnreachable byte = 0x04
)

var (
	// ErrKeyRequired re-exports runtime.ErrKeyRequired for compatibility with
	// pre-runtime callers that errors.Is-checked it.
	ErrKeyRequired = runtime.ErrKeyRequired
	// ErrKeySize re-exports runtime.ErrKeySize for the same reason.
	ErrKeySize = runtime.ErrKeySize
	// ErrSocks5AuthFailed is returned when SOCKS5 authentication fails.
	ErrSocks5AuthFailed = errors.New("SOCKS5 auth failed")
	// ErrSocks5ConnectFailed is returned when SOCKS5 connection fails.
	ErrSocks5ConnectFailed = errors.New("SOCKS5 connect failed")
)

// SessionOpenFunc is called after a successful handshake, before the server
// accepts tunnel streams on that session.
type SessionOpenFunc func(sessionID, deviceID string, claims map[string]any)

// SessionCloseFunc is called when a session is torn down. Possible reasons:
// "reconnect" (carrier dropped and was reestablished), "closed" (graceful
// shutdown or ctx cancel).
type SessionCloseFunc func(sessionID, reason string)

// TrafficFunc is called once per tunnel stream, after the copy loops finish.
// bytesIn counts client→target bytes; bytesOut counts target→client bytes.
type TrafficFunc func(sessionID, addr string, bytesIn, bytesOut uint64)

// HealthFunc is called when the server control health snapshot changes.
type HealthFunc func(control.Status)

// Server handles incoming tunnel connections and proxies their traffic.
type Server struct {
	// baseCtx is the long-lived server context established in bringUpLink. It
	// is propagated to reconnect-time goroutines (acceptHandshake, control
	// loops) instead of context.Background() so they observe shutdown.
	baseCtx context.Context //nolint:containedctx // server-lifetime ctx for reconnect goroutines
	// ln and peerLn are assigned once by bringUpLink, before the transport is
	// connected and before any goroutine can observe the Server, and are never
	// reassigned afterwards. They are therefore read without sessMu everywhere;
	// taking the mutex in some readers and not others (as resetLinkPeer used
	// to) only suggests a synchronisation that does not exist.
	ln     transport.Transport
	peerLn transport.PeerTransport
	cipher *crypto.Cipher
	conn   *muxconn.Conn
	// controlConn is wired to the transport's isolated control-plane channel
	// (transport.ControlPlane). When non-nil, the smux control session runs
	// over it so bulk data writes never block control ping/pong.
	controlConn *muxconn.Conn
	session     *smux.Session
	// controlSess is the smux session that carries the handshake/control
	// stream. For control-plane transports the handshake + liveness loop run
	// on this session, which is distinct from the data session (and in
	// peer-routing mode the data session is nil). reinstallSession compares
	// the dying session against this so a control-session reinstall is not
	// silently discarded by the s.session guard (issue #95).
	controlSess    *smux.Session
	controlStrm    *smux.Stream
	controlStop    context.CancelFunc
	sessMu         sync.RWMutex
	peerSessions   map[string]*peerSession
	peersMu        sync.Mutex
	peerStats      map[string]peerStat
	reinstallMu    sync.Mutex
	wg             sync.WaitGroup
	authHook       handshake.AuthFunc
	onOpen         SessionOpenFunc
	onClose        SessionCloseFunc
	onTraffic      TrafficFunc
	deviceID       string
	sessionID      string
	dnsServer      string
	resolver       *net.Resolver
	socksProxyAddr string
	socksProxyPort int
	socksProxyUser string
	socksProxyPass string
	liveness       control.Config
	health         *runtime.HealthTracker
	// state wakes serveSingle whenever the singleton session or its handshake
	// changes, so the accept loop reacts to installs instead of polling.
	state    stateGate
	done     chan struct{}
	doneOnce sync.Once
}

// stateGate is a broadcast channel: every waiter parked on wait() is released
// by the next broadcast(). Waiters must take the channel before sampling the
// state they are waiting on, otherwise a transition in between is missed. The
// zero value is ready to use and a broadcast without waiters is a no-op.
type stateGate struct {
	mu sync.Mutex
	ch chan struct{}
}

func (g *stateGate) wait() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
	return g.ch
}

func (g *stateGate) broadcast() {
	g.mu.Lock()
	ch := g.ch
	g.ch = nil
	g.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// waitState blocks until the session state changes or ctx is cancelled. It
// reports false when the caller must stop.
func waitState(ctx context.Context, changed <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-changed:
		return true
	}
}

// peerStat holds the per-session info needed to report the live peer count
// and a disconnect summary.
type peerStat struct {
	deviceID string
	openedAt time.Time
}

// peerSession is one client's state in peer-routing mode. Its mutable fields
// are touched from four directions at once: the transport callback attaches the
// data conn, the handshake goroutine fills in the identifiers, the control loop
// installs the control stream and its cancel func, and the accept/teardown
// paths read all of them. They are therefore guarded by the session's own mu
// rather than the server-wide sessMu, which would serialise unrelated peers.
// Lock order is sessMu -> mu; mu is never held while taking sessMu.
type peerSession struct {
	// peerID and sessionReady are set at construction and never reassigned,
	// so they are read without mu.
	peerID string
	// sessionReady is closed once sessionID is populated from
	// acceptPeerHandshake. It is nil on the legacy path, where
	// establishPeerSession drives the handshake inline instead.
	sessionReady chan struct{}

	mu          sync.Mutex
	conn        *muxconn.Conn
	session     *smux.Session
	controlConn *muxconn.Conn
	controlSess *smux.Session
	controlStrm *smux.Stream
	controlStop context.CancelFunc
	sessionID   string
	deviceID    string
}

// sid returns the peer's session ID, empty until the handshake completes.
func (ps *peerSession) sid() string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.sessionID
}

// setHandshake records the handshake outcome for this peer.
func (ps *peerSession) setHandshake(res handshakeResult) {
	ps.mu.Lock()
	ps.sessionID = res.sessionID
	ps.deviceID = res.deviceID
	ps.mu.Unlock()
}

// attachData wires the peer's data muxconn and smux session.
func (ps *peerSession) attachData(conn *muxconn.Conn, sess *smux.Session) {
	ps.mu.Lock()
	ps.conn = conn
	ps.session = sess
	ps.mu.Unlock()
}

// dataConn returns the peer's data muxconn, nil before the first data frame.
func (ps *peerSession) dataConn() *muxconn.Conn {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.conn
}

// dataSession returns the peer's data smux session, nil before the first data
// frame.
func (ps *peerSession) dataSession() *smux.Session {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.session
}

// controlPlane returns the peer's control muxconn and smux session.
func (ps *peerSession) controlPlane() (*muxconn.Conn, *smux.Session) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.controlConn, ps.controlSess
}

// setControl installs the peer's control stream and the cancel func of its
// liveness loop.
func (ps *peerSession) setControl(stream *smux.Stream, stop context.CancelFunc) {
	ps.mu.Lock()
	ps.controlStrm = stream
	ps.controlStop = stop
	ps.mu.Unlock()
}

// teardown is a consistent snapshot of everything closePeerSession must close.
type teardown struct {
	conn        *muxconn.Conn
	session     *smux.Session
	controlConn *muxconn.Conn
	controlSess *smux.Session
	controlStrm *smux.Stream
	controlStop context.CancelFunc
	sessionID   string
}

// snapshot copies the closable state under mu. The fields are left in place so
// a concurrent handshake or control loop still sees a valid session; the
// underlying Close calls are all idempotent.
func (ps *peerSession) snapshot() teardown {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return teardown{
		conn:        ps.conn,
		session:     ps.session,
		controlConn: ps.controlConn,
		controlSess: ps.controlSess,
		controlStrm: ps.controlStrm,
		controlStop: ps.controlStop,
		sessionID:   ps.sessionID,
	}
}

// ConnectRequest is a message from the client to establish a new connection.
type ConnectRequest struct {
	Cmd  string `json:"cmd"`
	Addr string `json:"addr"`
	Port int    `json:"port"`
}

// Config holds runtime configuration for [Run].
type Config struct {
	Transport        string
	Carrier          string
	RoomURL          string
	ChannelID        string
	KeyHex           string
	DNSServer        string
	Resolver         *net.Resolver
	SOCKSProxyAddr   string
	SOCKSProxyPort   int
	SOCKSProxyUser   string
	SOCKSProxyPass   string
	TransportOptions transport.Options
	Engine           string
	URL              string
	Token            string
	AuthToken        string
	Liveness         control.Config
	Traffic          transport.TrafficConfig

	// AuthHook is invoked after CLIENT_HELLO to authorize the client and
	// return a session ID. If nil, every client is admitted with a random UUID.
	AuthHook handshake.AuthFunc

	// OnSessionOpen fires after a successful handshake. Nil means no-op.
	OnSessionOpen SessionOpenFunc
	// OnSessionClose fires when the session is torn down (reconnect, closed). Nil means no-op.
	OnSessionClose SessionCloseFunc
	// OnTraffic fires once per tunnel stream after both copy loops finish. Nil means no-op.
	OnTraffic TrafficFunc
	// OnHealth fires when liveness/reconnect status changes. Nil means no-op.
	OnHealth HealthFunc
}

// Run starts the server with the given configuration.
func Run(ctx context.Context, cfg Config) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cipher, err := setupCipher(cfg.KeyHex)
	if err != nil {
		return fmt.Errorf("setupCipher failed: %w", err)
	}

	hook := cfg.AuthHook
	if hook == nil {
		hook = defaultAuthHook
	}
	onOpen := cfg.OnSessionOpen
	if onOpen == nil {
		onOpen = func(string, string, map[string]any) {}
	}
	onClose := cfg.OnSessionClose
	if onClose == nil {
		onClose = func(string, string) {}
	}
	onTraffic := cfg.OnTraffic
	if onTraffic == nil {
		onTraffic = func(string, string, uint64, uint64) {}
	}
	s := &Server{
		cipher:         cipher,
		authHook:       hook,
		onOpen:         onOpen,
		onClose:        onClose,
		onTraffic:      onTraffic,
		dnsServer:      cfg.DNSServer,
		resolver:       cfg.Resolver,
		socksProxyAddr: cfg.SOCKSProxyAddr,
		socksProxyPort: cfg.SOCKSProxyPort,
		socksProxyUser: cfg.SOCKSProxyUser,
		socksProxyPass: cfg.SOCKSProxyPass,
		liveness:       cfg.Liveness,
		health:         runtime.NewHealthTracker(cfg.OnHealth),
		peerSessions:   make(map[string]*peerSession),
		peerStats:      make(map[string]peerStat),
		done:           make(chan struct{}),
	}
	s.setupResolver()

	// Register shutdown BEFORE bringUpLink so a partial setup (e.g.
	// link.New succeeded but ln.Connect timed out) still tears the
	// link down and sends MUC presence-unavailable. Without this, an
	// early bringUpLink error returns straight to the caller and the
	// already-joined MUC presence stays behind as a ghost participant
	// for subsequent tests against the same room. shutdown is
	// idempotent and safe to call before s.serve runs.
	defer func() {
		s.shutdown()
		s.wg.Wait()
	}()

	if err := s.bringUpLink(runCtx, cfg, cancel); err != nil {
		return err
	}

	go func() {
		<-runCtx.Done()
		s.closeSession()
	}()

	s.serve(runCtx)

	return nil
}

func setupCipher(keyHex string) (*crypto.Cipher, error) {
	cipher, err := runtime.SetupCipher(keyHex)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	return cipher, nil
}

func (s *Server) setupResolver() {
	if s.resolver == nil {
		s.resolver = protect.NewResolver(s.dnsServer)
	}
}

// dataSmuxConfig returns the data-plane smux config for the server's
// transport. It mirrors the client (buildSmuxClient -> runtime.SmuxConfigFor):
// ControlPlane transports (vp8channel) get the relaxed keep-alive window so a
// legitimately silent carrier (publisher-PC reconnect / SFU renegotiation) is
// not torn down at the conservative 30s timeout. Keeping this in lockstep with
// the client avoids the asymmetric teardown where the server drops its peer
// data session first, surfacing as "closed pipe" on the client and a reconnect
// storm (issue #95).
func dataSmuxConfig(tr transport.Transport) *smux.Config {
	return runtime.SmuxConfigFor(tr)
}

func controlSmuxConfig(maxWirePayload int) *smux.Config {
	return runtime.ControlSmuxConfig(maxWirePayload)
}

func linkMaxPayload(tr transport.Transport) int {
	return runtime.MaxPayload(tr)
}

func (s *Server) bringUpLink(
	ctx context.Context,
	cfg Config,
	cancel context.CancelFunc,
) error {
	s.baseCtx = ctx
	ln, err := transport.New(ctx, cfg.Transport, transport.Config{
		Carrier:    cfg.Carrier,
		RoomURL:    cfg.RoomURL,
		Engine:     cfg.Engine,
		URL:        cfg.URL,
		Token:      cfg.Token,
		AuthToken:  cfg.AuthToken,
		ChannelID:  cfg.ChannelID,
		DeviceID:   "",
		Name:       names.Generate(),
		OnData:     s.onData,
		OnPeerData: s.onPeerData,
		DNSServer:  s.dnsServer,
		Resolver:   s.resolver,
		ProxyAddr:  s.socksProxyAddr,
		ProxyPort:  s.socksProxyPort,
		Options:    cfg.TransportOptions,
		Traffic:    cfg.Traffic,
	})
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
		if ctx.Err() != nil {
			return
		}
		s.handleReconnect()
	})

	logger.Infof("Connecting transport=%s carrier=%s ...", cfg.Transport, cfg.Carrier)
	if s.peerLn == nil {
		s.installSession()
	} else {
		// Peer-routing mode: installSession is skipped, but we still need to
		// wire up the control-plane smux session so that liveness ping/pong
		// works correctly over the isolated control track. Build the full
		// control conn + smux session and launch acceptHandshake exactly as
		// installSession does for the non-peer-routing path.
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
	conn := muxconn.New(s.ln, s.cipher)
	sess, err := smux.Server(conn, dataSmuxConfig(s.ln))
	if err != nil {
		logger.Warnf("smux server init failed: %v", err)
		return
	}
	// If the transport has an isolated control plane, build a dedicated
	// control smux session over it and launch the handshake acceptor.
	// For transports without a control plane, serveSingle drives the
	// handshake in its own loop.
	controlConn := muxconn.NewControl(s.ln, s.cipher)
	var ctrlSess *smux.Session
	if controlConn != nil {
		controlSess, cerr := smux.Server(controlConn, controlSmuxConfig(linkMaxPayload(s.ln)))
		if cerr != nil {
			logger.Warnf("control smux server init failed: %v", cerr)
			_ = controlConn.Close()
			controlConn = nil
		} else {
			// Isolated control plane: handshake runs on the control session.
			ctrlSess = controlSess
			go s.acceptSingletonHandshake(s.baseCtx, controlSess)
		}
	}
	s.sessMu.Lock()
	s.conn = conn
	s.controlConn = controlConn
	s.controlSess = ctrlSess
	s.session = sess
	s.sessMu.Unlock()
	s.state.broadcast()
}

// installControlSession wires up the per-peer control-plane routing for
// transports that implement transport.PeerControlPlane. In peer-routing mode
// each client gets its own control KCP (keyed by data epoch in the transport);
// this function registers the callback that fires when a new peer's control
// data arrives, creating a per-peer smux+handshake+liveness session on demand.
// For transports with only transport.ControlPlane (no per-peer routing), it
// falls back to the singleton control session so legacy non-peer-routing code
// still works.
func (s *Server) installControlSession() {
	// Prefer per-peer control plane (PeerControlPlane) when available.
	if pcp, ok := s.ln.(transport.PeerControlPlane); ok {
		s.installPeerControlPlane(pcp)
		return
	}
	// Fallback: singleton control plane (ControlPlane only).
	controlConn := muxconn.NewControl(s.ln, s.cipher)
	if controlConn == nil {
		return
	}
	controlSess, err := smux.Server(controlConn, controlSmuxConfig(linkMaxPayload(s.ln)))
	if err != nil {
		logger.Warnf("control smux server init failed (peer-routing): %v", err)
		_ = controlConn.Close()
		return
	}
	s.sessMu.Lock()
	s.controlConn = controlConn
	s.controlSess = controlSess
	s.sessMu.Unlock()
	go s.acceptSingletonHandshake(s.baseCtx, controlSess)
}

// installPeerControlPlane registers the per-peer control callback on the
// transport. When the transport delivers a control frame for a new peer ID, we
// create a dedicated muxconn+smux session for that peer, run acceptHandshake
// on it, and then start the liveness control loop — exactly what the singleton
// path does, but one instance per client instead of shared.
func (s *Server) installPeerControlPlane(pcp transport.PeerControlPlane) {
	pcp.SetControlOnPeerData(func(peerID string, data []byte) {
		s.onPeerControlData(peerID, data)
	})
}

// onPeerControlData is the transport callback for per-peer control frames.
// It routes the frame to the peer's control muxconn, creating one on demand.
func (s *Server) onPeerControlData(peerID string, data []byte) {
	ps := s.getOrCreatePeerControlSession(peerID)
	if ps == nil {
		return
	}
	if conn, _ := ps.controlPlane(); conn != nil {
		conn.Push(data)
	}
}

// getOrCreatePeerControlSession returns an existing peerSession for peerID, or
// creates one with a fresh per-peer control muxconn+smux session and launches
// acceptHandshake on it. The data smux session is created later by
// getPeerSession when the first data frame arrives.
func (s *Server) getOrCreatePeerControlSession(peerID string) *peerSession {
	s.sessMu.Lock()
	ps := s.peerSessions[peerID]
	if ps != nil {
		s.sessMu.Unlock()
		return ps
	}

	if _, ok := s.ln.(transport.PeerControlPlane); !ok {
		s.sessMu.Unlock()
		return nil
	}

	controlConn := muxconn.NewPeerControlUnbound(s.ln, s.cipher, peerID)
	if controlConn == nil {
		s.sessMu.Unlock()
		return nil
	}
	controlSess, err := smux.Server(controlConn, controlSmuxConfig(linkMaxPayload(s.ln)))
	if err != nil {
		logger.Warnf("control smux init failed for peer %s: %v", peerID, err)
		_ = controlConn.Close()
		s.sessMu.Unlock()
		return nil
	}
	ps = &peerSession{
		peerID:       peerID,
		sessionReady: make(chan struct{}),
		controlConn:  controlConn,
		controlSess:  controlSess,
	}
	s.peerSessions[peerID] = ps
	s.sessMu.Unlock()

	logger.Infof("server: peer control session created peerID=%s", peerID)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptPeerHandshake(s.baseCtx, ps)
	}()
	return ps
}

func (s *Server) handleReconnect() {
	s.recordReconnect()
	logger.Infof("server reconnect reason=carrier - tearing down smux session")
	s.sessMu.RLock()
	current := s.session
	s.sessMu.RUnlock()
	s.reinstallSession(current)
}

func (s *Server) reinstallSession(dead *smux.Session) {
	s.reinstallMu.Lock()
	defer s.reinstallMu.Unlock()

	// Close the old muxconns immediately so that any in-flight Push calls
	// (from data arriving on a new bridge before this reinstall completes)
	// are discarded rather than feeding stale frames into the dying smux
	// session.
	s.sessMu.RLock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.controlConn != nil {
		_ = s.controlConn.Close()
	}
	s.sessMu.RUnlock()

	// Pre-build the replacement so we can swap atomically below.
	r := s.buildReplacementSession()
	if r == nil {
		return
	}

	if !s.swapSession(dead, r) {
		return
	}

	// Launch the handshake acceptor on the control session only when
	// an isolated control plane exists. Without one, serveSingle drives
	// the handshake in its own loop (same as before).
	if r.controlSess != nil {
		go s.acceptSingletonHandshake(s.baseCtx, r.controlSess)
	}
}

// replacementSession holds a freshly-built data + (optional) control smux
// session pair, prior to atomically swapping it into the live server.
type replacementSession struct {
	conn        *muxconn.Conn
	sess        *smux.Session
	controlConn *muxconn.Conn
	controlSess *smux.Session
}

// buildReplacementSession constructs a fresh data + (optional) control smux
// session over new muxconns. It returns nil when the data session could not be
// built.
func (s *Server) buildReplacementSession() *replacementSession {
	conn := muxconn.New(s.ln, s.cipher)
	sess, err := smux.Server(conn, dataSmuxConfig(s.ln))
	if err != nil {
		logger.Warnf("smux server init failed: %v", err)
		_ = conn.Close()
		return nil
	}

	r := &replacementSession{conn: conn, sess: sess}
	r.controlConn = muxconn.NewControl(s.ln, s.cipher)
	if r.controlConn != nil {
		r.controlSess, err = smux.Server(r.controlConn, controlSmuxConfig(linkMaxPayload(s.ln)))
		if err != nil {
			logger.Warnf("control smux server init failed: %v", err)
			_ = r.controlConn.Close()
			r.controlConn = nil
			r.controlSess = nil
		}
	}
	return r
}

// staleReinstall reports whether a pre-built replacement must be discarded
// because another reinstall already swapped in a fresh session - i.e. dead
// matches neither the live data session nor the live control session. A nil
// dead (carrier-triggered reconnect) always proceeds. The dying session may be
// the data session (legacy/datachannel path) or the control session
// (control-plane transports, where s.session is nil in peer-routing mode).
// Matching against controlSess is what keeps a control-session reinstall from
// being silently dropped; without it acceptHandshake was never re-armed and
// every later reconnect hung forever in waitPeerHandshake (issue #95).
func (s *Server) staleReinstall(dead *smux.Session) bool {
	return dead != nil && dead != s.session && dead != s.controlSess
}

// discardReplacement tears down a replacement session that lost the reinstall
// race.
func discardReplacement(r *replacementSession) {
	_ = r.sess.Close()
	_ = r.conn.Close()
	if r.controlConn != nil {
		_ = r.controlSess.Close()
		_ = r.controlConn.Close()
	}
}

// swapSession atomically replaces the live session with the pre-built one and
// tears down the old one. Returns false (discarding the new build) when another
// reinstall already won the race.
func (s *Server) swapSession(dead *smux.Session, r *replacementSession) bool {
	s.sessMu.Lock()
	if s.staleReinstall(dead) {
		s.sessMu.Unlock()
		discardReplacement(r)
		return false
	}
	oldSess := s.session
	oldCtrlSess := s.controlSess
	oldControl := s.controlStrm
	oldControlStop := s.controlStop
	oldSID := s.sessionID
	s.session = r.sess
	s.conn = r.conn
	s.controlConn = r.controlConn
	s.controlSess = r.controlSess
	s.controlStrm = nil
	s.controlStop = nil
	s.sessionID = ""
	s.deviceID = ""
	s.sessMu.Unlock()
	s.state.broadcast()

	if oldControlStop != nil {
		oldControlStop()
	}
	if oldSess != nil {
		_ = oldSess.Close()
	}
	if oldCtrlSess != nil && oldCtrlSess != oldSess {
		_ = oldCtrlSess.Close()
	}
	if oldControl != nil {
		_ = oldControl.Close()
	}
	if oldSID != "" {
		s.onClose(oldSID, "reconnect")
		s.trackPeerClose(oldSID, "reconnect")
	}
	return true
}

func (s *Server) closeSession() {
	s.sessMu.Lock()
	sess := s.session
	conn := s.conn
	ctrlConn := s.controlConn
	control := s.controlStrm
	controlStop := s.controlStop
	peers := s.peerSessions
	s.peerSessions = make(map[string]*peerSession)
	s.session = nil
	s.conn = nil
	s.controlConn = nil
	s.controlSess = nil
	s.controlStrm = nil
	s.controlStop = nil
	oldSID := s.sessionID
	s.sessionID = ""
	s.deviceID = ""
	s.sessMu.Unlock()
	s.state.broadcast()

	if controlStop != nil {
		controlStop()
	}
	notifyControlClose(control)
	if sess != nil {
		_ = sess.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
	if ctrlConn != nil {
		_ = ctrlConn.Close()
	}
	if oldSID != "" {
		s.onClose(oldSID, "closed")
		s.trackPeerClose(oldSID, "closed")
	}
	for _, ps := range peers {
		s.closePeerSession(ps, "closed")
	}
}

func (s *Server) removePeerSession(peerID, reason string) {
	s.sessMu.Lock()
	ps := s.peerSessions[peerID]
	delete(s.peerSessions, peerID)
	s.sessMu.Unlock()
	if ps != nil {
		s.closePeerSession(ps, reason)
	}
}

// closePeerSession tears one peer down in the same order as closeSession: tell
// the peer we are going away while its control stream is still usable, stop the
// liveness loop, then unwind stream -> session -> conn. Closing the control smux
// session before the stream (as this used to) swallowed the CONTROL_CLOSE we
// had just tried to send, so the client only learned of the teardown when its
// own liveness timer expired.
func (s *Server) closePeerSession(ps *peerSession, reason string) {
	td := ps.snapshot()

	notifyControlClose(td.controlStrm)
	if td.controlStop != nil {
		td.controlStop()
	}
	if td.controlStrm != nil {
		_ = td.controlStrm.Close()
	}
	if td.controlSess != nil {
		_ = td.controlSess.Close()
	}
	if td.controlConn != nil {
		_ = td.controlConn.Close()
	}
	if td.session != nil {
		_ = td.session.Close()
	}
	if td.conn != nil {
		_ = td.conn.Close()
	}
	if td.sessionID != "" {
		s.onClose(td.sessionID, reason)
		s.trackPeerClose(td.sessionID, reason)
	}
}

// trackPeerOpen records a newly opened session and logs the live peer summary.
func (s *Server) trackPeerOpen(sessionID, deviceID string) {
	s.peersMu.Lock()
	s.peerStats[sessionID] = peerStat{deviceID: deviceID, openedAt: time.Now()}
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("peer connected: device=%s session=%s", deviceID, sessionID)
	logger.Infof("%s", line)
}

// trackPeerClose drops a closed session and logs a disconnect summary plus the
// live peer summary.
func (s *Server) trackPeerClose(sessionID, reason string) {
	s.peersMu.Lock()
	st, ok := s.peerStats[sessionID]
	if !ok {
		s.peersMu.Unlock()
		return // session was never tracked (or already removed) - avoid double count
	}
	delete(s.peerStats, sessionID)
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("peer disconnected: device=%s session=%s reason=%s duration=%s",
		st.deviceID, sessionID, reason, time.Since(st.openedAt).Round(time.Second))
	logger.Infof("%s", line)
}

// peersLineLocked builds the "Current peers count: N, Devices: [...]" summary
// line from the live sessions. The caller must hold peersMu.
func (s *Server) peersLineLocked() string {
	devices := make([]string, 0, len(s.peerStats))
	for _, st := range s.peerStats {
		devices = append(devices, st.deviceID)
	}
	sort.Strings(devices)
	return fmt.Sprintf("Current peers count: %d, Devices: [%s]", len(s.peerStats), strings.Join(devices, ", "))
}

// logPeersLine logs the current peer summary line (count + device list).
func (s *Server) logPeersLine() {
	s.peersMu.Lock()
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("%s", line)
}

func notifyControlClose(stream *smux.Stream) {
	if stream == nil {
		return
	}
	_ = stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := control.SendClose(stream); err == nil {
		time.Sleep(200 * time.Millisecond)
	}
	_ = stream.SetWriteDeadline(time.Time{})
	_ = stream.CloseWrite()
}

func (s *Server) onData(data []byte) {
	s.sessMu.RLock()
	conn := s.conn
	s.sessMu.RUnlock()
	if conn != nil {
		conn.Push(data)
	}
}

func (s *Server) onPeerData(peerID string, data []byte) {
	ps := s.getPeerSession(peerID)
	if ps == nil {
		// Not in peer-routing mode: fall back to the single data conn.
		s.onData(data)
		return
	}
	if conn := ps.dataConn(); conn != nil {
		conn.Push(data)
	}
}

func (s *Server) getPeerSession(peerID string) *peerSession {
	if peerID == "" || s.peerLn == nil {
		return nil
	}
	// In peer-routing mode with PeerControlPlane, the peerSession may already
	// exist (created by getOrCreatePeerControlSession when the first control
	// frame arrived). If so, just attach the data conn to it.
	s.sessMu.Lock()
	ps := s.peerSessions[peerID]
	if ps != nil && ps.dataConn() != nil {
		// Data conn already wired; nothing to do.
		s.sessMu.Unlock()
		return ps
	}
	// Build the data smux session for this peer.
	conn := muxconn.NewPeer(s.peerLn, s.cipher, peerID)
	sess, err := smux.Server(conn, dataSmuxConfig(s.ln))
	if err != nil {
		s.sessMu.Unlock()
		logger.Warnf("smux server init failed for peer %s: %v", peerID, err)
		_ = conn.Close()
		return nil
	}
	if ps == nil {
		// No PeerControlPlane: create the peerSession with an empty
		// sessionID (legacy path). servePeer uses an empty sessionID as
		// the "needs handshake" flag; establishPeerSession fills in
		// sessionID/deviceID after acceptHandshake succeeds. Seeding them
		// from the singleton here would make a reconnecting peer reuse
		// stale state and skip its handshake.
		ps = &peerSession{peerID: peerID}
		s.peerSessions[peerID] = ps
	}
	ps.attachData(conn, sess)
	s.sessMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.servePeer(ps)
	}()
	return ps
}

// serve drives the smux Accept loop. The first accepted stream on a given
// smux session is the control stream - the handshake runs there. Subsequent
// streams are tunnel streams and proxy traffic.
func (s *Server) serve(ctx context.Context) {
	if s.peerLn != nil {
		<-ctx.Done()
		return
	}
	s.serveSingle(ctx)
}

func (s *Server) serveSingle(ctx context.Context) {
	for {
		if contextDone(ctx) {
			return
		}

		// Take the change channel before sampling the state, so a transition
		// racing with the read below still wakes us. Both waits used to be
		// timer polls (50ms for the session, 10ms for the handshake).
		changed := s.state.wait()
		s.sessMu.RLock()
		sess := s.session
		hasControlConn := s.controlConn != nil
		ready := s.sessionID != ""
		s.sessMu.RUnlock()

		// No session yet, or an isolated control plane whose handshake
		// goroutine owns acceptHandshake: park until the state moves.
		if sess == nil || (!ready && hasControlConn) {
			if !waitState(ctx, changed) {
				return
			}
			continue
		}
		// Legacy path: drive the handshake in this loop.
		if !ready && !s.acceptSingletonHandshake(ctx, sess) {
			continue
		}

		stream, err := sess.AcceptStream()
		if err != nil {
			if s.handleAcceptError(ctx, sess, err) {
				return
			}
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleStream(ctx, stream, s.currentSessionID())
		}()
	}
}

// handleAcceptError handles a failed AcceptStream. Returns true if the server should stop.
func (s *Server) handleAcceptError(ctx context.Context, sess *smux.Session, err error) bool {
	if contextDone(ctx) {
		return true
	}
	hadSession := s.handshakeReady()
	logger.Infof("server: AcceptStream(data) error - reinstalling session: %v", err)
	s.reinstallSession(sess)
	if hadSession && s.ln != nil {
		s.ln.Reconnect("liveness")
	}
	return false
}

func (s *Server) currentSessionID() string {
	s.sessMu.RLock()
	defer s.sessMu.RUnlock()
	return s.sessionID
}

func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// handshakeReady reports whether the current session has completed its
// handshake. The session is reset on reconnect, so this is recomputed.
func (s *Server) handshakeReady() bool {
	s.sessMu.RLock()
	defer s.sessMu.RUnlock()
	return s.sessionID != ""
}

// handshakeResult is the outcome of one completed server-side handshake. It is
// handed back to the caller instead of being written into the Server fields:
// the singleton path owns s.sessionID/s.deviceID, the per-peer path owns its
// peerSession. Routing every handshake through the Server fields made two
// clients overwrite each other's identity.
type handshakeResult struct {
	sessionID string
	deviceID  string
}

// acceptHandshake accepts the next stream on sess and runs the server-side
// handshake on it. On success it returns the control stream, which the caller
// hands to the matching liveness loop, and the handshake result, which the
// caller stores where it belongs.
func (s *Server) acceptHandshake(
	ctx context.Context,
	sess *smux.Session,
) (*smux.Stream, handshakeResult, bool) {
	// Retry loop: after a session reinstall, stale control frames from the
	// old client smux session may arrive on the new smux session with a
	// matching stream ID. These raw JSON bytes (e.g. CONTROL_PING) are
	// interpreted by the framing layer as an impossibly large length prefix,
	// triggering ErrFrameTooLarge. We close the polluted stream and accept
	// the next one (the real handshake).
	const maxStaleRetries = 3
	for retry := 0; retry <= maxStaleRetries; retry++ {
		stream, err := sess.AcceptStream()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil, handshakeResult{}, false
			default:
			}
			logger.Infof("server: AcceptStream(control) error - reinstalling session: %v", err)
			s.resetLinkPeer()
			s.reinstallSession(sess)
			return nil, handshakeResult{}, false
		}
		_ = stream.SetDeadline(time.Now().Add(handshake.DefaultTimeout))
		hello, sid, err := handshake.Server(stream, s.authHook)
		_ = stream.SetDeadline(time.Time{})
		if err != nil {
			_ = stream.Close()
			if errors.Is(err, framing.ErrFrameTooLarge) && retry < maxStaleRetries {
				logger.Debugf("handshake: discarding stale stream (attempt %d): %v", retry+1, err)
				continue
			}
			logger.Warnf("handshake failed: %v", err)
			s.resetLinkPeer()
			s.reinstallSession(sess)
			return nil, handshakeResult{}, false
		}
		s.recordSession(sid)
		s.onOpen(sid, hello.DeviceID, hello.Claims)
		s.trackPeerOpen(sid, hello.DeviceID)
		logger.Infof("session %s opened (device=%s)", sid, hello.DeviceID)
		return stream, handshakeResult{sessionID: sid, deviceID: hello.DeviceID}, true
	}
	return nil, handshakeResult{}, false
}

// acceptSingletonHandshake runs the handshake for the process-wide session,
// stores its result in the Server fields and starts the liveness loop.
func (s *Server) acceptSingletonHandshake(ctx context.Context, sess *smux.Session) bool {
	stream, res, ok := s.acceptHandshake(ctx, sess)
	if !ok {
		return false
	}
	s.sessMu.Lock()
	s.deviceID = res.deviceID
	s.sessionID = res.sessionID
	s.sessMu.Unlock()
	s.state.broadcast()
	s.startControlLoop(ctx, sess, stream)
	return true
}

// acceptPeerHandshake runs the handshake on the per-peer control smux session
// and then starts the liveness control loop. It mirrors acceptHandshake but
// writes sessionID/deviceID into the peerSession (not the shared server fields)
// so multiple clients can complete their handshakes independently.
func (s *Server) acceptPeerHandshake(ctx context.Context, ps *peerSession) {
	const maxStaleRetries = 3
	_, controlSess := ps.controlPlane()
	if controlSess == nil {
		return
	}
	for retry := 0; retry <= maxStaleRetries; retry++ {
		stream, err := controlSess.AcceptStream()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			logger.Infof("server: AcceptStream(peer control=%s) error: %v", ps.peerID, err)
			s.removePeerSession(ps.peerID, "handshake failed")
			return
		}
		_ = stream.SetDeadline(time.Now().Add(handshake.DefaultTimeout))
		hello, sid, err := handshake.Server(stream, s.authHook)
		_ = stream.SetDeadline(time.Time{})
		if err != nil {
			_ = stream.Close()
			if errors.Is(err, framing.ErrFrameTooLarge) && retry < maxStaleRetries {
				logger.Debugf("handshake peer=%s: stale stream retry %d: %v", ps.peerID, retry+1, err)
				continue
			}
			logger.Warnf("handshake peer=%s failed: %v", ps.peerID, err)
			s.removePeerSession(ps.peerID, "handshake failed")
			return
		}
		// Populate the peerSession and signal readiness so waitPeerHandshake unblocks.
		ps.setHandshake(handshakeResult{sessionID: sid, deviceID: hello.DeviceID})
		if ps.sessionReady != nil {
			close(ps.sessionReady)
		}
		s.recordSession(sid)
		s.onOpen(sid, hello.DeviceID, hello.Claims)
		s.trackPeerOpen(sid, hello.DeviceID)
		logger.Infof("peer session %s opened (peer=%s device=%s)", sid, ps.peerID, hello.DeviceID)
		s.startPeerControlLoop(ctx, ps, stream)
		return
	}
}

// startPeerControlLoop launches the liveness ping/pong loop for a per-peer
// control stream, mirroring startControlLoop for the singleton path.
func (s *Server) startPeerControlLoop(ctx context.Context, ps *peerSession, stream *smux.Stream) {
	controlCtx, stop := context.WithCancel(ctx)
	ps.setControl(stream, stop)

	liveness := s.liveness
	if runtime.IsControlPlane(s.ln) && liveness.Timeout <= control.DefaultTimeout {
		liveness.Timeout = runtime.LivenessTimeout(s.ln)
	}
	onPong := liveness.OnPong
	onMissedPong := liveness.OnMissedPong
	onUnhealthy := liveness.OnUnhealthy
	liveness.OnPong = func(h control.Health) {
		s.recordPong(h)
		logger.Debugf("control alive peer=%s rtt=%v seq=%d", ps.peerID, h.RTT, h.Seq)
		if onPong != nil {
			onPong(h)
		}
	}
	liveness.OnMissedPong = func(missed int) {
		s.recordMissed(missed)
		logger.Warnf("control missed pong peer=%s missed=%d", ps.peerID, missed)
		if onMissedPong != nil {
			onMissedPong(missed)
		}
	}
	liveness.OnUnhealthy = func(missed int) {
		s.recordUnhealthy(missed)
		logger.Warnf("control unhealthy peer=%s missed=%d", ps.peerID, missed)
		if onUnhealthy != nil {
			onUnhealthy(missed)
		}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { _ = stream.Close() }()
		err := control.Run(controlCtx, stream, liveness)
		if controlCtx.Err() != nil || ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Warnf("peer control stream ended peer=%s: %v", ps.peerID, err)
		}
		s.removePeerSession(ps.peerID, "liveness")
	}()
}

func (s *Server) servePeer(ps *peerSession) {
	if ps.sid() == "" && !s.establishPeerSession(ps) {
		return
	}
	sess := ps.dataSession()
	if sess == nil {
		return
	}
	// Tunnel streams run on the server-lifetime context, not
	// context.Background(): with the latter a shutdown never reached the
	// in-flight peer streams and they lingered until their peer hung up.
	ctx := s.streamContext()
	sid := ps.sid()
	for {
		if s.stopping() {
			return
		}
		stream, err := sess.AcceptStream()
		if err != nil {
			if s.stopping() {
				return
			}
			logger.Infof("server: AcceptStream(peer=%s) error - closing peer session: %v", ps.peerID, err)
			s.removePeerSession(ps.peerID, "closed")
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleStream(ctx, stream, sid)
		}()
	}
}

// streamContext returns the server-lifetime context used by tunnel-stream
// goroutines. Bare Server values in unit tests never run bringUpLink, so fall
// back to a background context there.
func (s *Server) streamContext() context.Context {
	if s.baseCtx != nil {
		return s.baseCtx
	}
	return context.Background()
}

// establishPeerSession ensures the per-peer handshake has completed. When a
// per-peer control smux session was set up by getOrCreatePeerControlSession,
// the sessionReady channel signals completion; otherwise the handshake runs
// inline on the data smux session (legacy / datachannel path).
func (s *Server) establishPeerSession(ps *peerSession) bool {
	// Per-peer control plane path: sessionReady is closed by acceptPeerHandshake.
	if ps.sessionReady != nil {
		return s.waitPeerHandshake(ps)
	}
	// No isolated control plane: drive the handshake inline on the peer's own
	// data session, and keep both the result and the liveness loop on the
	// peerSession. The old code ran it through the singleton Server fields
	// (sessionID, deviceID, controlStrm, controlStop) and copied them back, so
	// two clients clobbered each other's identity and control stream.
	sess := ps.dataSession()
	if sess == nil {
		return false
	}
	ctx := s.streamContext()
	stream, res, ok := s.acceptHandshake(ctx, sess)
	if !ok {
		s.removePeerSession(ps.peerID, "handshake failed")
		return false
	}
	ps.setHandshake(res)
	s.startPeerControlLoop(ctx, ps, stream)
	return true
}

// waitPeerHandshake blocks until acceptPeerHandshake closes ps.sessionReady
// (writing sessionID/deviceID into ps), or the server shuts down.
func (s *Server) waitPeerHandshake(ps *peerSession) bool {
	done := s.done
	ready := ps.sessionReady
	if ready == nil {
		return false
	}
	select {
	case <-ready:
		if ps.sid() == "" {
			// acceptPeerHandshake failed (removed the peer session before signalling).
			return false
		}
		return true
	case <-done:
		s.removePeerSession(ps.peerID, "closed")
		return false
	}
}

// resetLinkPeer asks the transport to drop its peer epoch. s.ln is fixed by
// bringUpLink before anything can observe the Server, so no lock is taken here
// - the same as every other s.ln reader.
func (s *Server) resetLinkPeer() {
	if resetter, ok := s.ln.(interface{ ResetPeer() }); ok {
		resetter.ResetPeer()
	}
}

func (s *Server) startControlLoop(ctx context.Context, sess *smux.Session, stream *smux.Stream) {
	controlCtx, stop := context.WithCancel(ctx)
	s.sessMu.Lock()
	s.controlStrm = stream
	s.controlStop = stop
	s.sessMu.Unlock()

	liveness := s.liveness
	// Relax the pong timeout only for transports with an isolated control
	// plane (vp8channel); conventional carriers keep the conservative default
	// so dead links are detected and reconnected promptly. A user-set timeout
	// larger than the default is left untouched.
	if runtime.IsControlPlane(s.ln) && liveness.Timeout <= control.DefaultTimeout {
		liveness.Timeout = runtime.LivenessTimeout(s.ln)
	}
	onPong := liveness.OnPong
	onMissedPong := liveness.OnMissedPong
	onUnhealthy := liveness.OnUnhealthy
	liveness.OnPong = func(h control.Health) {
		s.sessMu.RLock()
		sid := s.sessionID
		s.sessMu.RUnlock()
		s.recordPong(h)
		logger.Debugf("control alive session=%s rtt=%v seq=%d", sid, h.RTT, h.Seq)
		if onPong != nil {
			onPong(h)
		}
	}
	liveness.OnMissedPong = func(missed int) {
		s.recordMissed(missed)
		logger.Warnf("control missed pong on server: missed_pongs=%d", missed)
		if onMissedPong != nil {
			onMissedPong(missed)
		}
	}
	liveness.OnUnhealthy = func(missed int) {
		s.recordUnhealthy(missed)
		logger.Warnf("control stream unhealthy on server: missed_pongs=%d", missed)
		if onUnhealthy != nil {
			onUnhealthy(missed)
		}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { _ = stream.Close() }()
		err := control.Run(controlCtx, stream, liveness)
		if controlCtx.Err() != nil || ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Warnf("server control stream ended: %v", err)
		}
		s.recordReconnect()
		logger.Infof("server reconnect reason=liveness - reinstalling smux session")
		s.resetLinkPeer()
		s.reinstallSession(sess)
		// Tell the carrier to rebuild itself too. Without this the SFU side
		// keeps its dead PC around and the client's reconnect handshakes
		// keep landing in the void until the carrier eventually notices on
		// its own (which observationally takes ~40s on a Telemost room).
		if s.ln != nil {
			s.ln.Reconnect("liveness")
		}
	}()
}

func (s *Server) stopping() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Status returns the latest server-side control health snapshot.
func (s *Server) Status() control.Status {
	return s.health.Status()
}

func (s *Server) recordSession(sessionID string) { s.health.RecordSession(sessionID) }
func (s *Server) recordPong(h control.Health)    { s.health.RecordPong(h) }
func (s *Server) recordMissed(missed int)        { s.health.RecordMissed(missed) }
func (s *Server) recordUnhealthy(missed int)     { s.health.RecordUnhealthy(missed) }
func (s *Server) recordReconnect()               { s.health.RecordReconnect() }

func (s *Server) shutdown() {
	if s.done != nil {
		s.doneOnce.Do(func() { close(s.done) })
	}
	s.closeSession()
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

func (s *Server) handleStream(ctx context.Context, stream *smux.Stream, sessionID string) {
	defer func() { _ = stream.Close() }()
	if sessionID == "" {
		sessionID = s.currentSessionID()
	}
	// Unblock the reads and the copy loops below when the server shuts down.
	// A background context has a nil Done channel and needs no watcher.
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{})
		defer close(finished)
		go func() {
			select {
			case <-done:
				_ = stream.Close()
			case <-finished:
			}
		}()
	}

	// Read the connect JSON. The client writes the whole JSON in one
	// stream.Write so it usually arrives intact; tolerate fragmentation
	// by reading incrementally up to a sane cap.
	const maxConnReq = 4096
	header := make([]byte, 0, 256)
	tmp := make([]byte, 256)
	_ = stream.SetReadDeadline(time.Now().Add(15 * time.Second))
	for {
		n, err := stream.Read(tmp)
		if n > 0 {
			header = append(header, tmp[:n]...)
			if req, ok := parseConnectRequest(header); ok {
				_ = stream.SetReadDeadline(time.Time{})
				s.dispatch(stream, req, sessionID)
				return
			}
		}
		if err != nil {
			return
		}
		if len(header) > maxConnReq {
			return
		}
	}
}

func parseConnectRequest(buf []byte) (ConnectRequest, bool) {
	var req ConnectRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		return req, false
	}
	if req.Cmd != connectCommand {
		return req, false
	}
	return req, true
}

// defaultAuthHook admits every client and assigns a random session ID.
// Replace it via [Config.AuthHook] to plug in real authorization.
func defaultAuthHook(_ string, _ map[string]any) (string, error) {
	return uuid.NewString(), nil
}

func (s *Server) dispatch(stream *smux.Stream, req ConnectRequest, sessionID string) {
	addr := net.JoinHostPort(req.Addr, strconv.Itoa(req.Port))
	logger.Infof("sid=%d connect %s", stream.ID(), addr)

	dialStart := time.Now()
	conn, err := s.dial(req)
	dialElapsed := time.Since(dialStart)

	if err != nil {
		logger.Infof("sid=%d dial %s failed (%v): %v", stream.ID(), addr, dialElapsed, err)
		// Answer with a negative ack instead of dropping the stream silently:
		// a client with nothing to read blocks for the whole CONNECT ack
		// window (15s, 90s on control-plane transports) before giving up.
		_, _ = stream.Write([]byte{ConnectAckHostUnreachable})
		return
	}
	defer func() { _ = conn.Close() }()

	logger.Infof("sid=%d connected %s in %v", stream.ID(), addr, dialElapsed)

	if _, err := stream.Write([]byte{ConnectAckOK}); err != nil {
		return
	}

	var bytesOut uint64
	done := make(chan struct{})
	go func() {
		n, _ := io.Copy(stream, conn)
		if n > 0 {
			bytesOut = uint64(n)
		}
		_ = stream.Close()
		close(done)
	}()
	in, _ := io.Copy(conn, stream)
	_ = conn.Close()
	<-done
	bytesIn := uint64(0)
	if in > 0 {
		bytesIn = uint64(in)
	}
	if s.onTraffic != nil {
		s.onTraffic(sessionID, addr, bytesIn, bytesOut)
	}
}

func (s *Server) dial(req ConnectRequest) (net.Conn, error) {
	addr := net.JoinHostPort(req.Addr, strconv.Itoa(req.Port))
	if s.socksProxyAddr == "" {
		dialer := &net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			Resolver:  s.resolver,
		}
		conn, err := dialer.Dial("tcp4", addr)
		if err != nil {
			return nil, fmt.Errorf("dial failed: %w", err)
		}
		return conn, nil
	}

	proxyAddr := net.JoinHostPort(s.socksProxyAddr, strconv.Itoa(s.socksProxyPort))
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Resolver:  s.resolver,
	}
	conn, err := dialer.Dial("tcp4", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial proxy: %w", err)
	}

	if err := s.socks5Connect(conn, req.Addr, req.Port); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (s *Server) socks5Connect(conn net.Conn, targetAddr string, targetPort int) error {
	if err := s.socks5Authenticate(conn); err != nil {
		return err
	}

	addrLen := len(targetAddr)
	if addrLen > 255 {
		addrLen = 255
		targetAddr = targetAddr[:255]
	}

	req := make([]byte, 0, 7+addrLen)
	req = append(req, 5, 1, 0, 3, byte(addrLen))
	req = append(req, []byte(targetAddr)...)
	req = append(req, byte(targetPort>>8), byte(targetPort)) //nolint:gosec,lll // G115: bounded conversion verified by surrounding logic

	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("failed to write socks5 connect req: %w", err)
	}

	resp := make([]byte, 10)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("failed to read socks5 connect resp: %w", err)
	}
	if resp[0] != 5 || resp[1] != 0 {
		return fmt.Errorf("%w: %d", ErrSocks5ConnectFailed, resp[1])
	}

	return nil
}

func (s *Server) socks5Authenticate(conn net.Conn) error {
	if s.socksProxyUser != "" {
		// Offer username/password auth (RFC 1929) only.
		if _, err := conn.Write([]byte{5, 1, 2}); err != nil {
			return fmt.Errorf("failed to write socks5 auth: %w", err)
		}
	} else {
		// No authentication.
		if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
			return fmt.Errorf("failed to write socks5 auth: %w", err)
		}
	}

	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("failed to read socks5 auth resp: %w", err)
	}
	if resp[0] != 5 {
		return ErrSocks5AuthFailed
	}
	switch resp[1] {
	case 0: // no auth accepted
		if s.socksProxyUser != "" {
			return ErrSocks5AuthFailed
		}
	case 2: // username/password
		return s.socks5SendCredentials(conn)
	default:
		return ErrSocks5AuthFailed
	}
	return nil
}

func (s *Server) socks5SendCredentials(conn net.Conn) error {
	user := s.socksProxyUser
	pass := s.socksProxyPass
	if len(user) > 255 {
		user = user[:255]
	}
	if len(pass) > 255 {
		pass = pass[:255]
	}
	authMsg := make([]byte, 0, 3+len(user)+len(pass))
	authMsg = append(authMsg, 1, byte(len(user))) //nolint:gosec // G115: len clamped to ≤255 above
	authMsg = append(authMsg, []byte(user)...)
	authMsg = append(authMsg, byte(len(pass))) //nolint:gosec // G115: len clamped to ≤255 above
	authMsg = append(authMsg, []byte(pass)...)
	if _, err := conn.Write(authMsg); err != nil {
		return fmt.Errorf("failed to write socks5 credentials: %w", err)
	}
	authResp := make([]byte, 2)
	if _, err := io.ReadFull(conn, authResp); err != nil {
		return fmt.Errorf("failed to read socks5 credentials resp: %w", err)
	}
	if authResp[1] != 0 {
		return ErrSocks5AuthFailed
	}
	return nil
}
