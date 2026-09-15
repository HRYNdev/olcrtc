// Package livekit implements an engine.Session backed by the LiveKit SFU
// protocol via the upstream livekit/server-sdk-go client.
//
// This engine is service-agnostic: it accepts a wss:// signaling URL and an
// access token, and provides byte-stream + video-track primitives over a
// LiveKit room. Service-specific token acquisition (e.g. WB Stream,
// or a self-hosted LiveKit deployment) lives in the auth package.
//
// Every LiveKit participant has a unique identity, and data packets carry the
// sender identity and may be addressed to a list of identities. The engine
// exposes this through engine.PeerSession (SendTo + OnPeerData) for the
// server and engine.RoomDirectorySession (announces, pinning, participant
// left) so that several clients can share one room with one server.
package livekit

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	protoLogger "github.com/livekit/protocol/logger"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
	lksdk "github.com/owenewans/owenlivekit/v2"
	"github.com/pion/webrtc/v4"
)

const (
	defaultSendQueueSize    = 5000
	defaultSendQueueCapHard = 4000
	dataPublishTopic        = "olcrtc"
	// announceTopic carries out-of-band room announces (server beacons). The
	// receiving engine hands them to the announce handler, never to OnData.
	announceTopic   = "olcrtc-beacon"
	videoTrackName  = "videochannel"
	reconnectWindow = 5 * time.Minute
	maxReconnects   = 10
)

var (
	// ErrSessionClosed is returned when an operation is attempted on a closed session.
	ErrSessionClosed = errors.New("livekit session closed")
	// ErrSendQueueFull is returned when the outbound queue cannot accept more data.
	ErrSendQueueFull = errors.New("livekit send queue full")
	// ErrRoomNotConnected is returned when the underlying room is not connected yet.
	ErrRoomNotConnected = errors.New("livekit room not connected")
	// ErrURLRequired is returned when no signaling URL was supplied.
	ErrURLRequired = errors.New("livekit signaling URL required")
	// ErrTokenRequired is returned when no access token was supplied.
	ErrTokenRequired = errors.New("livekit access token required")
)

type roomHandle interface {
	// publishData sends one reliable data packet on topic. Empty destinations
	// means every other participant in the room.
	publishData(data []byte, topic string, destinations []string) error
	publishTrack(track webrtc.TrackLocal) error
	unpublishLocalTracks()
	disconnect()
	connectionState() lksdk.ConnectionState
	localIdentity() string
}

// peerLeftSource is implemented by room handles that report departed
// participants out of band instead of through lksdk.RoomCallback (the
// in-memory hub, which cannot construct *lksdk.RemoteParticipant).
type peerLeftSource interface {
	setPeerLeftSink(fn func(identity string))
}

type sdkRoom struct {
	room *lksdk.Room
}

func (r *sdkRoom) publishData(data []byte, topic string, destinations []string) error {
	opts := []lksdk.DataPublishOption{
		lksdk.WithDataPublishTopic(topic),
		lksdk.WithDataPublishReliable(true),
	}
	if len(destinations) > 0 {
		opts = append(opts, lksdk.WithDataPublishDestination(destinations))
	}
	if err := r.room.LocalParticipant.PublishDataPacket(lksdk.UserData(data), opts...); err != nil {
		return fmt.Errorf("publish data packet: %w", err)
	}
	return nil
}

func (r *sdkRoom) publishTrack(track webrtc.TrackLocal) error {
	_, err := r.room.LocalParticipant.PublishTrack(track, &lksdk.TrackPublicationOptions{Name: videoTrackName})
	if err != nil {
		return fmt.Errorf("publish track: %w", err)
	}
	return nil
}

func (r *sdkRoom) unpublishLocalTracks() {
	if r.room == nil || r.room.LocalParticipant == nil {
		return
	}
	for _, publication := range r.room.LocalParticipant.TrackPublications() {
		if publication.SID() == "" {
			continue
		}
		if err := r.room.LocalParticipant.UnpublishTrack(publication.SID()); err != nil {
			log.Printf("livekit unpublish track error: %v", err)
		}
	}
}

func (r *sdkRoom) disconnect() {
	r.room.Disconnect()
	// LiveKit's Disconnect returns after local SDK teardown, before the
	// server necessarily evicts the participant. Give the signalling path a
	// short grace period so immediate reconnects do not inherit stale room
	// state from a ghost participant.
	time.Sleep(2 * time.Second)
}

func (r *sdkRoom) connectionState() lksdk.ConnectionState {
	return r.room.ConnectionState()
}

func (r *sdkRoom) localIdentity() string {
	if r.room == nil || r.room.LocalParticipant == nil {
		return ""
	}
	return r.room.LocalParticipant.Identity()
}

type connectRoomFunc func(
	url, token string, callback *lksdk.RoomCallback, opts ...lksdk.ConnectOption,
) (roomHandle, error)

func connectSDKRoom(
	url, token string, callback *lksdk.RoomCallback, opts ...lksdk.ConnectOption,
) (roomHandle, error) {
	opts = append([]lksdk.ConnectOption{
		lksdk.WithAutoSubscribe(true),
		lksdk.WithLogger(protoLogger.GetDiscardLogger()),
	}, opts...)
	room, err := lksdk.ConnectToRoomWithToken(
		url,
		token,
		callback,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("connect to livekit room: %w", err)
	}
	return &sdkRoom{room: room}, nil
}

// outboundPacket is one queued data packet. An empty to means broadcast.
type outboundPacket struct {
	data  []byte
	topic string
	to    string
}

// Session is the LiveKit engine handle.
type Session struct {
	url             string
	token           string
	name            string
	refresh         func(ctx context.Context) (engine.Credentials, error)
	connectRoom     connectRoomFunc
	connectOpts     []lksdk.ConnectOption
	room            roomHandle
	roomMu          sync.RWMutex
	onData          func([]byte)
	onPeerData      func(peerID string, data []byte)
	onReconnect     func(*webrtc.DataChannel)
	shouldReconnect func() bool
	onEnded         func(string)
	reconnectCh     chan struct{}
	closeCh         chan struct{}
	lastReconnect   time.Time
	reconnectCount  int
	sendQueue       chan outboundPacket
	closed          atomic.Bool
	reconnecting    atomic.Bool
	done            chan struct{}
	cancel          context.CancelFunc
	shutdownOnce    sync.Once
	sendWorkerOnce  sync.Once
	videoTrackMu    sync.RWMutex
	videoTracks     []webrtc.TrackLocal
	onVideoTrack    func(*webrtc.TrackRemote, *webrtc.RTPReceiver)
	wg              sync.WaitGroup

	onAnnounce     atomic.Pointer[func(peerID string, data []byte)]
	onPeerLeft     atomic.Pointer[func(peerID string)]
	pinned         atomic.Pointer[string]
	foreignDropped atomic.Uint64
}

// New creates a new LiveKit engine session.
func New(ctx context.Context, cfg engine.Config) (engine.Session, error) {
	if cfg.URL == "" {
		return nil, ErrURLRequired
	}
	if cfg.Token == "" {
		return nil, ErrTokenRequired
	}
	_, cancel := context.WithCancel(ctx)
	httpClient := protect.NewHTTPClient(cfg.Resolver)
	wsDialer := protect.NewWebSocketDialer(0, cfg.Resolver)
	connectOpts := []lksdk.ConnectOption{
		lksdk.WithConnectHTTPClient(httpClient),
		lksdk.WithWebSocketDialer(&wsDialer),
	}
	if protect.Protector != nil || cfg.Resolver != nil || runtime.GOOS == "android" {
		pnet, err := protect.NewProtectedNet(cfg.Resolver)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("protected net: %w", err)
		}
		connectOpts = append(connectOpts, lksdk.WithSettingEngineFunc(func(settings *webrtc.SettingEngine) {
			settings.SetNet(pnet)
			settings.SetICEProxyDialer(protect.NewProxyDialer(cfg.Resolver))
		}))
	}
	s := newSession(cfg, connectSDKRoom, connectOpts)
	s.cancel = cancel
	return s, nil
}

func newSession(cfg engine.Config, connect connectRoomFunc, connectOpts []lksdk.ConnectOption) *Session {
	return &Session{
		url:         cfg.URL,
		token:       cfg.Token,
		name:        cfg.Name,
		refresh:     cfg.Refresh,
		connectRoom: connect,
		connectOpts: connectOpts,
		onData:      cfg.OnData,
		onPeerData:  cfg.OnPeerData,
		reconnectCh: make(chan struct{}, 1),
		closeCh:     make(chan struct{}),
		sendQueue:   make(chan outboundPacket, defaultSendQueueSize),
		done:        make(chan struct{}),
	}
}

// Capabilities reports what this engine can do.
func (s *Session) Capabilities() engine.Capabilities {
	return engine.Capabilities{ByteStream: true, VideoTrack: true}
}

// Connect joins the LiveKit room.
func (s *Session) Connect(ctx context.Context) error {
	s.closed.Store(false)
	if err := s.connectSession(ctx); err != nil {
		return err
	}
	s.startSendWorker()
	return nil
}

func (s *Session) connectSession(_ context.Context) error {
	roomCB := &lksdk.RoomCallback{
		ParticipantCallback: lksdk.ParticipantCallback{
			OnDataReceived: func(data []byte, params lksdk.DataReceiveParams) {
				s.handleData(data, params.SenderIdentity, params.Topic)
			},
			OnTrackSubscribed: func(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, _ *lksdk.RemoteParticipant) {
				if track.Kind() != webrtc.RTPCodecTypeVideo {
					return
				}
				s.videoTrackMu.RLock()
				cb := s.onVideoTrack
				s.videoTrackMu.RUnlock()
				if cb != nil {
					cb(track, nil)
				}
			},
		},
		OnParticipantDisconnected: func(rp *lksdk.RemoteParticipant) {
			if rp != nil {
				s.handlePeerLeft(rp.Identity())
			}
		},
		OnDisconnected: func() {
			if s.closed.Load() || s.reconnecting.Load() {
				return
			}
			if !s.queueReconnect() {
				s.signalEnded("disconnected from livekit")
			}
		},
	}

	room, err := s.connectRoom(s.url, s.token, roomCB, s.connectOpts...)
	if err != nil {
		return fmt.Errorf("connect to room: %w", err)
	}
	if src, ok := room.(peerLeftSource); ok {
		src.setPeerLeftSink(s.handlePeerLeft)
	}

	s.setRoom(room)
	if err := s.publishPendingTracks(); err != nil {
		return err
	}
	return nil
}

// handleData routes one received data packet: announces go to the announce
// handler, packets from participants other than the pinned one are dropped,
// the rest go to OnPeerData (with the sender identity) or OnData.
func (s *Session) handleData(data []byte, sender, topic string) {
	if topic == announceTopic {
		if cb := s.onAnnounce.Load(); cb != nil && sender != "" {
			(*cb)(sender, data)
		}
		return
	}
	if pinned := s.PinnedPeer(); pinned != "" && sender != pinned {
		// Another client (or any other participant) in a shared room. Its
		// frames are not for us; pushing them into our smux session would
		// corrupt it.
		if n := s.foreignDropped.Add(1); n == 1 || n%10000 == 0 {
			logger.Debugf("livekit: dropped %d packet(s) from non-server participants (last from %s)", n, sender)
		}
		return
	}
	if s.onPeerData != nil && sender != "" {
		s.onPeerData(sender, data)
		return
	}
	if s.onData != nil {
		s.onData(data)
	}
}

func (s *Session) handlePeerLeft(identity string) {
	if identity == "" {
		return
	}
	if cb := s.onPeerLeft.Load(); cb != nil {
		(*cb)(identity)
	}
}

func (s *Session) publishPendingTracks() error {
	room := s.currentRoom()
	if room == nil {
		return ErrRoomNotConnected
	}
	s.videoTrackMu.RLock()
	defer s.videoTrackMu.RUnlock()
	for _, track := range s.videoTracks {
		if err := room.publishTrack(track); err != nil {
			return fmt.Errorf("failed to publish track: %w", err)
		}
	}
	return nil
}

func (s *Session) startSendWorker() {
	s.sendWorkerOnce.Do(func() {
		s.wg.Add(1)
		go s.processSendQueue()
	})
}

func (s *Session) processSendQueue() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case pkt, ok := <-s.sendQueue:
			if !ok {
				return
			}
			room := s.waitForConnectedRoom()
			if room == nil {
				return
			}
			var dest []string
			if pkt.to != "" {
				dest = []string{pkt.to}
			}
			if err := room.publishData(pkt.data, pkt.topic, dest); err != nil {
				log.Printf("livekit publish data error: %v", err)
			}
		}
	}
}

func (s *Session) waitForConnectedRoom() roomHandle {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		room := s.currentRoom()
		if room != nil && room.connectionState() == lksdk.ConnectionStateConnected {
			return room
		}
		select {
		case <-s.done:
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Session) enqueue(pkt outboundPacket) error {
	if s.closed.Load() {
		return ErrSessionClosed
	}
	select {
	case s.sendQueue <- pkt:
		return nil
	default:
		return ErrSendQueueFull
	}
}

// Send queues data for transmission: to the pinned participant when one is
// pinned, otherwise to the whole room.
func (s *Session) Send(data []byte) error {
	return s.enqueue(outboundPacket{data: data, topic: dataPublishTopic, to: s.PinnedPeer()})
}

// SendTo queues data for one participant. Implements engine.PeerSession.
func (s *Session) SendTo(peerID string, data []byte) error {
	if peerID == "" {
		return s.Send(data)
	}
	return s.enqueue(outboundPacket{data: data, topic: dataPublishTopic, to: peerID})
}

// Announce broadcasts an out-of-band message to the room.
func (s *Session) Announce(data []byte) error {
	return s.enqueue(outboundPacket{data: data, topic: announceTopic})
}

// SetAnnounceHandler registers the callback for announces from other participants.
func (s *Session) SetAnnounceHandler(cb func(peerID string, data []byte)) {
	if cb == nil {
		s.onAnnounce.Store(nil)
		return
	}
	s.onAnnounce.Store(&cb)
}

// SetPeerLeftHandler registers the callback fired when a participant leaves.
func (s *Session) SetPeerLeftHandler(cb func(peerID string)) {
	if cb == nil {
		s.onPeerLeft.Store(nil)
		return
	}
	s.onPeerLeft.Store(&cb)
}

// PinPeer restricts inbound data to peerID and addresses Send to it.
func (s *Session) PinPeer(peerID string) {
	if peerID == "" {
		s.pinned.Store(nil)
		return
	}
	s.pinned.Store(&peerID)
}

// PinnedPeer returns the pinned participant identity, or "".
func (s *Session) PinnedPeer() string {
	if p := s.pinned.Load(); p != nil {
		return *p
	}
	return ""
}

// LocalPeerID returns this participant's identity, or "" when not joined.
func (s *Session) LocalPeerID() string {
	room := s.currentRoom()
	if room == nil {
		return ""
	}
	return room.localIdentity()
}

// Close terminates the session.
func (s *Session) Close() error {
	s.closed.Store(true)
	s.shutdown()
	return nil
}

func (s *Session) shutdown() {
	s.shutdownOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		closeSignal(s.closeCh)
		closeSignal(s.done)
		if room := s.swapRoom(nil); room != nil {
			room.unpublishLocalTracks()
			room.disconnect()
		}
		s.wg.Wait()
	})
}

// SetReconnectCallback stores the reconnect callback.
func (s *Session) SetReconnectCallback(cb func(*webrtc.DataChannel)) { s.onReconnect = cb }

// SetShouldReconnect stores the reconnect predicate.
func (s *Session) SetShouldReconnect(fn func() bool) { s.shouldReconnect = fn }

// SetEndedCallback registers a function to call when the session ends.
func (s *Session) SetEndedCallback(cb func(string)) { s.onEnded = cb }

// WatchConnection monitors the connection lifecycle and reconnects as needed.
func (s *Session) WatchConnection(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closeCh:
			return
		case <-s.reconnectCh:
			if s.handleReconnectAttempt(ctx) {
				return
			}
		}
	}
}

func (s *Session) handleReconnectAttempt(ctx context.Context) bool {
	if time.Since(s.lastReconnect) > reconnectWindow {
		s.reconnectCount = 0
	}
	s.reconnectCount++
	s.lastReconnect = time.Now()

	if s.reconnectCount > maxReconnects {
		s.signalEnded("reconnect limit reached")
		return true
	}

	backoff := time.Duration(s.reconnectCount) * 2 * time.Second
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}

	for {
		if err := s.reconnect(ctx); err != nil {
			logger.Debugf("livekit reconnect failed: %v", err)
			select {
			case <-ctx.Done():
				return true
			case <-s.closeCh:
				return true
			case <-time.After(backoff):
				continue
			}
		}
		s.drainReconnectQueue()
		return false
	}
}

func (s *Session) reconnect(ctx context.Context) error {
	s.reconnecting.Store(true)
	defer s.reconnecting.Store(false)

	if room := s.swapRoom(nil); room != nil {
		room.unpublishLocalTracks()
		room.disconnect()
	}

	if s.refresh != nil {
		creds, err := s.refresh(ctx)
		if err != nil {
			return fmt.Errorf("refresh credentials: %w", err)
		}
		s.applyRefreshedCredentials(creds)
	}

	if err := s.connectSession(ctx); err != nil {
		return err
	}
	// Несущая собрана — писать уже можно. Флаг снимаем ДО обработчика:
	// он синхронно открывает control-поток, а при поднятом флаге запись
	// запрещена, и клиент запирает сам себя до 30-секундного потолка smux.
	s.reconnecting.Store(false)
	if s.onReconnect != nil {
		s.onReconnect(nil)
	}
	return nil
}

func (s *Session) applyRefreshedCredentials(creds engine.Credentials) {
	if creds.URL != "" {
		s.url = creds.URL
	}
	if creds.Token != "" {
		s.token = creds.Token
	}
}

func (s *Session) queueReconnect() bool {
	if s.closed.Load() || s.reconnecting.Load() {
		return false
	}
	if s.shouldReconnect != nil && !s.shouldReconnect() {
		return false
	}
	select {
	case s.reconnectCh <- struct{}{}:
	default:
	}
	return true
}

// Reconnect asks the LiveKit session to tear down its room handle and rejoin.
// Triggered by upper layers when liveness probes declare the carrier dead
// before LiveKit has noticed (silent data-path black-hole).
func (s *Session) Reconnect(reason string) {
	if s.closed.Load() {
		return
	}
	logger.Infof("livekit reconnect requested: %s", reason)
	s.queueReconnect()
}

func (s *Session) drainReconnectQueue() {
	for {
		select {
		case <-s.reconnectCh:
		default:
			return
		}
	}
}

func (s *Session) signalEnded(reason string) {
	s.closed.Store(true)
	s.shutdown()
	if s.onEnded != nil {
		s.onEnded(reason)
	}
}

// CanSend reports whether the session is ready to accept data.
func (s *Session) CanSend() bool {
	if s.closed.Load() || s.reconnecting.Load() || len(s.sendQueue) >= defaultSendQueueCapHard {
		return false
	}
	room := s.currentRoom()
	return room != nil && room.connectionState() == lksdk.ConnectionStateConnected
}

// GetSendQueue is part of engine.Session. The LiveKit queue carries addressed
// packets rather than raw payloads and has no external consumer, so nil is
// returned.
func (s *Session) GetSendQueue() chan []byte { return nil }

// SubscriberCanSend reports whether the subscriber path is ready to send.
func (s *Session) SubscriberCanSend() bool { return s.CanSend() }

// GetBufferedAmount is a stub for LiveKit (the SDK handles its own buffering).
func (s *Session) GetBufferedAmount() uint64 { return 0 }

// AddVideoTrack publishes a video track to the room.
func (s *Session) AddVideoTrack(track webrtc.TrackLocal) error {
	s.videoTrackMu.Lock()
	s.videoTracks = append(s.videoTracks, track)
	s.videoTrackMu.Unlock()

	room := s.currentRoom()
	if room == nil {
		return nil
	}
	if err := room.publishTrack(track); err != nil {
		return fmt.Errorf("failed to publish track: %w", err)
	}
	return nil
}

// SetVideoTrackHandler registers a callback for remote video tracks.
func (s *Session) SetVideoTrackHandler(cb func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	s.videoTrackMu.Lock()
	defer s.videoTrackMu.Unlock()
	s.onVideoTrack = cb
}

func (s *Session) currentRoom() roomHandle {
	s.roomMu.RLock()
	defer s.roomMu.RUnlock()
	return s.room
}

func (s *Session) setRoom(room roomHandle) {
	s.roomMu.Lock()
	defer s.roomMu.Unlock()
	s.room = room
}

func (s *Session) swapRoom(room roomHandle) roomHandle {
	s.roomMu.Lock()
	defer s.roomMu.Unlock()
	old := s.room
	s.room = room
	return old
}

func closeSignal(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

var (
	_ engine.PeerSession          = (*Session)(nil)
	_ engine.RoomDirectorySession = (*Session)(nil)
)

func init() { //nolint:gochecknoinits // engine registration is the canonical Go pattern for plugins
	engine.Register("livekit", New)
}
