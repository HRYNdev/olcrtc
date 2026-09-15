package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
	"github.com/openlibrecommunity/olcrtc/internal/client"
	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/engine/livekit"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/server"
)

// These tests run the real server, client, datachannel transport and LiveKit
// engine over livekit.MemoryHub, an in-process LiveKit room with identities,
// addressing and participant-left events, so that several clients share one
// room with one server exactly as they do in a WB Stream call.

var errBlobMismatch = errors.New("blob hash mismatch")

// legacyLiveKitSession hides the addressed-mode surface of the LiveKit engine
// and reproduces the engine built before it: no SendTo, no room directory,
// broadcast sends, and every packet (server beacons included) delivered to
// OnData.
type legacyLiveKitSession struct{ engine.Session }

func newLegacyLiveKitSession(hub *livekit.MemoryHub, cfg enginebuiltin.Config) engine.Session {
	sess := hub.NewSession(engine.Config{Name: cfg.Name, OnData: cfg.OnData})
	if rd, ok := sess.(engine.RoomDirectorySession); ok && cfg.OnData != nil {
		onData := cfg.OnData
		rd.SetAnnounceHandler(func(_ string, data []byte) { onData(data) })
	}
	return legacyLiveKitSession{Session: sess}
}

func registerHubCarrier(t *testing.T, hub *livekit.MemoryHub, legacy bool) string {
	t.Helper()
	session.RegisterDefaults()
	name := fmt.Sprintf("e2e-lkhub-%s-legacy=%t", t.Name(), legacy)
	enginebuiltin.Register(name, func(_ context.Context, cfg enginebuiltin.Config) (engine.Session, error) {
		if legacy {
			return newLegacyLiveKitSession(hub, cfg), nil
		}
		return hub.NewSession(engine.Config{Name: cfg.Name, OnData: cfg.OnData, OnPeerData: cfg.OnPeerData}), nil
	})
	return name
}

func waitHubParticipants(t *testing.T, hub *livekit.MemoryHub, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(hub.Participants()) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("hub participants = %v, want at least %d", hub.Participants(), n)
}

type dcCloseRecord struct{ sid, device, reason string }

type dcServerLog struct {
	mu      sync.Mutex
	devices map[string]string
	opens   int
	closes  []dcCloseRecord
}

func (l *dcServerLog) snapshot() (int, []dcCloseRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.opens, append([]dcCloseRecord(nil), l.closes...)
}

func startDCServer(t *testing.T, ctx context.Context, carrier string) *dcServerLog {
	t.Helper()
	log := &dcServerLog{devices: make(map[string]string)}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(ctx, server.Config{
			Transport: transportData,
			Carrier:   carrier,
			RoomURL:   testRoom,
			KeyHex:    testKeyHex,
			DNSServer: localDNSServer,
			OnSessionOpen: func(sid, device string, _ map[string]any) {
				log.mu.Lock()
				log.devices[sid] = device
				log.opens++
				log.mu.Unlock()
			},
			OnSessionClose: func(sid, reason string) {
				log.mu.Lock()
				log.closes = append(log.closes, dcCloseRecord{sid: sid, device: log.devices[sid], reason: reason})
				log.mu.Unlock()
			},
		})
	}()
	t.Cleanup(func() {
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
		}
	})
	return log
}

type dcClient struct {
	device     string
	socks      string
	cancel     context.CancelFunc
	ready      chan struct{}
	errCh      chan error
	reconnects atomic.Uint64
}

func startDCClient(t *testing.T, parent context.Context, carrier, device string) *dcClient {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	c := &dcClient{
		device: device,
		socks:  freeLocalAddr(ctx, t),
		cancel: cancel,
		ready:  make(chan struct{}),
		errCh:  make(chan error, 1),
	}
	go func() {
		c.errCh <- client.RunWithReady(ctx, client.Config{
			Transport: transportData,
			Carrier:   carrier,
			RoomURL:   testRoom,
			KeyHex:    testKeyHex,
			DeviceID:  device,
			LocalAddr: c.socks,
			DNSServer: localDNSServer,
			OnHealth: func(st control.Status) {
				for {
					cur := c.reconnects.Load()
					if st.Reconnects <= cur || c.reconnects.CompareAndSwap(cur, st.Reconnects) {
						return
					}
				}
			},
		}, func() { close(c.ready) })
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-c.errCh:
		case <-time.After(10 * time.Second):
		}
	})
	return c
}

func waitDCClientsReady(t *testing.T, budget time.Duration, clients ...*dcClient) {
	t.Helper()
	deadline := time.After(budget)
	for _, c := range clients {
		select {
		case <-c.ready:
		case err := <-c.errCh:
			t.Fatalf("client %s exited before ready: %v", c.device, err)
		case <-deadline:
			t.Fatalf("client %s not ready within %s", c.device, budget)
		}
	}
}

// Blob service: 'D' streams size deterministic bytes for seed, 'U' reads size
// bytes and answers their SHA-256. Both directions of the tunnel are checked
// byte for byte without keeping payloads in memory.
func startBlobServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveBlob(conn)
		}
	}()
	return ln.Addr().String()
}

func serveBlob(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	var hdr [13]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return
	}
	seed := binary.BigEndian.Uint64(hdr[1:9])
	size := int64(binary.BigEndian.Uint32(hdr[9:13]))
	switch hdr[0] {
	case 'D':
		_, _ = io.CopyN(conn, blobSource(seed), size)
	case 'U':
		h := sha256.New()
		if _, err := io.CopyN(h, conn, size); err == nil {
			_, _ = conn.Write(h.Sum(nil))
		}
	}
}

func blobSource(seed uint64) io.Reader {
	var key [32]byte
	binary.BigEndian.PutUint64(key[:8], seed)
	return rand.NewChaCha8(key)
}

func blobHash(seed uint64, size int64) []byte {
	h := sha256.New()
	_, _ = io.CopyN(h, blobSource(seed), size)
	return h.Sum(nil)
}

func socksDial(socksAddr, targetAddr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp4", socksAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial socks: %w", err)
	}
	fail := func(step string, err error) (net.Conn, error) {
		_ = conn.Close()
		return nil, fmt.Errorf("socks %s: %w", step, err)
	}
	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		return fail("greeting", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil || greeting[1] != 0 {
		return fail("greeting reply", fmt.Errorf("%w %v", err, greeting))
	}
	host, portText, _ := net.SplitHostPort(targetAddr)
	port, _ := strconv.Atoi(portText)
	req := append([]byte{5, 1, 0, 1}, net.ParseIP(host).To4()...)
	req = binary.BigEndian.AppendUint16(req, uint16(port)) //nolint:gosec // port from a local listener
	if _, err := conn.Write(req); err != nil {
		return fail("connect", err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fail("connect reply", err)
	}
	if reply[1] != 0 {
		return fail("connect reply", fmt.Errorf("%w: %v", errSocksUnexpectedReply, reply))
	}
	return conn, nil
}

type countingWriter struct{ n *atomic.Int64 }

func (w countingWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))
	return len(p), nil
}

func blobDownload(socks, blob string, seed uint64, size int64) error {
	return blobDownloadCounted(socks, blob, seed, size, nil)
}

// blobDownloadCounted is blobDownload that also adds received bytes to
// progress as they arrive, for rate measurements over a fixed window.
func blobDownloadCounted(socks, blob string, seed uint64, size int64, progress *atomic.Int64) error {
	conn, err := socksDial(socks, blob)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var hdr [13]byte
	hdr[0] = 'D'
	binary.BigEndian.PutUint64(hdr[1:9], seed)
	binary.BigEndian.PutUint32(hdr[9:13], uint32(size)) //nolint:gosec // test sizes are small
	if _, err := conn.Write(hdr[:]); err != nil {
		return fmt.Errorf("download request: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
	h := sha256.New()
	var sink io.Writer = h
	if progress != nil {
		sink = io.MultiWriter(h, countingWriter{progress})
	}
	n, err := io.Copy(sink, conn)
	if err != nil {
		return fmt.Errorf("download after %d bytes: %w", n, err)
	}
	if n != size || !bytes.Equal(h.Sum(nil), blobHash(seed, size)) {
		return fmt.Errorf("%w: download got %d/%d bytes", errBlobMismatch, n, size)
	}
	return nil
}

func blobUpload(socks, blob string, seed uint64, size int64) error {
	conn, err := socksDial(socks, blob)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var hdr [13]byte
	hdr[0] = 'U'
	binary.BigEndian.PutUint64(hdr[1:9], seed)
	binary.BigEndian.PutUint32(hdr[9:13], uint32(size)) //nolint:gosec // test sizes are small
	if _, err := conn.Write(hdr[:]); err != nil {
		return fmt.Errorf("upload request: %w", err)
	}
	if _, err := io.CopyN(conn, blobSource(seed), size); err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
	sum := make([]byte, sha256.Size)
	if _, err := io.ReadFull(conn, sum); err != nil {
		return fmt.Errorf("upload hash: %w", err)
	}
	if !bytes.Equal(sum, blobHash(seed, size)) {
		return fmt.Errorf("%w: upload", errBlobMismatch)
	}
	return nil
}

// startRogueBroadcaster joins the room as a participant speaking the old
// broadcast protocol with the same key: encrypted smux data frames for the
// stream ids a client uses. Without sender pinning they would land in every
// client's control and tunnel streams.
func startRogueBroadcaster(t *testing.T, hub *livekit.MemoryHub) {
	t.Helper()
	sess := hub.NewSession(engine.Config{})
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	cipher, err := runtime.SetupCipher(testKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // test noise
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			sendRogueFrame(sess, cipher, uint32(1+2*(i%3)), rng) //nolint:gosec // small ids
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
		_ = sess.Close()
	})
}

func sendRogueFrame(sess engine.Session, cipher *crypto.Cipher, sid uint32, rng *rand.Rand) {
	const bodyLen = 96
	frame := make([]byte, 8+bodyLen)
	frame[0], frame[1] = 2, 2 // smux v2 PSH
	binary.LittleEndian.PutUint16(frame[2:4], bodyLen)
	binary.LittleEndian.PutUint32(frame[4:8], sid)
	for i := 8; i < len(frame); i++ {
		frame[i] = byte(rng.Uint32())
	}
	if ct, err := cipher.Encrypt(frame); err == nil {
		_ = sess.Send(ct)
	}
}

func TestDatachannelAddressedFourClientsShareRoom(t *testing.T) {
	hub := livekit.NewMemoryHub()
	hub.SubscribeDelay = time.Second // a joining participant misses its first second, as on LiveKit
	carrier := registerHubCarrier(t, hub, false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	blob := startBlobServer(t)

	srvLog := startDCServer(t, ctx, carrier)
	waitHubParticipants(t, hub, 1)
	startRogueBroadcaster(t, hub)

	clients := make([]*dcClient, 4)
	for i := range clients {
		clients[i] = startDCClient(t, ctx, carrier, fmt.Sprintf("client-%d", i))
	}
	waitDCClientsReady(t, 30*time.Second, clients...)

	const down, up = 3 << 20, 1 << 20
	errs := make(chan error, 2*len(clients))
	for i, c := range clients {
		go func(i int, c *dcClient) {
			errs <- blobDownload(c.socks, blob, uint64(100+i), down) //nolint:gosec // small index
		}(i, c)
		go func(i int, c *dcClient) {
			errs <- blobUpload(c.socks, blob, uint64(200+i), up) //nolint:gosec // small index
		}(i, c)
	}
	for range 2 * len(clients) {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	opens, closes := srvLog.snapshot()
	if opens != len(clients) || len(closes) != 0 {
		t.Fatalf("server sessions opened=%d closed=%v, want %d opened and none closed", opens, closes, len(clients))
	}
	for _, c := range clients {
		if n := c.reconnects.Load(); n != 0 {
			t.Fatalf("client %s reconnected %d time(s)", c.device, n)
		}
	}
}

func TestDatachannelAddressedClientLeavesAndFifthJoins(t *testing.T) {
	hub := livekit.NewMemoryHub()
	carrier := registerHubCarrier(t, hub, false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	blob := startBlobServer(t)

	srvLog := startDCServer(t, ctx, carrier)
	waitHubParticipants(t, hub, 1)
	clients := make([]*dcClient, 4)
	for i := range clients {
		clients[i] = startDCClient(t, ctx, carrier, fmt.Sprintf("client-%d", i))
	}
	waitDCClientsReady(t, 30*time.Second, clients...)

	stop := make(chan struct{})
	errs := make(chan error, 3)
	for i, c := range clients[1:] {
		go func(i int, c *dcClient) {
			for round := 0; ; round++ {
				select {
				case <-stop:
					errs <- nil
					return
				default:
				}
				if err := blobDownload(c.socks, blob, uint64(i*1000+round), 1<<20); err != nil { //nolint:gosec // small
					errs <- fmt.Errorf("%s round %d: %w", c.device, round, err)
					return
				}
			}
		}(i, c)
	}

	time.Sleep(300 * time.Millisecond)
	clients[0].cancel()
	select {
	case <-clients[0].errCh:
	case <-time.After(15 * time.Second):
		t.Fatal("leaving client did not stop")
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, closes := srvLog.snapshot(); len(closes) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server kept the session of the departed client")
		}
		time.Sleep(20 * time.Millisecond)
	}

	fifth := startDCClient(t, ctx, carrier, "client-4")
	waitDCClientsReady(t, 30*time.Second, fifth)
	if err := blobDownload(fifth.socks, blob, 4242, 2<<20); err != nil {
		t.Fatalf("fifth client: %v", err)
	}
	if err := blobUpload(fifth.socks, blob, 4343, 1<<20); err != nil {
		t.Fatalf("fifth client: %v", err)
	}

	close(stop)
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatalf("remaining client disturbed: %v", err)
		}
	}
	opens, closes := srvLog.snapshot()
	if opens != 5 {
		t.Fatalf("server sessions opened = %d, want 5", opens)
	}
	if len(closes) != 1 || closes[0].device != "client-0" {
		t.Fatalf("server session closes = %+v, want exactly client-0", closes)
	}
	t.Logf("departed client session closed with reason %q", closes[0].reason)
	for _, c := range append(clients[1:], fifth) {
		if n := c.reconnects.Load(); n != 0 {
			t.Fatalf("client %s reconnected %d time(s)", c.device, n)
		}
	}
}

// Old client (broadcast, no beacon handling) next to new clients on a new
// server: the server beacons reach the old client's smux and must be ignored
// there, and the new clients must not see its broadcast uplink.
func TestDatachannelLegacyClientBesideAddressedClients(t *testing.T) {
	hub := livekit.NewMemoryHub()
	carrier := registerHubCarrier(t, hub, false)
	legacyCarrier := registerHubCarrier(t, hub, true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	blob := startBlobServer(t)

	srvLog := startDCServer(t, ctx, carrier)
	waitHubParticipants(t, hub, 1)
	legacy := startDCClient(t, ctx, legacyCarrier, "legacy-client")
	fresh := []*dcClient{
		startDCClient(t, ctx, carrier, "client-a"),
		startDCClient(t, ctx, carrier, "client-b"),
	}
	all := append([]*dcClient{legacy}, fresh...)
	waitDCClientsReady(t, 30*time.Second, all...)

	errs := make(chan error, 2*len(all))
	for i, c := range all {
		go func(i int, c *dcClient) {
			errs <- blobDownload(c.socks, blob, uint64(300+i), 2<<20) //nolint:gosec // small
		}(i, c)
		go func(i int, c *dcClient) {
			errs <- blobUpload(c.socks, blob, uint64(400+i), 1<<20) //nolint:gosec // small
		}(i, c)
	}
	for range 2 * len(all) {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	// Beacons keep flowing into the legacy client's smux while it is idle.
	time.Sleep(2 * runtime.ServerBeaconInterval)
	if err := blobDownload(legacy.socks, blob, 999, 256<<10); err != nil {
		t.Fatalf("legacy client after beacons: %v", err)
	}
	opens, closes := srvLog.snapshot()
	if opens != 3 || len(closes) != 0 {
		t.Fatalf("server opened=%d closed=%v, want 3 and none", opens, closes)
	}
	for _, c := range all {
		if n := c.reconnects.Load(); n != 0 {
			t.Fatalf("client %s reconnected %d time(s)", c.device, n)
		}
	}
}

// New client against an old server (single session, no beacon): the client
// waits for the old server's first keepalive, then the beacon grace, and
// falls back to broadcast.
func TestDatachannelAddressedClientWithLegacyServer(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the legacy server keepalive (~16s)")
	}
	hub := livekit.NewMemoryHub()
	carrier := registerHubCarrier(t, hub, false)
	legacyCarrier := registerHubCarrier(t, hub, true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	blob := startBlobServer(t)

	srvLog := startDCServer(t, ctx, legacyCarrier)
	waitHubParticipants(t, hub, 1)
	start := time.Now()
	c := startDCClient(t, ctx, carrier, "client-new")
	waitDCClientsReady(t, 45*time.Second, c)
	t.Logf("new client ready against legacy server in %s", time.Since(start).Round(100*time.Millisecond))
	if err := blobDownload(c.socks, blob, 7, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := blobUpload(c.socks, blob, 8, 512<<10); err != nil {
		t.Fatal(err)
	}
	if opens, _ := srvLog.snapshot(); opens != 1 {
		t.Fatalf("legacy server opened %d sessions, want 1", opens)
	}
}

// TestDatachannelPeersFairness shows what one shared engine send queue does to
// clients sharing a server over a narrow pipe. Every participant's uplink is
// capped (OLCRTC_DCPEERS_UPLINK_MBIT, default 40) like a WB data channel;
// three clients download continuously while a fourth makes small requests.
// With a single FIFO queue the light client waits behind megabytes queued
// for the heavy ones.
//
//	OLCRTC_DCPEERS_BENCH=1 go test ./internal/e2e -run TestDatachannelPeersFairness -v
func TestDatachannelPeersFairness(t *testing.T) {
	if os.Getenv("OLCRTC_DCPEERS_BENCH") == "" {
		t.Skip("set OLCRTC_DCPEERS_BENCH=1 to run")
	}
	uplinkMbit := int64(40)
	if v, err := strconv.Atoi(os.Getenv("OLCRTC_DCPEERS_UPLINK_MBIT")); err == nil && v > 0 {
		uplinkMbit = int64(v)
	}
	hub := livekit.NewMemoryHub()
	hub.UplinkBitsPerSec = uplinkMbit * 1_000_000
	carrier := registerHubCarrier(t, hub, false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	blob := startBlobServer(t)
	startDCServer(t, ctx, carrier)
	waitHubParticipants(t, hub, 1)
	clients := make([]*dcClient, 4)
	for i := range clients {
		clients[i] = startDCClient(t, ctx, carrier, fmt.Sprintf("fair-%d", i))
	}
	waitDCClientsReady(t, 30*time.Second, clients...)
	light := clients[3]

	const lightSize = 16 << 10
	// sample makes at least n light requests and keeps going until minWindow
	// has passed, so the heavy rate is measured over a fixed-length window.
	sample := func(n int, minWindow time.Duration, seed uint64) []time.Duration {
		out := make([]time.Duration, 0, n)
		start := time.Now()
		for i := 0; i < n || time.Since(start) < minWindow; i++ {
			t0 := time.Now()
			if err := blobDownload(light.socks, blob, seed+uint64(i), lightSize); err != nil { //nolint:gosec // small
				t.Fatalf("light client: %v", err)
			}
			out = append(out, time.Since(t0))
			time.Sleep(200 * time.Millisecond)
		}
		sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
		return out
	}
	pct := func(d []time.Duration, p float64) time.Duration { return d[int(float64(len(d)-1)*p)] }

	alone := sample(10, 0, 10_000)

	var heavyBytes atomic.Int64
	stop := make(chan struct{})
	var heavy sync.WaitGroup
	for i, c := range clients[:3] {
		heavy.Add(1)
		go func(i int, c *dcClient) {
			defer heavy.Done()
			for round := 0; ; round++ {
				select {
				case <-stop:
					return
				default:
				}
				err := blobDownloadCounted(c.socks, blob, uint64(i*100_000+round), 8<<20, &heavyBytes) //nolint:gosec // small
				if err != nil {
					select {
					case <-stop:
					default:
						t.Errorf("heavy %s: %v", c.device, err)
					}
					return
				}
			}
		}(i, c)
	}
	time.Sleep(3 * time.Second) // let the heavy transfers fill the queues
	bytesBefore := heavyBytes.Load()
	start := time.Now()
	loaded := sample(30, 15*time.Second, 20_000)
	elapsed := time.Since(start)
	heavyMbit := float64((heavyBytes.Load()-bytesBefore)*8) / 1e6 / elapsed.Seconds()
	close(stop)
	cancel()
	heavy.Wait()

	t.Logf("uplink cap %d Mbit/s; light 16 KiB request alone: p50 %s max %s",
		uplinkMbit, pct(alone, 0.5).Round(time.Millisecond), alone[len(alone)-1].Round(time.Millisecond))
	t.Logf("light 16 KiB request beside 3 heavy downloads: p50 %s p90 %s max %s; heavy total ≈%.0f Mbit/s",
		pct(loaded, 0.5).Round(time.Millisecond), pct(loaded, 0.9).Round(time.Millisecond),
		loaded[len(loaded)-1].Round(time.Millisecond), heavyMbit)
}

// TestDatachannelPeersThroughput measures the in-memory tunnel with 1, 2 and 4
// clients sharing one server. The hub has no bandwidth limit, so the numbers
// show whether olcrtc's own code (engine queue, server peer routing) is the
// bottleneck, not what a WB Stream room can carry.
//
//	OLCRTC_DCPEERS_BENCH=1 [OLCRTC_DCPEERS_MB=64] go test ./internal/e2e -run TestDatachannelPeersThroughput -v
func TestDatachannelPeersThroughput(t *testing.T) {
	if os.Getenv("OLCRTC_DCPEERS_BENCH") == "" {
		t.Skip("set OLCRTC_DCPEERS_BENCH=1 to run")
	}
	sizeMB := 64
	if v, err := strconv.Atoi(os.Getenv("OLCRTC_DCPEERS_MB")); err == nil && v > 0 {
		sizeMB = v
	}
	size := int64(sizeMB) << 20
	for _, n := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("clients=%d", n), func(t *testing.T) {
			hub := livekit.NewMemoryHub()
			carrier := registerHubCarrier(t, hub, false)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			blob := startBlobServer(t)
			startDCServer(t, ctx, carrier)
			waitHubParticipants(t, hub, 1)
			clients := make([]*dcClient, n)
			for i := range clients {
				clients[i] = startDCClient(t, ctx, carrier, fmt.Sprintf("bench-%d", i))
			}
			waitDCClientsReady(t, 30*time.Second, clients...)

			for _, dir := range []string{"download", "upload"} {
				durations := make([]time.Duration, n)
				errs := make(chan error, n)
				start := time.Now()
				for i, c := range clients {
					go func(i int, c *dcClient) {
						t0 := time.Now()
						var err error
						if dir == "download" {
							err = blobDownload(c.socks, blob, uint64(i+1), size) //nolint:gosec // small
						} else {
							err = blobUpload(c.socks, blob, uint64(i+1), size) //nolint:gosec // small
						}
						durations[i] = time.Since(t0)
						errs <- err
					}(i, c)
				}
				for range n {
					if err := <-errs; err != nil {
						t.Fatal(err)
					}
				}
				wall := time.Since(start)
				sort.Slice(durations, func(a, b int) bool { return durations[a] < durations[b] })
				mbit := func(d time.Duration) float64 { return float64(size*8) / 1e6 / d.Seconds() }
				t.Logf("clients=%d %s %d MiB each: total %.0f Mbit/s, per client fastest %.0f / slowest %.0f Mbit/s",
					n, dir, sizeMB, float64(int64(n)*size*8)/1e6/wall.Seconds(), mbit(durations[0]), mbit(durations[n-1]))
			}
		})
	}
}
