package mobile

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

const (
	defaultCheckTimeout = 8 * time.Second
	defaultPingTimeout  = 10 * time.Second
	probeShutdownWait   = 2 * time.Second
	maxPortAttempts     = 16
)

type probeReadyAction func(context.Context, string) (int64, error)

// Check starts an isolated client and returns RTC startup latency in milliseconds.
func (r *Runtime) Check(
	providerName, transportName, roomID, deviceID, keyHex string,
	socksPort, timeoutMillis, vp8FPS, vp8BatchSize int,
) (int64, error) {
	cfg, socksAddr, release, err := r.probeConfig(
		providerName, transportName, roomID, deviceID, keyHex, socksPort, vp8FPS, vp8BatchSize,
	)
	if err != nil {
		return 0, err
	}
	defer release()
	return r.runProbe(cfg, socksAddr, timeoutFromMillis(timeoutMillis, defaultCheckTimeout), nil)
}

// Ping starts an isolated client and returns HTTP latency through its SOCKS tunnel.
func (r *Runtime) Ping(
	providerName, transportName, roomID, deviceID, keyHex string,
	socksPort, timeoutMillis int,
	pingURL string,
	vp8FPS, vp8BatchSize int,
) (int64, error) {
	cfg, socksAddr, release, err := r.probeConfig(
		providerName, transportName, roomID, deviceID, keyHex, socksPort, vp8FPS, vp8BatchSize,
	)
	if err != nil {
		return 0, err
	}
	defer release()
	action := func(ctx context.Context, address string) (int64, error) {
		return httpPingThroughSocks(ctx, address, pingURL)
	}
	return r.runProbe(cfg, socksAddr, timeoutFromMillis(timeoutMillis, defaultPingTimeout), action)
}

func (r *Runtime) probeConfig(
	providerName, transportName, roomID, deviceID, keyHex string,
	socksPort, vp8FPS, vp8BatchSize int,
) (client.Config, string, func(), error) {
	r.mu.Lock()
	temporary := r.defaults
	r.mu.Unlock()

	temporary.provider = providerName
	temporary.transport = transportName
	temporary.roomURL = roomID
	temporary.deviceID = deviceID
	temporary.keyHex = keyHex
	temporary.socksHost = defaultSOCKSHost
	temporary.socksUser = ""
	temporary.socksPass = ""
	if vp8FPS > 0 {
		temporary.vp8.FPS = vp8FPS
	}
	if vp8BatchSize > 0 {
		temporary.vp8.BatchSize = vp8BatchSize
	}
	port, release, err := r.claimProbePort(socksPort)
	if err != nil {
		return client.Config{}, "", nil, err
	}
	temporary.socksPort = port
	if err := validateRuntimeConfig(temporary); err != nil {
		release()
		return client.Config{}, "", nil, err
	}
	if err := validateProbeVP8(temporary.vp8); err != nil {
		release()
		return client.Config{}, "", nil, err
	}
	return temporary.clientConfig(), net.JoinHostPort(defaultSOCKSHost, strconv.Itoa(port)), release, nil
}

func validateProbeVP8(options client.VP8Options) error {
	if options.FPS < 1 || options.FPS > 120 || options.BatchSize < 1 {
		return fmt.Errorf("%w: invalid VP8 probe options", ErrInvalidConfig)
	}
	return nil
}

func (r *Runtime) runProbe(
	cfg client.Config,
	socksAddr string,
	timeout time.Duration,
	action probeReadyAction,
) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ready := make(chan struct{})
	done := make(chan error, 1)
	var readyOnce sync.Once
	started := time.Now()
	go func() {
		done <- r.runner(ctx, cfg, func() { readyOnce.Do(func() { close(ready) }) })
	}()

	select {
	case <-ready:
		return finishProbe(ctx, cancel, done, started, socksAddr, action)
	case err := <-done:
		if channelClosed(ready) {
			return finishProbe(ctx, cancel, nil, started, socksAddr, action)
		}
		if err != nil {
			return 0, err
		}
		return 0, ErrStoppedBeforeReady
	case <-ctx.Done():
		waitProbeDone(done)
		return 0, ErrReadyTimeout
	}
}

func finishProbe(
	ctx context.Context,
	cancel context.CancelFunc,
	done <-chan error,
	started time.Time,
	socksAddr string,
	action probeReadyAction,
) (int64, error) {
	var latency int64
	var err error
	if action == nil {
		latency = time.Since(started).Milliseconds()
	} else {
		latency, err = action(ctx, socksAddr)
	}
	cancel()
	if done != nil {
		waitProbeDone(done)
	}
	return latency, err
}

func waitProbeDone(done <-chan error) {
	if done == nil {
		return
	}
	timer := time.NewTimer(probeShutdownWait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

func (r *Runtime) claimProbePort(requested int) (int, func(), error) {
	if requested < 0 || requested > 65535 {
		return 0, nil, fmt.Errorf("%w: probe port must be between 0 and 65535", ErrInvalidConfig)
	}
	if requested > 0 {
		return r.claimPort(requested)
	}
	for range maxPortAttempts {
		port, err := ephemeralPort()
		if err != nil {
			return 0, nil, err
		}
		claimed, release, claimErr := r.claimPort(port)
		if claimErr == nil {
			return claimed, release, nil
		}
	}
	return 0, nil, fmt.Errorf("%w: no ephemeral port available", ErrProbePortInUse)
}

func (r *Runtime) claimPort(port int) (int, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activePortLocked() == port {
		return 0, nil, ErrProbePortInUse
	}
	if _, exists := r.probePorts[port]; exists {
		return 0, nil, ErrProbePortInUse
	}
	r.probePorts[port] = struct{}{}
	return port, func() { r.releaseProbePort(port) }, nil
}

func (r *Runtime) releaseProbePort(port int) {
	r.mu.Lock()
	delete(r.probePorts, port)
	r.mu.Unlock()
}

func (r *Runtime) activePortLocked() int {
	if !r.isActiveLocked() || r.current == nil {
		return 0
	}
	_, portText, err := net.SplitHostPort(r.current.cfg.LocalAddr)
	if err != nil {
		return 0
	}
	port, _ := strconv.Atoi(portText)
	return port
}

func (r *Runtime) probePortClaimedLocked(address string) bool {
	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return false
	}
	_, claimed := r.probePorts[port]
	return claimed
}

func ephemeralPort() (int, error) {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp4", net.JoinHostPort(defaultSOCKSHost, "0"))
	if err != nil {
		return 0, fmt.Errorf("reserve probe port: %w", err)
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		_ = listener.Close()
		return 0, fmt.Errorf("reserve probe port: %w", ErrInvalidConfig)
	}
	port := tcpAddress.Port
	if err := listener.Close(); err != nil {
		return 0, fmt.Errorf("release probe port reservation: %w", err)
	}
	return port, nil
}
