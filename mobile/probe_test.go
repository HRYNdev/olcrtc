package mobile

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

func TestCheckUsesEphemeralPortAndAvoidsActiveRuntime(t *testing.T) {
	configs := make(chan client.Config, 2)
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func()) error {
		configs <- cfg
		onReady()
		<-ctx.Done()
		return ctx.Err()
	})
	if err := runtime.SetSocksPort(19081); err != nil {
		t.Fatalf("SetSocksPort() error = %v", err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(100); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	activeConfig := <-configs
	if _, err := runtime.Check("jitsi", transportVP8, testRoom, "device", testKey, 19081, 100, 30, 64); !errors.Is(err, ErrProbePortInUse) {
		t.Fatalf("Check(active port) error = %v, want %v", err, ErrProbePortInUse)
	}
	latency, err := runtime.Check("jitsi", transportVP8, testRoom, "device", testKey, 0, 100, 30, 64)
	if err != nil {
		t.Fatalf("Check(ephemeral port) error = %v", err)
	}
	if latency < 0 {
		t.Fatalf("Check() latency = %d", latency)
	}
	probeConfig := <-configs
	_, activePort, _ := net.SplitHostPort(activeConfig.LocalAddr)
	probeHost, probePort, splitErr := net.SplitHostPort(probeConfig.LocalAddr)
	if splitErr != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", probeConfig.LocalAddr, splitErr)
	}
	if probeHost != defaultSOCKSHost || probePort == "0" || probePort == activePort {
		t.Fatalf("probe address = %q, active = %q", probeConfig.LocalAddr, activeConfig.LocalAddr)
	}
	if err := runtime.Stop(100); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestCheckSupportsEveryTransport(t *testing.T) {
	configs := make(chan client.Config, 4)
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func()) error {
		configs <- cfg
		onReady()
		<-ctx.Done()
		return ctx.Err()
	})
	for _, transport := range []string{transportData, transportVP8, transportSEI, transportVideo} {
		if _, err := runtime.Check("jitsi", transport, testRoom, "device", testKey, 0, 100, 30, 64); err != nil {
			t.Fatalf("Check(%q) error = %v", transport, err)
		}
		cfg := <-configs
		if cfg.Transport != transport {
			t.Fatalf("Check(%q) transport = %q", transport, cfg.Transport)
		}
	}
}

func TestCheckTimeoutAndRunnerError(t *testing.T) {
	runtime := configuredRuntime(t, func(ctx context.Context, _ client.Config, _ func()) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if _, err := runtime.Check("jitsi", transportData, testRoom, "device", testKey, 0, 1, 0, 0); !errors.Is(err, ErrReadyTimeout) {
		t.Fatalf("Check(timeout) error = %v, want %v", err, ErrReadyTimeout)
	}
	runtime.runner = func(context.Context, client.Config, func()) error { return errTestRun }
	if _, err := runtime.Check("jitsi", transportData, testRoom, "device", testKey, 0, 100, 0, 0); !errors.Is(err, errTestRun) {
		t.Fatalf("Check(runner error) = %v, want %v", err, errTestRun)
	}
}

func TestPingURLAndHTTPRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	httpClient := server.Client()
	latency, err := singleHTTPPingRequest(context.Background(), httpClient, server.URL, httpPingSampleTimeout)
	if err != nil {
		t.Fatalf("singleHTTPPingRequest() error = %v", err)
	}
	if latency < 0 {
		t.Fatalf("latency = %d", latency)
	}
	if _, err := normalizeHTTPPingURL("file:///tmp/test"); err == nil {
		t.Fatal("normalizeHTTPPingURL() accepted unsupported scheme")
	}
}

func TestEphemeralPortIsUsable(t *testing.T) {
	port, err := ephemeralPort()
	if err != nil {
		t.Fatalf("ephemeralPort() error = %v", err)
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort(defaultSOCKSHost, strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("listen on ephemeral port %d: %v", port, err)
	}
	_ = listener.Close()
}
