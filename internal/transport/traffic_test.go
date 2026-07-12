package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

type trafficStubTransport struct {
	features Features
	sent     [][]byte
}

func (s *trafficStubTransport) Connect(context.Context) error { return nil }
func (s *trafficStubTransport) Send(data []byte) error {
	s.sent = append(s.sent, append([]byte(nil), data...))
	return nil
}
func (s *trafficStubTransport) Close() error                    { return nil }
func (s *trafficStubTransport) SetReconnectCallback(func())     {}
func (s *trafficStubTransport) SetShouldReconnect(func() bool)  {}
func (s *trafficStubTransport) SetEndedCallback(func(string))   {}
func (s *trafficStubTransport) WatchConnection(context.Context) {}
func (s *trafficStubTransport) CanSend() bool                   { return true }
func (s *trafficStubTransport) Reconnect(string)                {}
func (s *trafficStubTransport) Features() Features              { return s.features }

func TestWithTrafficReturnsInnerWhenDisabled(t *testing.T) {
	inner := &trafficStubTransport{}
	got := WithTraffic(inner, TrafficConfig{})
	if got != inner {
		t.Fatalf("WithTraffic disabled returned %T, want inner", got)
	}
}

func TestTrafficWrapperRejectsOversizedPayloadAndClampsFeatures(t *testing.T) {
	inner := &trafficStubTransport{features: Features{MaxPayloadSize: 5}}
	tr := WithTraffic(inner, TrafficConfig{MaxPayloadSize: 10})
	if features := tr.Features(); features.MaxPayloadSize != 5 {
		t.Fatalf("Features().MaxPayloadSize = %d, want 5", features.MaxPayloadSize)
	}
	err := tr.Send([]byte("123456"))
	if !errors.Is(err, ErrTrafficPayloadTooLarge) {
		t.Fatalf("Send() error = %v, want %v", err, ErrTrafficPayloadTooLarge)
	}
	if len(inner.sent) != 0 {
		t.Fatalf("inner sent %d payloads, want 0", len(inner.sent))
	}
	if err := tr.Send([]byte("12345")); err != nil {
		t.Fatalf("Send(max sized) error = %v", err)
	}
	if got := string(inner.sent[0]); got != "12345" {
		t.Fatalf("inner payload = %q, want 12345", got)
	}
}

func TestTrafficWrapperAppliesMinimumDelay(t *testing.T) {
	inner := &trafficStubTransport{}
	tr := WithTraffic(inner, TrafficConfig{MinDelay: 2 * time.Millisecond})
	start := time.Now()
	if err := tr.Send([]byte("x")); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 2*time.Millisecond {
		t.Fatalf("Send() elapsed = %v, want at least 2ms", elapsed)
	}
}

// trafficStubControlHealthTransport additionally implements
// ControlHealthObserver, to verify trafficTransport forwards to it.
type trafficStubControlHealthTransport struct {
	trafficStubTransport
	unhealthy []bool
}

func (s *trafficStubControlHealthTransport) NotifyControlHealth(unhealthy bool) {
	s.unhealthy = append(s.unhealthy, unhealthy)
}

func TestTrafficWrapperForwardsControlHealth(t *testing.T) {
	inner := &trafficStubControlHealthTransport{}
	tr := WithTraffic(inner, TrafficConfig{MinDelay: time.Millisecond})
	tt, ok := tr.(ControlHealthObserver)
	if !ok {
		t.Fatal("wrapped transport does not implement ControlHealthObserver")
	}
	tt.NotifyControlHealth(true)
	tt.NotifyControlHealth(false)
	if got := inner.unhealthy; len(got) != 2 || got[0] != true || got[1] != false {
		t.Fatalf("inner.unhealthy = %v, want [true false]", got)
	}
}

func TestTrafficWrapperNotifyControlHealthNoopWhenUnsupported(t *testing.T) {
	inner := &trafficStubTransport{}
	tr := WithTraffic(inner, TrafficConfig{MinDelay: time.Millisecond})
	tt, ok := tr.(ControlHealthObserver)
	if !ok {
		t.Fatal("wrapped transport does not implement ControlHealthObserver")
	}
	tt.NotifyControlHealth(true) // must not panic when inner lacks the method
}
