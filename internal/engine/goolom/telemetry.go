package goolom

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

func (s *Session) startTelemetry(ctx context.Context, serverHello map[string]any) {
	endpoint, interval, ok := parseTelemetryCfg(serverHello)
	if !ok {
		return
	}
	if !s.telemetryActive.CompareAndSwap(false, true) {
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.telemetryActive.Store(false)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		s.sendTelemetry(ctx, endpoint, "join")
		for {
			select {
			case <-ticker.C:
				s.sendTelemetry(ctx, endpoint, "stats")
			case <-s.telemetryCh:
				s.sendTelemetry(ctx, endpoint, "leave")
				return
			case <-s.closeCh:
				s.sendTelemetry(ctx, endpoint, "leave")
				return
			}
		}
	}()
}

func parseTelemetryCfg(serverHello map[string]any) (string, time.Duration, bool) {
	cfg, ok := serverHello["telemetryConfiguration"].(map[string]any)
	if !ok {
		return "", 0, false
	}
	endpoint, ok := cfg["logEndpoint"].(string)
	if !ok || endpoint == "" {
		endpoint, ok = cfg["endpoint"].(string)
		if !ok || endpoint == "" {
			endpoint, _ = cfg["url"].(string)
		}
	}
	if endpoint == "" {
		return "", 0, false
	}
	interval := defaultTelemetryInterval
	if raw, ok := cfg["sendingInterval"].(float64); ok && raw > 0 {
		interval = time.Duration(raw) * time.Millisecond
	}
	return endpoint, interval, true
}

func (s *Session) stopTelemetry() {
	if s.telemetryActive.Load() {
		select {
		case s.telemetryCh <- struct{}{}:
		default:
		}
	}
}

func (s *Session) sendTelemetry(ctx context.Context, endpoint, event string) {
	body, err := json.Marshal(map[string]any{
		"event":          event,
		"timestamp":      time.Now().UnixMilli(),
		"peerId":         s.peerID,
		"roomId":         s.roomID,
		"displayName":    s.name,
		"implementation": "browser",
		"dataChannel": map[string]any{
			"bufferedAmount": s.GetBufferedAmount(),
			"sendQueue":      len(s.sendQueue),
		},
	})
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		logger.Verbosef("Telemetry req error: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) Gecko/20100101 Firefox/149.0")
	if s.telemetryReferer != "" {
		req.Header.Set("Referer", s.telemetryReferer)
	}
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Client-Instance-Id", uuid.New().String())
	req.Header.Set("X-Telemost-Client-Version", "187.1.0")
	req.Header.Set("Idempotency-Key", uuid.New().String())

	client := protect.NewHTTPClient(s.resolver)
	resp, err := client.Do(req)
	if err != nil {
		logger.Verbosef("Telemetry send error: %v", err)
		return
	}
	_ = resp.Body.Close()
}
