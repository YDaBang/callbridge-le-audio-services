package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

const maximumHTTPBody = 64 << 10

func (s *Service) newHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
	}
}

func (s *Service) internalHandler() http.Handler {
	return s.bounded(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/health":
			snapshot := s.snapshot()
			s.writeJSON(writer, http.StatusOK, snapshot)
		case request.Method == http.MethodGet && request.URL.Path == "/ready":
			snapshot := s.snapshot()
			s.writeJSON(writer, readinessCode(snapshot), snapshot)
		case request.Method == http.MethodGet && request.URL.Path == "/messages":
			s.handleList(writer, request, false)
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/messages/"):
			s.handleGet(writer, request)
		case request.Method == http.MethodGet && request.URL.Path == "/poll":
			s.handleList(writer, request, true)
		case request.Method == http.MethodPost && request.URL.Path == "/send/sms":
			s.handleSend(writer, request)
		case request.Method == http.MethodPost && request.URL.Path == "/send/raw":
			s.handleRaw(writer, request)
		case request.Method == http.MethodPost && request.URL.Path == "/send/mms":
			s.writeJSON(writer, http.StatusNotImplemented, map[string]any{"ok": false, "error": "MMS sending is not enabled"})
		default:
			s.writeJSON(writer, http.StatusNotFound, map[string]any{"ok": false, "error": "not found"})
		}
	}))
}

func (s *Service) outboxHandler() http.Handler {
	return s.bounded(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !s.sourceAllowed(request.RemoteAddr) {
			s.writeJSON(writer, http.StatusForbidden, map[string]any{"ok": false, "error": "source not allowed"})
			return
		}
		if request.Method == http.MethodGet && request.URL.Path == "/health" {
			snapshot := s.snapshot()
			s.writeJSON(writer, http.StatusOK, snapshot)
			return
		}
		if request.Method == http.MethodGet && request.URL.Path == "/ready" {
			snapshot := s.snapshot()
			s.writeJSON(writer, readinessCode(snapshot), snapshot)
			return
		}
		if request.Method != http.MethodPost || request.URL.Path != "/send/sms" {
			s.writeJSON(writer, http.StatusNotFound, map[string]any{"ok": false, "error": "not found"})
			return
		}
		s.handleSend(writer, request)
	}))
}

func (s *Service) bounded(next http.Handler) http.Handler {
	semaphore := make(chan struct{}, 16)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case semaphore <- struct{}{}:
			defer func() { <-semaphore }()
			next.ServeHTTP(writer, request)
		default:
			s.writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "busy"})
		}
	})
}

func (s *Service) handleSend(writer http.ResponseWriter, request *http.Request) {
	var payload struct {
		To   string `json:"to"`
		Text string `json:"text"`
	}
	if err := decodeJSON(request, &payload); err != nil {
		s.writeJSON(writer, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid request"})
		return
	}
	if err := s.enqueue(payload.To, payload.Text); err != nil {
		s.writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "queue rejected"})
		return
	}
	s.writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "queued": true})
}

func (s *Service) handleList(writer http.ResponseWriter, request *http.Request, onlyNew bool) {
	maximum, err := queryInt(request, "max", 20, 1, 500)
	if err != nil {
		s.writeJSON(writer, 400, map[string]any{"ok": false, "error": "invalid max"})
		return
	}
	offset, err := queryInt(request, "offset", 0, 0, 65535)
	if err != nil {
		s.writeJSON(writer, 400, map[string]any{"ok": false, "error": "invalid offset"})
		return
	}
	folder := request.URL.Query().Get("folder")
	if folder == "" {
		folder = "telecom/msg/inbox"
	}
	ctx, cancel := context.WithTimeout(request.Context(), 12*time.Second)
	defer cancel()
	response := s.submit(mapCommand{context: ctx, kind: commandList, folder: folder, maximum: maximum, offset: offset})
	if response.err != nil {
		s.writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "MAP unavailable"})
		return
	}
	items := response.listing
	if onlyNew {
		filtered := items[:0]
		for _, item := range items {
			if !s.store.HasSeen(item.Handle) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	s.writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "count": len(items), "messages": items})
}

func (s *Service) handleGet(writer http.ResponseWriter, request *http.Request) {
	handle := strings.TrimPrefix(request.URL.Path, "/messages/")
	folder := request.URL.Query().Get("folder")
	if folder == "" {
		folder = "telecom/msg/inbox"
	}
	ctx, cancel := context.WithTimeout(request.Context(), 12*time.Second)
	defer cancel()
	response := s.submit(mapCommand{context: ctx, kind: commandGet, folder: folder, handle: handle})
	if response.err != nil {
		s.writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "MAP unavailable"})
		return
	}
	s.writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "handle": handle, "message": response.message})
}

func (s *Service) handleRaw(writer http.ResponseWriter, request *http.Request) {
	var payload struct {
		BMessage    string `json:"bmessage"`
		Folder      string `json:"folder"`
		Transparent bool   `json:"transparent"`
		Retry       bool   `json:"retry"`
	}
	if err := decodeJSON(request, &payload); err != nil || payload.BMessage == "" {
		s.writeJSON(writer, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid request"})
		return
	}
	if payload.Folder == "" {
		payload.Folder = "telecom/msg/outbox"
	}
	ctx, cancel := context.WithTimeout(request.Context(), 12*time.Second)
	defer cancel()
	response := s.submit(mapCommand{context: ctx, kind: commandRaw, folder: payload.Folder, body: payload.BMessage, transparent: payload.Transparent, retry: payload.Retry})
	if response.err != nil {
		s.writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "MAP send failed"})
		return
	}
	s.writeJSON(writer, http.StatusOK, map[string]any{"ok": true})
}

func (s *Service) sourceAllowed(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return false
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.Is4() {
		return false
	}
	for _, prefix := range s.cfg.AllowedSources {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func (s *Service) writeJSON(writer http.ResponseWriter, status int, value any) {
	// Legacy container supervision looks for the exact fragment `"ok": true`.
	// Indented JSON preserves that contract while remaining standards-compliant.
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		http.Error(writer, "internal error", 500)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_, _ = writer.Write(raw)
}

func decodeJSON(request *http.Request, destination any) error {
	if request.Body == nil {
		return errors.New("empty body")
	}
	defer request.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(request.Body, maximumHTTPBody+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func queryInt(request *http.Request, name string, fallback, minimum, maximum int) (int, error) {
	raw := request.URL.Query().Get(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, errors.New("query outside bounds")
	}
	return value, nil
}

func readinessCode(snapshot healthSnapshot) int {
	if snapshot.Ready {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}
