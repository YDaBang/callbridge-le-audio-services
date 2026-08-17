package bridge

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"callbridge.local/callbridge-sms-go/internal/config"
	"callbridge.local/callbridge-sms-go/internal/media"
	"callbridge.local/callbridge-sms-go/internal/message"
	"callbridge.local/callbridge-sms-go/internal/model"
	"callbridge.local/callbridge-sms-go/internal/state"
)

type fakeAMI struct {
	calls        int
	body         string
	contentTypes []string
	err          error
}

func (a *fakeAMI) SendMessage(_ context.Context, _, _, body, contentType, _ string) error {
	a.calls++
	a.body = body
	a.contentTypes = append(a.contentTypes, contentType)
	return a.err
}

type fakeMedia struct {
	calls      int
	attachment model.Attachment
	err        error
}

func (m *fakeMedia) Upload(_ context.Context, attachment model.Attachment) (media.Descriptor, error) {
	m.calls++
	m.attachment = attachment
	if m.err != nil {
		return media.Descriptor{}, m.err
	}
	return media.Descriptor{
		ContentType: attachment.ContentType, ContentURL: "https://mmmsg.acrobits.net/ABCDEFGHIJKLMNOP",
		ContentSize: len(attachment.Data), Filename: attachment.Filename,
		EncryptionKey: "00112233445566778899AABBCCDDEEFF", Hash: "12345",
	}, nil
}

type fakeMAP struct {
	raw       []byte
	getErr    error
	sendErrs  []error
	sentTexts []string
}

func (m *fakeMAP) RegisterNotifications(bool) error { return nil }
func (m *fakeMAP) Keepalive() error                 { return nil }
func (m *fakeMAP) ListMessages(string, int, int) ([]model.ListingMessage, error) {
	return nil, nil
}
func (m *fakeMAP) GetMessage(string, string) ([]byte, error) { return m.raw, m.getErr }
func (m *fakeMAP) SendSMS(_, text string) error {
	m.sentTexts = append(m.sentTexts, text)
	if len(m.sendErrs) == 0 {
		return nil
	}
	err := m.sendErrs[0]
	m.sendErrs = m.sendErrs[1:]
	return err
}
func (m *fakeMAP) PushBMessage(string, string, bool, bool) error { return nil }
func (m *fakeMAP) Close() error                                  { return nil }

func newTestService(t *testing.T, sender amiSender, logBuffer *bytes.Buffer) *Service {
	t.Helper()
	store, _, err := state.Open(filepath.Join(t.TempDir(), "state.json"), state.LegacyPaths{})
	if err != nil {
		t.Fatal(err)
	}
	return &Service{
		cfg: config.Config{Recipients: []string{"1002"}, MediaMaxBytes: 1024, MediaTimeout: time.Second}, logger: log.New(logBuffer, "", 0),
		store: store, ami: sender, media: &fakeMedia{}, status: newStatus(), events: make(chan model.Event, 4),
		commands: make(chan mapCommand, 4), outboundWake: make(chan struct{}, 1), fetchDelays: []time.Duration{0}, mediaRetries: make(map[string]mediaRetry),
	}
}

func TestInboundIsConsumedBeforeFailedAMIDeliveryAndLogsAreRedacted(t *testing.T) {
	var logs bytes.Buffer
	sender := &fakeAMI{err: errors.New("rejected")}
	service := newTestService(t, sender, &logs)
	bridge := &fakeMAP{raw: []byte("BEGIN:VCARD\r\nTEL:01000000000\r\nEND:VCARD\r\nBEGIN:MSG\r\nPRIVATE-BODY\r\nEND:MSG\r\n")}
	event := model.Event{EventType: "NewMessage", Handle: "ABCD", Folder: "telecom/msg/inbox", MessageType: "SMS_GSM", DateTime: "20260723T102719"}
	if err := service.processEvent(context.Background(), bridge, event); err != nil {
		t.Fatal(err)
	}
	if !service.store.HasSeen("ABCD") || sender.calls != 1 {
		t.Fatalf("seen=%t calls=%d", service.store.HasSeen("ABCD"), sender.calls)
	}
	if err := service.processEvent(context.Background(), bridge, event); err != nil || sender.calls != 1 {
		t.Fatalf("duplicate event redelivered calls=%d err=%v", sender.calls, err)
	}
	if strings.Contains(logs.String(), "PRIVATE-BODY") || strings.Contains(logs.String(), "01000000000") {
		t.Fatalf("private content leaked to logs: %q", logs.String())
	}
}

func TestOversizedInboundIsConsumedWithoutFloodingAMI(t *testing.T) {
	var logs bytes.Buffer
	sender := &fakeAMI{}
	service := newTestService(t, sender, &logs)
	body := strings.Repeat("한", message.MaxSIPPartBytes*message.MaxSIPParts)
	bridge := &fakeMAP{raw: []byte("BEGIN:VCARD\r\nTEL:01000000000\r\nEND:VCARD\r\nBEGIN:MSG\r\n" + body + "\r\nEND:MSG\r\n")}
	event := model.Event{EventType: "NewMessage", Handle: "CDEF", Folder: "telecom/msg/inbox", MessageType: "MMS"}
	if err := service.processEvent(context.Background(), bridge, event); err != nil {
		t.Fatal(err)
	}
	if !service.store.HasSeen("CDEF") || sender.calls != 0 || !strings.Contains(logs.String(), "body=oversize") {
		t.Fatalf("seen=%t calls=%d logs=%q", service.store.HasSeen("CDEF"), sender.calls, logs.String())
	}
}

func TestMMSImageUsesExistingGroundwireMediaMessagePath(t *testing.T) {
	var logs bytes.Buffer
	sender := &fakeAMI{}
	uploader := &fakeMedia{}
	service := newTestService(t, sender, &logs)
	service.media = uploader
	raw := []byte("BEGIN:VCARD\r\nTEL:01000000000\r\nEND:VCARD\r\n" +
		"BEGIN:MSG\r\nMIME-Version: 1.0\r\nContent-Type: multipart/related; boundary=map-boundary\r\n\r\n" +
		"--map-boundary\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n사진입니다\r\n" +
		"--map-boundary\r\nContent-Type: image/jpeg\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=photo.jpg\r\n\r\n/9j/2Q==\r\n" +
		"--map-boundary--\r\nEND:MSG\r\n")
	bridge := &fakeMAP{raw: raw}
	event := model.Event{EventType: "NewMessage", Handle: "AA11", Folder: "telecom/msg/inbox", MessageType: "MMS"}
	if err := service.processEvent(context.Background(), bridge, event); err != nil {
		t.Fatal(err)
	}
	if !service.store.HasSeen("AA11") || uploader.calls != 1 || sender.calls != 1 {
		t.Fatalf("seen=%t uploads=%d sends=%d", service.store.HasSeen("AA11"), uploader.calls, sender.calls)
	}
	if uploader.attachment.ContentType != "image/jpeg" || string(uploader.attachment.Data) != string([]byte{0xff, 0xd8, 0xff, 0xd9}) {
		t.Fatalf("bad uploaded attachment: %#v", uploader.attachment)
	}
	if len(sender.contentTypes) != 1 || sender.contentTypes[0] != media.SignalContentType || !strings.Contains(sender.body, `"body":"사진입니다"`) || strings.Contains(sender.body, "/9j/2Q==") {
		t.Fatalf("content-types=%v body=%q", sender.contentTypes, sender.body)
	}
}

func TestMMSUploadFailureDefersWithoutConsuming(t *testing.T) {
	var logs bytes.Buffer
	uploader := &fakeMedia{err: errors.New("offline")}
	service := newTestService(t, &fakeAMI{}, &logs)
	service.media = uploader
	raw := []byte("BEGIN:MSG\r\nMIME-Version: 1.0\r\nContent-Type: image/jpeg\r\nContent-Transfer-Encoding: base64\r\n\r\n/9j/2Q==\r\nEND:MSG\r\n")
	bridge := &fakeMAP{raw: raw}
	event := model.Event{EventType: "NewMessage", Handle: "AA22", Folder: "telecom/msg/inbox", MessageType: "MMS"}
	if err := service.processEvent(context.Background(), bridge, event); err != nil {
		t.Fatal(err)
	}
	if service.store.HasSeen("AA22") || uploader.calls != 1 || !strings.Contains(logs.String(), "inbound deferred") {
		t.Fatalf("seen=%t uploads=%d logs=%q", service.store.HasSeen("AA22"), uploader.calls, logs.String())
	}
	if err := service.processEvent(context.Background(), bridge, event); err != nil || uploader.calls != 1 {
		t.Fatalf("cooldown uploads=%d err=%v", uploader.calls, err)
	}
}

func TestOutboundFailureConsumesOnlyAttemptedItem(t *testing.T) {
	service := newTestService(t, &fakeAMI{}, &bytes.Buffer{})
	for index, text := range []string{"first", "second"} {
		if err := service.store.AddOutbound(model.Outbound{ID: string(rune('A' + index)), To: "+821000000000", Text: text, QueuedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	bridge := &fakeMAP{sendErrs: []error{errors.New("ambiguous failure")}}
	if err := service.drainOutbound(bridge); err == nil {
		t.Fatal("expected first attempt to fail")
	}
	_, depth := service.store.Counts()
	if depth != 1 || len(bridge.sentTexts) != 1 || bridge.sentTexts[0] != "first" {
		t.Fatalf("after failure depth=%d sent=%v", depth, bridge.sentTexts)
	}
	if err := service.drainOutbound(bridge); err != nil {
		t.Fatal(err)
	}
	_, depth = service.store.Counts()
	if depth != 0 || len(bridge.sentTexts) != 2 || bridge.sentTexts[1] != "second" {
		t.Fatalf("after recovery depth=%d sent=%v", depth, bridge.sentTexts)
	}
}

func TestActionIDCompatibility(t *testing.T) {
	if got := deliveryActionID("ABCD", "1002", 0); got != "smsv3-85642c58f61b7ae4094726c2765c6f43bb6e58b2" {
		t.Fatalf("deliveryActionID()=%q", got)
	}
}

func TestOutboxSourceAllowlist(t *testing.T) {
	service := newTestService(t, &fakeAMI{}, &bytes.Buffer{})
	service.cfg.AllowedSources = []netip.Prefix{netip.MustParsePrefix("10.0.0.1/32")}
	handler := service.outboxHandler()
	request := httptest.NewRequest(http.MethodPost, "http://room/send/sms", strings.NewReader(`{"to":"01000000000","text":"hello"}`))
	request.RemoteAddr = "10.0.0.2:50000"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("disallowed source status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "http://room/send/sms", strings.NewReader(`{"to":"01000000000","text":"hello"}`))
	request.RemoteAddr = "10.0.0.1:50000"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"queued": true`) {
		t.Fatalf("allowed source status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestLegacyHealthIsLiveWhileReadinessIsDegraded(t *testing.T) {
	service := newTestService(t, &fakeAMI{}, &bytes.Buffer{})
	service.cfg.AllowedSources = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	handler := service.outboxHandler()
	request := httptest.NewRequest(http.MethodGet, "http://room/health", nil)
	request.RemoteAddr = "127.0.0.1:50000"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"ok": true`) || !strings.Contains(response.Body.String(), `"ready": false`) {
		t.Fatalf("legacy health status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "http://room/ready", nil)
	request.RemoteAddr = "127.0.0.1:50000"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("degraded readiness status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"le_audio_mode": ""`) ||
		!strings.Contains(response.Body.String(), `"le_audio_ready": false`) ||
		!strings.Contains(response.Body.String(), `"le_audio_extended_advertising": false`) ||
		!strings.Contains(response.Body.String(), `"le_audio_bap_announcement": false`) {
		t.Fatalf("separate LE Audio status missing: %s", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"classic_profile_mode": "normal"`) ||
		!strings.Contains(response.Body.String(), `"classic_profiles_enabled": true`) {
		t.Fatalf("Classic profile status missing: %s", response.Body.String())
	}
}

func TestClassicProfileIsolationIsExplicitAndNotReady(t *testing.T) {
	service := newTestService(t, &fakeAMI{}, &bytes.Buffer{})
	service.cfg.ClassicProfileMode = config.ClassicProfileIsolated
	service.cfg.LEAudioMode = "le-canary"
	service.cfg.AllowedSources = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	if service.classicProfilesEnabled() {
		t.Fatal("Classic profiles remained enabled in isolated mode")
	}

	request := httptest.NewRequest(http.MethodGet, "http://room/health", nil)
	request.RemoteAddr = "127.0.0.1:50000"
	response := httptest.NewRecorder()
	service.outboxHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"classic_profile_mode": "isolated"`) ||
		!strings.Contains(response.Body.String(), `"classic_profiles_enabled": false`) ||
		!strings.Contains(response.Body.String(), `"ready": false`) {
		t.Fatalf("isolated health status=%d body=%s", response.Code, response.Body.String())
	}
}
