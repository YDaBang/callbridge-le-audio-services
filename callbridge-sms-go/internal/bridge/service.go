package bridge

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"callbridge.local/callbridge-sms-go/internal/ami"
	"callbridge.local/callbridge-sms-go/internal/bluez"
	"callbridge.local/callbridge-sms-go/internal/config"
	"callbridge.local/callbridge-sms-go/internal/mapclient"
	"callbridge.local/callbridge-sms-go/internal/media"
	"callbridge.local/callbridge-sms-go/internal/message"
	"callbridge.local/callbridge-sms-go/internal/mns"
	"callbridge.local/callbridge-sms-go/internal/model"
	"callbridge.local/callbridge-sms-go/internal/state"
)

type mapSession interface {
	RegisterNotifications(bool) error
	Keepalive() error
	ListMessages(string, int, int) ([]model.ListingMessage, error)
	GetMessage(string, string) ([]byte, error)
	SendSMS(string, string) error
	PushBMessage(string, string, bool, bool) error
	Close() error
}

type amiSender interface {
	SendMessage(context.Context, string, string, string, string, string) error
}

type mediaUploader interface {
	Upload(context.Context, model.Attachment) (media.Descriptor, error)
}

type commandKind uint8

const (
	commandList commandKind = iota + 1
	commandGet
	commandRaw
)

type mapCommand struct {
	context     context.Context
	kind        commandKind
	folder      string
	handle      string
	maximum     int
	offset      int
	body        string
	transparent bool
	retry       bool
	response    chan commandResponse
}

type commandResponse struct {
	listing []model.ListingMessage
	message model.Message
	err     error
}

type Service struct {
	cfg          config.Config
	logger       *log.Logger
	store        *state.Store
	ami          amiSender
	media        mediaUploader
	status       *status
	events       chan model.Event
	commands     chan mapCommand
	outboundWake chan struct{}
	connect      func(string, uint8, time.Duration) (mapSession, error)
	mnsReady     chan struct{}
	mnsReadyOnce sync.Once
	fetchDelays  []time.Duration
	mediaRetries map[string]mediaRetry
	leAudio      *bluez.Broker
}

type mediaRetry struct {
	attempts int
	next     time.Time
}

type sipDelivery struct {
	body        string
	contentType string
	mediaIndex  int
}

func New(cfg config.Config, logger *log.Logger) (*Service, error) {
	store, imported, err := state.Open(cfg.StateFile, state.LegacyPaths{
		SeenFile: cfg.LegacySeenFile, MessageLog: cfg.LegacyMessageLog,
		ForwardState: cfg.LegacyForwardState, OutboundQueue: cfg.LegacyOutboundQueue,
	})
	if err != nil {
		return nil, fmt.Errorf("open SMS state: %w", err)
	}
	client, err := ami.New(cfg.AMIAddress, cfg.AMIUser, cfg.AMISecretFile)
	if err != nil {
		return nil, err
	}
	mediaClient, err := media.New(cfg.MediaURL, cfg.MediaMaxBytes, cfg.MediaTimeout)
	if err != nil {
		return nil, err
	}
	service := &Service{
		cfg: cfg, logger: logger, store: store, ami: client, media: mediaClient, status: newStatus(),
		events: make(chan model.Event, 256), commands: make(chan mapCommand, 32),
		outboundWake: make(chan struct{}, 1),
		mnsReady:     make(chan struct{}),
		fetchDelays:  []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 5 * time.Second},
		mediaRetries: make(map[string]mediaRetry),
		connect: func(device string, channel uint8, timeout time.Duration) (mapSession, error) {
			return mapclient.Connect(device, channel, timeout)
		},
	}
	seen, outbound := store.Counts()
	logger.Printf("state ready imported=%t initialized=%t seen=%d outbound=%d", imported, store.IsInitialized(), seen, outbound)
	return service, nil
}

func (s *Service) Run(ctx context.Context) error {
	ctx, stop := context.WithCancel(ctx)
	var leAudioDone <-chan error
	defer func() {
		stop()
		if leAudioDone == nil {
			return
		}
		select {
		case runErr := <-leAudioDone:
			if runErr != nil {
				s.logger.Printf("le audio broker shutdown reason=%T", runErr)
			}
		case <-time.After(3 * time.Second):
			s.logger.Printf("le audio broker shutdown wait timed out")
		}
	}()
	if s.cfg.LEAudioMode == "le-canary" {
		broker, brokerErr := bluez.NewBroker(
			s.cfg.Adapter, s.cfg.BluetoothDevice, s.cfg.LEAudioSocket, s.cfg.LEAudioPeerUID, s.logger,
		)
		if brokerErr != nil {
			s.logger.Printf("le audio broker disabled reason=%T", brokerErr)
		} else {
			s.leAudio = broker
			done := make(chan error, 1)
			leAudioDone = done
			go func() {
				runErr := broker.Run(ctx)
				if runErr != nil && ctx.Err() == nil {
					s.logger.Printf("le audio broker stopped reason=%T", runErr)
				}
				done <- runErr
			}()
		}
	}
	internalListener, err := net.Listen("tcp", s.cfg.InternalListen)
	if err != nil {
		return fmt.Errorf("listen internal MAP API: %w", err)
	}
	defer internalListener.Close()
	outboxListener, err := net.Listen("tcp", s.cfg.OutboxListen)
	if err != nil {
		return fmt.Errorf("listen outbox API: %w", err)
	}
	defer outboxListener.Close()
	internalServer := s.newHTTPServer(s.internalHandler())
	outboxServer := s.newHTTPServer(s.outboxHandler())
	errCh := make(chan error, 2)
	go func() {
		if serveErr := internalServer.Serve(internalListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()
	go func() {
		if serveErr := outboxServer.Serve(outboxListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()
	if s.classicProfilesEnabled() {
		mnsServer, err := mns.New(s.cfg.BluetoothDevice, s.cfg.Adapter, s.cfg.MNSChannel, s.events, s.logger, mns.Callbacks{
			Registered: func(value bool) {
				s.status.setMNSRegistered(value)
				if value {
					s.mnsReadyOnce.Do(func() { close(s.mnsReady) })
				}
			},
			Connected: s.status.setMNSConnected,
			Dropped:   s.status.dropped,
		})
		if err != nil {
			return err
		}
		go mnsServer.Run(ctx)
		go func() {
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return
			case <-s.mnsReady:
			case <-timer.C:
				s.logger.Printf("mns startup delayed; MAP polling fallback enabled")
			}
			s.mapLoop(ctx)
		}()
	} else {
		s.logger.Printf("classic Bluetooth profiles intentionally isolated")
	}
	s.logger.Printf("service started internal=%s outbox=%s classic_profile_mode=%s", s.cfg.InternalListen, s.cfg.OutboxListen, s.classicProfileMode())
	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = internalServer.Shutdown(shutdown)
	_ = outboxServer.Shutdown(shutdown)
	return nil
}

func (s *Service) classicProfileMode() string {
	if s.cfg.ClassicProfileMode == config.ClassicProfileIsolated {
		return config.ClassicProfileIsolated
	}
	return config.ClassicProfileNormal
}

func (s *Service) classicProfilesEnabled() bool {
	return s.classicProfileMode() == config.ClassicProfileNormal
}

func (s *Service) mapLoop(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		session, err := s.connect(s.cfg.BluetoothDevice, s.cfg.MASChannel, s.cfg.MAPTimeout)
		if err != nil {
			s.logger.Printf("map connect unavailable reason=%s", reason(err))
			s.waitReconnect(ctx, backoff)
			backoff = nextBackoff(backoff)
			continue
		}
		s.status.setMAP(true)
		if err := session.RegisterNotifications(true); err != nil {
			s.status.setMAP(false)
			_ = session.Close()
			s.logger.Printf("map notification registration failed reason=%s", reason(err))
			s.waitReconnect(ctx, backoff)
			backoff = nextBackoff(backoff)
			continue
		}
		s.status.setNotifications(true)
		s.logger.Printf("map session ready channel=%d", s.cfg.MASChannel)
		backoff = time.Second
		if err := s.bootstrap(session); err == nil {
			err = s.runConnected(ctx, session)
		} else {
			s.logger.Printf("map bootstrap failed reason=%s", reason(err))
		}
		s.status.setMAP(false)
		_ = session.Close()
		if err != nil && ctx.Err() == nil {
			s.logger.Printf("map session reconnecting reason=%s", reason(err))
		}
		s.waitReconnect(ctx, backoff)
		backoff = nextBackoff(backoff)
	}
}

func (s *Service) bootstrap(session mapSession) error {
	if s.store.IsInitialized() {
		return nil
	}
	messages, err := session.ListMessages("telecom/msg/inbox", s.cfg.PollMaxCount, 0)
	if err != nil {
		return err
	}
	handles := make([]string, 0, len(messages))
	for _, item := range messages {
		handles = append(handles, item.Handle)
	}
	if err := s.store.MarkSeenMany(handles); err != nil {
		return err
	}
	if err := s.store.SetInitialized(); err != nil {
		return err
	}
	s.logger.Printf("map initial baseline created count=%d", len(handles))
	return nil
}

func (s *Service) runConnected(ctx context.Context, session mapSession) error {
	poll := time.NewTicker(s.cfg.PollInterval)
	keepalive := time.NewTicker(s.cfg.Keepalive)
	defer poll.Stop()
	defer keepalive.Stop()
	if err := s.drainOutbound(session); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-s.events:
			if err := s.processEvent(ctx, session, event); err != nil {
				return err
			}
		case <-s.outboundWake:
			if err := s.drainOutbound(session); err != nil {
				return err
			}
		case command := <-s.commands:
			if err := s.executeCommand(session, command); err != nil {
				return err
			}
		case <-poll.C:
			if err := s.poll(ctx, session); err != nil {
				if mapclient.IsTransportError(err) {
					return err
				}
				s.logger.Printf("map poll rejected reason=%s", reason(err))
			}
		case <-keepalive.C:
			if err := session.Keepalive(); err != nil {
				return err
			}
		}
	}
}

func (s *Service) poll(ctx context.Context, session mapSession) error {
	items, err := session.ListMessages("telecom/msg/inbox", s.cfg.PollMaxCount, 0)
	if err != nil {
		return err
	}
	s.status.polled()
	for index := len(items) - 1; index >= 0; index-- {
		item := items[index]
		if s.store.HasSeen(item.Handle) || !supportedType(item.Type) {
			continue
		}
		event := model.Event{
			EventType: "NewMessage", Handle: item.Handle, Folder: "telecom/msg/inbox",
			MessageType: defaultType(item.Type), DateTime: item.DateTime, Source: "poll",
		}
		if err := s.processEvent(ctx, session, event); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) processEvent(ctx context.Context, session mapSession, event model.Event) error {
	if event.EventType != "NewMessage" || event.Handle == "" || s.store.HasSeen(event.Handle) {
		return nil
	}
	if retry, ok := s.mediaRetries[event.Handle]; ok && time.Now().Before(retry.next) {
		return nil
	}
	folder := event.Folder
	if folder == "" || !strings.HasPrefix(folder, "telecom/msg/") {
		folder = "telecom/msg/inbox"
	}
	var raw []byte
	var lastErr error
	for _, delay := range s.fetchDelays {
		if !sleepContext(ctx, delay) {
			return ctx.Err()
		}
		raw, lastErr = session.GetMessage(folder, event.Handle)
		if lastErr == nil {
			break
		}
	}
	if lastErr != nil {
		if err := s.store.MarkSeen(event.Handle); err != nil {
			return err
		}
		s.status.inbound(false)
		s.logger.Printf("inbound consumed ref=%s fetch=failed", reference(event.Handle))
		if mapclient.IsTransportError(lastErr) {
			return lastErr
		}
		return nil
	}
	parsed := message.ParseBMessage(raw)
	if strings.EqualFold(event.MessageType, "MMS") {
		parsed, lastErr = message.ParseMMS(parsed, s.cfg.MediaMaxBytes)
		if lastErr != nil {
			if err := s.store.MarkSeen(event.Handle); err != nil {
				return err
			}
			s.status.inbound(false)
			s.logger.Printf("inbound consumed ref=%s mms=parse-failed", reference(event.Handle))
			return nil
		}
	}
	if parsed.Body == "" && len(parsed.Attachments) == 0 {
		if err := s.store.MarkSeen(event.Handle); err != nil {
			return err
		}
		s.status.inbound(false)
		s.logger.Printf("inbound consumed ref=%s body=empty", reference(event.Handle))
		return nil
	}
	clean := parsed.Body
	if !strings.EqualFold(event.MessageType, "MMS") {
		clean = message.StripMMSEnvelope(parsed.Body, event.MessageType)
	}
	var parts []string
	if clean != "" {
		parts = message.VisibleParts(clean, event.DateTime)
	}
	if len(parts) > message.MaxSIPParts {
		if err := s.store.MarkSeen(event.Handle); err != nil {
			return err
		}
		s.status.inbound(false)
		s.logger.Printf("inbound consumed ref=%s body=oversize", reference(event.Handle))
		return nil
	}

	descriptors := make([]media.Descriptor, 0, len(parsed.Attachments))
	for _, attachment := range parsed.Attachments {
		uploadCtx, cancel := context.WithTimeout(ctx, s.cfg.MediaTimeout)
		descriptor, uploadErr := s.media.Upload(uploadCtx, attachment)
		cancel()
		if uploadErr != nil {
			delay := s.deferMedia(event.Handle)
			s.logger.Printf("inbound deferred ref=%s media=upload-failed retry_seconds=%d", reference(event.Handle), int(delay.Seconds()))
			return nil
		}
		descriptors = append(descriptors, descriptor)
	}
	delete(s.mediaRetries, event.Handle)

	deliveries := make([]sipDelivery, 0, len(parts)+len(descriptors))
	captionIncluded := false
	for index, descriptor := range descriptors {
		caption := ""
		if index == 0 {
			caption = clean
		}
		signal, signalErr := media.MarshalSignal(caption, descriptor)
		if signalErr != nil || len(signal) > ami.MaxBodyBytes {
			signal, signalErr = media.MarshalSignal("", descriptor)
			caption = ""
		}
		if signalErr != nil || len(signal) > ami.MaxBodyBytes {
			if err := s.store.MarkSeen(event.Handle); err != nil {
				return err
			}
			s.status.inbound(false)
			s.logger.Printf("inbound consumed ref=%s media=signal-oversize", reference(event.Handle))
			return nil
		}
		if caption != "" {
			captionIncluded = true
		}
		deliveries = append(deliveries, sipDelivery{body: string(signal), contentType: media.SignalContentType, mediaIndex: index})
	}
	if !captionIncluded {
		for _, part := range parts {
			deliveries = append(deliveries, sipDelivery{body: part, contentType: "text/plain", mediaIndex: -1})
		}
	}
	if len(deliveries) == 0 {
		if err := s.store.MarkSeen(event.Handle); err != nil {
			return err
		}
		s.status.inbound(false)
		s.logger.Printf("inbound consumed ref=%s delivery=empty", reference(event.Handle))
		return nil
	}
	if err := s.store.MarkSeen(event.Handle); err != nil {
		return err
	}
	sender := message.NormalizeSender(parsed.SenderNumber)
	delivered := true
	for _, recipient := range s.cfg.Recipients {
		textIndex := 0
		for _, delivery := range deliveries {
			actionID := ""
			if delivery.mediaIndex >= 0 {
				actionID = mediaDeliveryActionID(event.Handle, recipient, delivery.mediaIndex)
			} else {
				actionID = deliveryActionID(event.Handle, recipient, textIndex)
				textIndex++
			}
			attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := s.ami.SendMessage(attemptCtx, recipient, sender, delivery.body, delivery.contentType, actionID)
			cancel()
			if err != nil {
				delivered = false
				break
			}
		}
	}
	s.status.inbound(delivered)
	s.logger.Printf("inbound consumed ref=%s delivery=%t text_parts=%d media_parts=%d", reference(event.Handle), delivered, len(parts), len(descriptors))
	return nil
}

func (s *Service) deferMedia(handle string) time.Duration {
	retry := s.mediaRetries[handle]
	retry.attempts++
	delays := []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour}
	index := retry.attempts - 1
	if index >= len(delays) {
		index = len(delays) - 1
	}
	delay := delays[index]
	retry.next = time.Now().Add(delay)
	s.mediaRetries[handle] = retry
	return delay
}

func (s *Service) drainOutbound(session mapSession) error {
	for {
		item, ok, err := s.store.TakeOutbound()
		if err != nil || !ok {
			return err
		}
		err = session.SendSMS(item.To, item.Text)
		s.status.outbound(err == nil)
		s.logger.Printf("outbound consumed ref=%s delivery=%t", reference(item.ID), err == nil)
		if err != nil {
			return err
		}
	}
}

func (s *Service) executeCommand(session mapSession, command mapCommand) error {
	if command.context.Err() != nil {
		return nil
	}
	response := commandResponse{}
	switch command.kind {
	case commandList:
		response.listing, response.err = session.ListMessages(command.folder, command.maximum, command.offset)
	case commandGet:
		var raw []byte
		raw, response.err = session.GetMessage(command.folder, command.handle)
		if response.err == nil {
			response.message = message.ParseBMessage(raw)
		}
	case commandRaw:
		response.err = session.PushBMessage(command.folder, command.body, command.transparent, command.retry)
	default:
		response.err = errors.New("unknown MAP command")
	}
	select {
	case command.response <- response:
	default:
	}
	if mapclient.IsTransportError(response.err) {
		return response.err
	}
	return nil
}

func (s *Service) enqueue(to, text string) error {
	to, err := message.NormalizeDestination(to)
	if err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" || len([]byte(text)) > 16<<10 {
		return errors.New("outbound text length invalid")
	}
	identifier := make([]byte, 16)
	if _, err := rand.Read(identifier); err != nil {
		return errors.New("create outbound identifier")
	}
	item := model.Outbound{ID: hex.EncodeToString(identifier), To: to, Text: text, QueuedAt: time.Now().UTC()}
	if err := s.store.AddOutbound(item); err != nil {
		return err
	}
	select {
	case s.outboundWake <- struct{}{}:
	default:
	}
	return nil
}

func (s *Service) submit(command mapCommand) commandResponse {
	if !s.status.connected() {
		return commandResponse{err: errors.New("MAP session unavailable")}
	}
	command.response = make(chan commandResponse, 1)
	select {
	case s.commands <- command:
	case <-command.context.Done():
		return commandResponse{err: command.context.Err()}
	}
	select {
	case response := <-command.response:
		return response
	case <-command.context.Done():
		return commandResponse{err: command.context.Err()}
	}
}

func (s *Service) snapshot() healthSnapshot {
	seen, outbound := s.store.Counts()
	leAudio := leAudioHealth{Mode: s.cfg.LEAudioMode}
	if s.leAudio != nil {
		broker := s.leAudio.Snapshot()
		leAudio.EndpointsRegistered = broker.EndpointsRegistered
		leAudio.Advertising = broker.Advertising
		leAudio.Discoverable = broker.Discoverable
		leAudio.ExtendedAdvertising = broker.ExtendedAdvertising
		leAudio.BAPAnnouncement = broker.BAPAnnouncement
		leAudio.CAPAnnouncement = broker.CAPAnnouncement
		leAudio.TMAPAnnouncement = broker.TMAPAnnouncement
		leAudio.HeadsetAppearance = broker.HeadsetAppearance
		leAudio.SinkConfigured = broker.SinkConfigured
		leAudio.SourceConfigured = broker.SourceConfigured
		leAudio.SinkAcquired = broker.SinkAcquired
		leAudio.SourceAcquired = broker.SourceAcquired
		leAudio.BidirectionalCIS = broker.BidirectionalCIS
		leAudio.Ready = broker.EndpointsRegistered && broker.Advertising && broker.BidirectionalCIS
	}
	return s.status.snapshot(config.Version, s.classicProfileMode(), s.store.IsInitialized(), seen, outbound, leAudio)
}

func (s *Service) waitReconnect(ctx context.Context, delay time.Duration) {
	_ = sleepContext(ctx, delay)
}

func nextBackoff(value time.Duration) time.Duration {
	value *= 2
	if value > 30*time.Second {
		return 30 * time.Second
	}
	return value
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func supportedType(value string) bool {
	return value == "" || value == "SMS_GSM" || value == "SMS_CDMA" || value == "MMS"
}

func defaultType(value string) string {
	if value == "" {
		return "SMS_GSM"
	}
	return value
}
func reason(err error) string {
	if err == nil {
		return "none"
	}
	return fmt.Sprintf("%T", err)
}

func reference(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:6])
}

func deliveryActionID(handle, recipient string, part int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("chunk-v1\x00%s\x00%s\x00%d", handle, recipient, part)))
	return "smsv3-" + hex.EncodeToString(digest[:20])
}

func mediaDeliveryActionID(handle, recipient string, part int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("media-v1\x00%s\x00%s\x00%d", handle, recipient, part)))
	return "mmsv1-" + hex.EncodeToString(digest[:20])
}
