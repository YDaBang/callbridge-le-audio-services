package bridge

import (
	"sync"
	"time"
)

type healthSnapshot struct {
	OK                         bool   `json:"ok"`
	Ready                      bool   `json:"ready"`
	Service                    string `json:"service"`
	Version                    string `json:"version"`
	ClassicProfileMode         string `json:"classic_profile_mode"`
	ClassicProfilesEnabled     bool   `json:"classic_profiles_enabled"`
	MNSRegistered              bool   `json:"mns_registered"`
	MNSConnected               bool   `json:"mns_connected"`
	MAPConnected               bool   `json:"map_connected"`
	NotificationsEnabled       bool   `json:"notifications_enabled"`
	Initialized                bool   `json:"initialized"`
	SeenHandles                int    `json:"seen_handles"`
	OutboundDepth              int    `json:"outbound_depth"`
	LastPollUnix               int64  `json:"last_poll_unix,omitempty"`
	InboundConsumed            uint64 `json:"inbound_consumed"`
	InboundFailed              uint64 `json:"inbound_failed"`
	OutboundConsumed           uint64 `json:"outbound_consumed"`
	OutboundFailed             uint64 `json:"outbound_failed"`
	DroppedMNSEvents           uint64 `json:"dropped_mns_events"`
	UptimeSeconds              int64  `json:"uptime_seconds"`
	LEAudioMode                string `json:"le_audio_mode"`
	LEAudioReady               bool   `json:"le_audio_ready"`
	LEAudioEndpointsRegistered bool   `json:"le_audio_endpoints_registered"`
	LEAudioAdvertising         bool   `json:"le_audio_advertising"`
	LEAudioDiscoverable        bool   `json:"le_audio_discoverable"`
	LEAudioExtendedAdvertising bool   `json:"le_audio_extended_advertising"`
	LEAudioBAPAnnouncement     bool   `json:"le_audio_bap_announcement"`
	LEAudioCAPAnnouncement     bool   `json:"le_audio_cap_announcement"`
	LEAudioTMAPAnnouncement    bool   `json:"le_audio_tmap_announcement"`
	LEAudioHeadsetAppearance   bool   `json:"le_audio_headset_appearance"`
	LEAudioSinkConfigured      bool   `json:"le_audio_sink_configured"`
	LEAudioSourceConfigured    bool   `json:"le_audio_source_configured"`
	LEAudioSinkAcquired        bool   `json:"le_audio_sink_acquired"`
	LEAudioSourceAcquired      bool   `json:"le_audio_source_acquired"`
	LEAudioBidirectionalCIS    bool   `json:"le_audio_bidirectional_cis"`
}

type leAudioHealth struct {
	Mode                string
	Ready               bool
	EndpointsRegistered bool
	Advertising         bool
	Discoverable        bool
	ExtendedAdvertising bool
	BAPAnnouncement     bool
	CAPAnnouncement     bool
	TMAPAnnouncement    bool
	HeadsetAppearance   bool
	SinkConfigured      bool
	SourceConfigured    bool
	SinkAcquired        bool
	SourceAcquired      bool
	BidirectionalCIS    bool
}

type status struct {
	mu               sync.Mutex
	started          time.Time
	registered       bool
	mnsConnected     bool
	mapConnected     bool
	notifications    bool
	lastPoll         time.Time
	inboundConsumed  uint64
	inboundFailed    uint64
	outboundConsumed uint64
	outboundFailed   uint64
	droppedMNS       uint64
}

func newStatus() *status { return &status{started: time.Now()} }

func (s *status) setMNSRegistered(value bool) { s.mu.Lock(); s.registered = value; s.mu.Unlock() }
func (s *status) setMNSConnected(value bool)  { s.mu.Lock(); s.mnsConnected = value; s.mu.Unlock() }
func (s *status) setMAP(value bool) {
	s.mu.Lock()
	s.mapConnected = value
	if !value {
		s.notifications = false
	}
	s.mu.Unlock()
}
func (s *status) setNotifications(value bool) { s.mu.Lock(); s.notifications = value; s.mu.Unlock() }
func (s *status) polled()                     { s.mu.Lock(); s.lastPoll = time.Now(); s.mu.Unlock() }
func (s *status) inbound(success bool) {
	s.mu.Lock()
	s.inboundConsumed++
	if !success {
		s.inboundFailed++
	}
	s.mu.Unlock()
}
func (s *status) outbound(success bool) {
	s.mu.Lock()
	s.outboundConsumed++
	if !success {
		s.outboundFailed++
	}
	s.mu.Unlock()
}
func (s *status) dropped() { s.mu.Lock(); s.droppedMNS++; s.mu.Unlock() }

func (s *status) connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mapConnected && s.notifications
}

func (s *status) snapshot(version, classicProfileMode string, initialized bool, seen, outbound int, leAudio leAudioHealth) healthSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := healthSnapshot{
		Service: "sms-bridge-go", Version: version,
		ClassicProfileMode: classicProfileMode, ClassicProfilesEnabled: classicProfileMode == "normal",
		MNSRegistered: s.registered, MNSConnected: s.mnsConnected,
		MAPConnected: s.mapConnected, NotificationsEnabled: s.notifications,
		Initialized: initialized, SeenHandles: seen, OutboundDepth: outbound,
		InboundConsumed: s.inboundConsumed, InboundFailed: s.inboundFailed,
		OutboundConsumed: s.outboundConsumed, OutboundFailed: s.outboundFailed,
		DroppedMNSEvents: s.droppedMNS,
		UptimeSeconds:    int64(time.Since(s.started).Seconds()),
		LEAudioMode:      leAudio.Mode, LEAudioReady: leAudio.Ready,
		LEAudioEndpointsRegistered: leAudio.EndpointsRegistered,
		LEAudioAdvertising:         leAudio.Advertising,
		LEAudioDiscoverable:        leAudio.Discoverable,
		LEAudioExtendedAdvertising: leAudio.ExtendedAdvertising,
		LEAudioBAPAnnouncement:     leAudio.BAPAnnouncement,
		LEAudioCAPAnnouncement:     leAudio.CAPAnnouncement,
		LEAudioTMAPAnnouncement:    leAudio.TMAPAnnouncement,
		LEAudioHeadsetAppearance:   leAudio.HeadsetAppearance,
		LEAudioSinkConfigured:      leAudio.SinkConfigured, LEAudioSourceConfigured: leAudio.SourceConfigured,
		LEAudioSinkAcquired: leAudio.SinkAcquired, LEAudioSourceAcquired: leAudio.SourceAcquired,
		LEAudioBidirectionalCIS: leAudio.BidirectionalCIS,
	}
	result.OK = true
	result.Ready = result.MNSRegistered && result.MAPConnected && result.NotificationsEnabled && initialized
	if !s.lastPoll.IsZero() {
		result.LastPollUnix = s.lastPoll.Unix()
	}
	return result
}
