package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
)

const Version = "1.2.0-p6"

const (
	ClassicProfileNormal   = "normal"
	ClassicProfileIsolated = "isolated"
)

type Config struct {
	BluetoothDevice string
	Adapter         string
	MASChannel      uint8
	MNSChannel      uint16
	MAPTimeout      time.Duration
	PollInterval    time.Duration
	Keepalive       time.Duration
	PollMaxCount    int

	InternalListen string
	OutboxListen   string
	AllowedSources []netip.Prefix

	AMIAddress    string
	AMIUser       string
	AMISecretFile string
	Recipients    []string
	MediaURL      string
	MediaMaxBytes int
	MediaTimeout  time.Duration

	LEAudioMode    string
	LEAudioSocket  string
	LEAudioPeerUID int

	ClassicProfileMode string

	StateFile           string
	LogFile             string
	LegacySeenFile      string
	LegacyMessageLog    string
	LegacyForwardState  string
	LegacyOutboundQueue string
}

func Load() (Config, error) {
	cfg := Config{
		BluetoothDevice:     strings.TrimSpace(os.Getenv("BT_DEVICE")),
		Adapter:             env("BT_ADAPTER", "hci0"),
		InternalListen:      env("INTERNAL_LISTEN", "127.0.0.1:8765"),
		OutboxListen:        env("OUTBOX_LISTEN", "0.0.0.0:8788"),
		AMIAddress:          net.JoinHostPort(env("AMI_HOST", "127.0.0.1"), env("AMI_PORT", "5038")),
		AMIUser:             env("AMI_USER", "callbridge"),
		AMISecretFile:       env("AMI_SECRET_FILE", "/etc/callbridge-sms-go/ami-secret"),
		MediaURL:            env("MEDIA_URL", "https://mmmsg.acrobits.net/"),
		LEAudioMode:         env("LE_AUDIO_MODE", "off"),
		LEAudioSocket:       env("LE_AUDIO_SOCKET", "/run/asterisk-leaudio/leaudio.sock"),
		ClassicProfileMode:  env("CLASSIC_PROFILE_MODE", ClassicProfileNormal),
		StateFile:           env("STATE_FILE", "/var/log/callbridge_sms_go.state.json"),
		LogFile:             env("LOG_FILE", "/var/log/callbridge_sms_go.log"),
		LegacySeenFile:      env("LEGACY_SEEN_FILE", "/var/log/callbridge_sms_seen_handles.json"),
		LegacyMessageLog:    env("LEGACY_MESSAGE_LOG", "/var/log/callbridge_sms_messages.jsonl"),
		LegacyForwardState:  env("LEGACY_FORWARD_STATE", "/var/log/callbridge_sms_forward_to_ami.state"),
		LegacyOutboundQueue: env("LEGACY_OUTBOUND_QUEUE", "/tmp/callbridge_sms_out_queue.jsonl"),
	}
	var err error
	if cfg.MASChannel, err = uint8Env("MAS_CHANNEL", 0, 1, 30); err != nil {
		return Config{}, err
	}
	mns, err := intEnv("MNS_CHANNEL", 19, 1, 30)
	if err != nil {
		return Config{}, err
	}
	cfg.MNSChannel = uint16(mns)
	seconds, err := intEnv("MAP_TIMEOUT_SEC", 15, 3, 60)
	if err != nil {
		return Config{}, err
	}
	cfg.MAPTimeout = time.Duration(seconds) * time.Second
	seconds, err = intEnv("POLL_INTERVAL_SEC", 10, 5, 300)
	if err != nil {
		return Config{}, err
	}
	cfg.PollInterval = time.Duration(seconds) * time.Second
	seconds, err = intEnv("KEEPALIVE_SEC", 60, 10, 600)
	if err != nil {
		return Config{}, err
	}
	cfg.Keepalive = time.Duration(seconds) * time.Second
	cfg.PollMaxCount, err = intEnv("POLL_MAX_COUNT", 50, 10, 500)
	if err != nil {
		return Config{}, err
	}
	cfg.AllowedSources, err = prefixes(env("OUTBOX_ALLOWED_SOURCES", "127.0.0.0/8"))
	if err != nil {
		return Config{}, err
	}
	cfg.Recipients, err = recipients(env("SMS_RECIPIENTS", "1002"))
	if err != nil {
		return Config{}, err
	}
	cfg.MediaMaxBytes, err = intEnv("MEDIA_MAX_BYTES", 1536<<10, 64<<10, 2<<20)
	if err != nil {
		return Config{}, err
	}
	seconds, err = intEnv("MEDIA_TIMEOUT_SEC", 30, 10, 120)
	if err != nil {
		return Config{}, err
	}
	cfg.MediaTimeout = time.Duration(seconds) * time.Second
	cfg.LEAudioPeerUID, err = intEnv("LE_AUDIO_PEER_UID", 0, 0, 1<<31-1)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.BluetoothDevice == "" {
		return errors.New("BT_DEVICE is required")
	}
	if _, err := ParseBluetoothAddress(c.BluetoothDevice); err != nil {
		return err
	}
	if !validAdapter(c.Adapter) {
		return errors.New("invalid Bluetooth adapter")
	}
	if c.MASChannel < 1 || c.MASChannel > 30 || c.MNSChannel < 1 || c.MNSChannel > 30 {
		return errors.New("Bluetooth channel outside 1..30")
	}
	if c.MASChannel == uint8(c.MNSChannel) {
		return errors.New("MAS and MNS channels must differ")
	}
	internalAddress, err := addressOf(c.InternalListen)
	if err != nil || !internalAddress.IsLoopback() {
		return errors.New("internal API must listen on loopback IPv4")
	}
	if _, err := addressOf(c.OutboxListen); err != nil {
		return errors.New("invalid outbox listen address")
	}
	amiAddress, err := addressOf(c.AMIAddress)
	if err != nil || (!amiAddress.IsPrivate() && !amiAddress.IsLoopback()) {
		return errors.New("AMI address must be private IPv4")
	}
	for _, path := range []string{
		c.AMISecretFile, c.StateFile, c.LogFile, c.LegacySeenFile,
		c.LegacyMessageLog, c.LegacyForwardState, c.LegacyOutboundQueue,
		c.LEAudioSocket,
	} {
		if !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "\x00\r\n") {
			return errors.New("runtime paths must be absolute")
		}
	}
	if c.AMISecretFile == c.StateFile || c.AMISecretFile == c.LogFile || c.StateFile == c.LogFile {
		return errors.New("secret, state, and log paths must differ")
	}
	if c.LEAudioMode != "off" && c.LEAudioMode != "le-canary" {
		return errors.New("LE_AUDIO_MODE must be off or le-canary")
	}
	if c.ClassicProfileMode != ClassicProfileNormal && c.ClassicProfileMode != ClassicProfileIsolated {
		return errors.New("CLASSIC_PROFILE_MODE must be normal or isolated")
	}
	if c.ClassicProfileMode == ClassicProfileIsolated && c.LEAudioMode != "le-canary" {
		return errors.New("CLASSIC_PROFILE_MODE=isolated requires LE_AUDIO_MODE=le-canary")
	}
	if c.LEAudioSocket == c.AMISecretFile || c.LEAudioSocket == c.StateFile || c.LEAudioSocket == c.LogFile {
		return errors.New("LE Audio socket must not overlap persistent paths")
	}
	if len(c.AllowedSources) == 0 || len(c.Recipients) == 0 {
		return errors.New("source and recipient allowlists must not be empty")
	}
	if c.MediaURL != "https://mmmsg.acrobits.net/" {
		return errors.New("MEDIA_URL must use the pinned Groundwire media service")
	}
	if c.MediaMaxBytes < 64<<10 || c.MediaMaxBytes > 2<<20 || c.MediaTimeout < 10*time.Second || c.MediaTimeout > 120*time.Second {
		return errors.New("media bounds invalid")
	}
	for _, prefix := range c.AllowedSources {
		if prefix.Addr().IsLoopback() {
			if prefix.Bits() < 8 {
				return errors.New("loopback source prefix is too broad")
			}
			continue
		}
		if prefix.Bits() != 32 {
			return errors.New("non-loopback outbox sources must be exact IPv4 addresses")
		}
	}
	if len(c.AllowedSources) > 16 || len(c.Recipients) > 8 {
		return errors.New("source or recipient allowlist exceeds bound")
	}
	return nil
}

func addressOf(value string) (netip.Addr, error) {
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		return netip.Addr{}, err
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.Is4() {
		return netip.Addr{}, errors.New("address is not IPv4")
	}
	return address, nil
}

func validAdapter(value string) bool {
	if !strings.HasPrefix(value, "hci") || len(value) < 4 || len(value) > 16 {
		return false
	}
	for _, r := range value[3:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ParseBluetoothAddress validates only. Byte-order conversion belongs to the
// Linux RFCOMM adapter package.
func ParseBluetoothAddress(value string) ([6]byte, error) {
	var result [6]byte
	parts := strings.Split(value, ":")
	if len(parts) != 6 {
		return result, errors.New("invalid Bluetooth address")
	}
	for index, part := range parts {
		if len(part) != 2 {
			return result, errors.New("invalid Bluetooth address")
		}
		parsed, err := strconv.ParseUint(part, 16, 8)
		if err != nil {
			return result, errors.New("invalid Bluetooth address")
		}
		result[index] = byte(parsed)
	}
	return result, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func intEnv(name string, fallback, min, max int) (int, error) {
	raw := env(name, strconv.Itoa(fallback))
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("%s outside %d..%d", name, min, max)
	}
	return value, nil
}

func uint8Env(name string, fallback, min, max int) (uint8, error) {
	value, err := intEnv(name, fallback, min, max)
	return uint8(value), err
}

func prefixes(raw string) ([]netip.Prefix, error) {
	var result []netip.Prefix
	seen := make(map[netip.Prefix]struct{})
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if !strings.Contains(item, "/") {
			item += "/32"
		}
		prefix, err := netip.ParsePrefix(item)
		if err != nil || !prefix.Addr().Is4() {
			return nil, errors.New("invalid OUTBOX_ALLOWED_SOURCES")
		}
		prefix = prefix.Masked()
		if _, ok := seen[prefix]; !ok {
			seen[prefix] = struct{}{}
			result = append(result, prefix)
		}
	}
	return result, nil
}

func recipients(raw string) ([]string, error) {
	var result []string
	seen := make(map[string]struct{})
	for _, value := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' }) {
		value = strings.TrimSpace(value)
		if value == "" || value == "1001" {
			continue
		}
		if len(value) > 32 {
			return nil, errors.New("invalid SMS recipient")
		}
		for _, r := range value {
			if r < '0' || r > '9' {
				return nil, errors.New("invalid SMS recipient")
			}
		}
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result, nil
}
