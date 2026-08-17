package config

import "testing"

func validEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("BT_DEVICE", "AA:BB:CC:DD:EE:FF")
	t.Setenv("BT_ADAPTER", "hci0")
	t.Setenv("MAS_CHANNEL", "5")
	t.Setenv("MNS_CHANNEL", "19")
	t.Setenv("OUTBOX_ALLOWED_SOURCES", "127.0.0.0/8,10.0.0.1/32")
	t.Setenv("SMS_RECIPIENTS", "1002")
}

func TestLoadValid(t *testing.T) {
	validEnvironment(t)
	cfg, err := Load()
	if err != nil || cfg.MASChannel != 5 || len(cfg.AllowedSources) != 2 || cfg.InternalListen != "127.0.0.1:8765" || cfg.MediaURL != "https://mmmsg.acrobits.net/" || cfg.LEAudioMode != "off" || cfg.ClassicProfileMode != ClassicProfileNormal {
		t.Fatalf("Load()=%#v,%v", cfg, err)
	}
}

func TestLEAudioIsDefaultOffAndBounded(t *testing.T) {
	validEnvironment(t)
	t.Setenv("LE_AUDIO_MODE", "le-canary")
	t.Setenv("LE_AUDIO_SOCKET", "/run/asterisk-leaudio/test.sock")
	t.Setenv("LE_AUDIO_PEER_UID", "123")
	cfg, err := Load()
	if err != nil || cfg.LEAudioMode != "le-canary" || cfg.LEAudioPeerUID != 123 {
		t.Fatalf("Load()=%#v,%v", cfg, err)
	}

	validEnvironment(t)
	t.Setenv("LE_AUDIO_MODE", "auto")
	if _, err := Load(); err == nil {
		t.Fatal("accepted unimplemented LE Audio policy")
	}
}

func TestClassicProfileIsolationRequiresLECanary(t *testing.T) {
	validEnvironment(t)
	t.Setenv("CLASSIC_PROFILE_MODE", ClassicProfileIsolated)
	if _, err := Load(); err == nil {
		t.Fatal("accepted Classic isolation while LE Audio was off")
	}

	validEnvironment(t)
	t.Setenv("CLASSIC_PROFILE_MODE", ClassicProfileIsolated)
	t.Setenv("LE_AUDIO_MODE", "le-canary")
	cfg, err := Load()
	if err != nil || cfg.ClassicProfileMode != ClassicProfileIsolated {
		t.Fatalf("Load()=%#v,%v", cfg, err)
	}

	validEnvironment(t)
	t.Setenv("CLASSIC_PROFILE_MODE", "disabled")
	if _, err := Load(); err == nil {
		t.Fatal("accepted unknown Classic profile mode")
	}
}

func TestRejectsUnpinnedMediaService(t *testing.T) {
	validEnvironment(t)
	t.Setenv("MEDIA_URL", "https://example.com/")
	if _, err := Load(); err == nil {
		t.Fatal("accepted unpinned media service")
	}
}

func TestRejectsBroadOrPublicConfiguration(t *testing.T) {
	validEnvironment(t)
	t.Setenv("OUTBOX_ALLOWED_SOURCES", "10.0.0.0/24")
	if _, err := Load(); err == nil {
		t.Fatal("accepted broad non-loopback source")
	}
	validEnvironment(t)
	t.Setenv("INTERNAL_LISTEN", "0.0.0.0:8765")
	if _, err := Load(); err == nil {
		t.Fatal("accepted exposed internal API")
	}
	validEnvironment(t)
	t.Setenv("AMI_HOST", "8.8.8.8")
	if _, err := Load(); err == nil {
		t.Fatal("accepted public AMI address")
	}
}
