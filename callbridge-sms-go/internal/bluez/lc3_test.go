package bluez

import (
	"testing"
	"time"
)

func TestSelectAndParsePreferredLC3Configuration(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseConfiguration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.SampleRate != 32000 || parsed.FrameDuration != 10*time.Millisecond ||
		parsed.OctetsPerFrame != 80 || parsed.ChannelAllocation != FrontLeftLocation {
		t.Fatalf("unexpected configuration: %#v", parsed)
	}
}

func TestSelectPreferredConfigurationIncludesFrontLeftAllocation(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	values, err := parseLTV(configuration)
	if err != nil {
		t.Fatal(err)
	}
	allocation := values[ltvChannelAllocation]
	if len(allocation) != 4 || allocation[0] != byte(FrontLeftLocation) ||
		allocation[1] != 0 || allocation[2] != 0 || allocation[3] != 0 {
		t.Fatalf("front-left allocation missing: %x", allocation)
	}
}

func TestParseConfigurationWithoutAllocationUsesFrontLeftFallback(t *testing.T) {
	candidate := conversationalPresets[0]
	configuration := []byte{
		0x02, ltvFrequency, candidate.frequencyCode,
		0x02, ltvDuration, candidate.durationCode,
		0x03, ltvFrameLength, byte(candidate.octets), byte(candidate.octets >> 8),
	}
	parsed, err := ParseConfiguration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ChannelAllocation != FrontLeftLocation {
		t.Fatalf("fallback allocation=%#x", parsed.ChannelAllocation)
	}
}

func TestParseAllConversationalPresets(t *testing.T) {
	for _, candidate := range conversationalPresets {
		parsed, err := ParseConfiguration(buildConfiguration(candidate, FrontCenterLocation))
		if err != nil || parsed.SampleRate != candidate.sampleRate || parsed.FrameDuration != candidate.frameDuration ||
			parsed.OctetsPerFrame != candidate.octets {
			t.Fatalf("preset %#v parsed as %#v,%v", candidate, parsed, err)
		}
	}
}

func TestParseConfigurationAcceptsOnlyOneFrameBlockPerSDU(t *testing.T) {
	base := buildConfiguration(conversationalPresets[0], FrontCenterLocation)
	withOne := append(append([]byte(nil), base...), 0x02, ltvFrameBlocksPerSDU, 0x01)
	if _, err := ParseConfiguration(withOne); err != nil {
		t.Fatalf("one frame block rejected: %v", err)
	}
	withTwo := append(append([]byte(nil), base...), 0x02, ltvFrameBlocksPerSDU, 0x02)
	if _, err := ParseConfiguration(withTwo); err == nil {
		t.Fatal("accepted two frame blocks per SDU")
	}
	malformed := append(append([]byte(nil), base...), 0x03, ltvFrameBlocksPerSDU, 0x01, 0x01)
	if _, err := ParseConfiguration(malformed); err == nil {
		t.Fatal("accepted malformed frame-block configuration")
	}
}

func TestTMAPMandatoryCodecSettingsArePresent(t *testing.T) {
	want := map[string]bool{"16_1": false, "32_1": false, "32_2": false}
	for _, candidate := range conversationalPresets {
		if _, required := want[candidate.name]; required {
			want[candidate.name] = true
		}
	}
	for name, present := range want {
		if !present {
			t.Fatalf("mandatory TMAP CT codec setting %s is missing", name)
		}
	}
	values, err := parseLTV(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if duration := values[ltvDuration]; len(duration) != 1 || duration[0]&0x03 != 0x03 {
		t.Fatalf("LC3 capabilities do not expose both durations: %x", duration)
	}
}

func TestSelectMandatorySevenPointFiveMillisecondPreset(t *testing.T) {
	capabilities := []byte{
		0x03, ltvFrequency, 0x04, 0x00,
		0x02, ltvDuration, 0x01,
		0x02, ltvChannelAllocation, 0x01,
		0x05, ltvFrameLength, 30, 0, 30, 0,
	}
	configuration, err := SelectConfigurationForAllocation(capabilities, FrontCenterLocation)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseConfiguration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.SampleRate != 16000 || parsed.FrameDuration != 7500*time.Microsecond || parsed.OctetsPerFrame != 30 {
		t.Fatalf("unexpected 16_1 configuration: %#v", parsed)
	}
}

func TestRejectMalformedDuplicateAndMismatchedLC3(t *testing.T) {
	cases := [][]byte{
		{0x02, ltvFrequency},
		{0x02, ltvFrequency, 0x06, 0x02, ltvFrequency, 0x06},
		{0x02, ltvFrequency, 0x06, 0x02, ltvDuration, 0x01, 0x03, ltvFrameLength, 60, 0},
		{0x02, ltvFrequency, 0x06, 0x02, ltvDuration, 0x01, 0x05, ltvChannelAllocation, 3, 0, 0, 0, 0x03, ltvFrameLength, 80, 0},
	}
	for _, raw := range cases {
		if _, err := ParseConfiguration(raw); err == nil {
			t.Fatalf("accepted invalid LC3 configuration %x", raw)
		}
	}
}

func FuzzParseConfiguration(f *testing.F) {
	f.Add(buildConfiguration(conversationalPresets[0], FrontCenterLocation))
	f.Add([]byte{0x02, ltvFrequency})
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = ParseConfiguration(raw)
	})
}
