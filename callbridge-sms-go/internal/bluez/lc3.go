package bluez

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"time"
)

const (
	LC3Codec = byte(0x06)

	PACSinkUUID   = "00002bc9-0000-1000-8000-00805f9b34fb"
	PACSourceUUID = "00002bcb-0000-1000-8000-00805f9b34fb"
	ASCSUUID      = "0000184e-0000-1000-8000-00805f9b34fb"
	PACSUUID      = "00001850-0000-1000-8000-00805f9b34fb"
	CASUUID       = "00001853-0000-1000-8000-00805f9b34fb"
	TMASUUID      = "00001855-0000-1000-8000-00805f9b34fb"
	VCSUUID       = "00001844-0000-1000-8000-00805f9b34fb"
	TMAPRoleCT    = "ct"

	UnspecifiedContext    = uint16(0x0001)
	ConversationalContext = uint16(0x0002)
	MediaContext          = uint16(0x0004)
	SupportedContexts     = UnspecifiedContext | ConversationalContext
	FrontLeftLocation     = uint32(0x00000001)
	FrontCenterLocation   = uint32(0x00000004)
	HeadsetAppearance     = uint16(0x0942)

	ltvFrequency         = byte(0x01)
	ltvDuration          = byte(0x02)
	ltvChannelAllocation = byte(0x03)
	ltvFrameLength       = byte(0x04)
	ltvFrameBlocksPerSDU = byte(0x05)
)

var LC3Capabilities = []byte{
	0x03, ltvFrequency, 0x34, 0x00, // 16, 24, and 32 kHz
	0x02, ltvDuration, 0x23, // 7.5 and 10 ms supported; 10 ms preferred
	0x02, ltvChannelAllocation, 0x01, // one channel
	0x05, ltvFrameLength, 30, 0, 80, 0,
}

type CodecConfig struct {
	SampleRate        int
	FrameDuration     time.Duration
	OctetsPerFrame    int
	ChannelAllocation uint32
}

type preset struct {
	name          string
	frequencyCode byte
	frequencyBit  uint16
	durationCode  byte
	durationBit   byte
	sampleRate    int
	frameDuration time.Duration
	octets        int
}

var conversationalPresets = []preset{
	// TMAP CT mandates 16_1, 32_1, and 32_2 in both PAC directions. 32_2
	// remains the preference for the current Asterisk conversational path.
	{name: "32_2", frequencyCode: 0x06, frequencyBit: 1 << 5, durationCode: 0x01, durationBit: 1 << 1, sampleRate: 32000, frameDuration: 10 * time.Millisecond, octets: 80},
	{name: "32_1", frequencyCode: 0x06, frequencyBit: 1 << 5, durationCode: 0x00, durationBit: 1 << 0, sampleRate: 32000, frameDuration: 7500 * time.Microsecond, octets: 60},
	{name: "16_1", frequencyCode: 0x03, frequencyBit: 1 << 2, durationCode: 0x00, durationBit: 1 << 0, sampleRate: 16000, frameDuration: 7500 * time.Microsecond, octets: 30},
	{name: "24_2", frequencyCode: 0x05, frequencyBit: 1 << 4, durationCode: 0x01, durationBit: 1 << 1, sampleRate: 24000, frameDuration: 10 * time.Millisecond, octets: 60},
	{name: "24_1", frequencyCode: 0x05, frequencyBit: 1 << 4, durationCode: 0x00, durationBit: 1 << 0, sampleRate: 24000, frameDuration: 7500 * time.Microsecond, octets: 45},
	{name: "16_2", frequencyCode: 0x03, frequencyBit: 1 << 2, durationCode: 0x01, durationBit: 1 << 1, sampleRate: 16000, frameDuration: 10 * time.Millisecond, octets: 40},
}

func SelectConfiguration(capabilities []byte) ([]byte, error) {
	return SelectConfigurationForAllocation(capabilities, FrontLeftLocation)
}

func SelectConfigurationForAllocation(capabilities []byte, allocation uint32) ([]byte, error) {
	if allocation == 0 || bits.OnesCount32(allocation) != 1 {
		return nil, errors.New("LC3 channel allocation must select one location")
	}
	values, err := parseLTV(capabilities)
	if err != nil {
		return nil, err
	}
	frequency, ok := values[ltvFrequency]
	if !ok || len(frequency) != 2 {
		return nil, errors.New("LC3 frequency capability missing")
	}
	duration, ok := values[ltvDuration]
	if !ok || len(duration) != 1 || duration[0]&0x03 == 0 {
		return nil, errors.New("LC3 frame duration capability missing")
	}
	channels, ok := values[ltvChannelAllocation]
	if !ok || len(channels) != 1 || channels[0]&0x01 == 0 {
		return nil, errors.New("LC3 mono capability missing")
	}
	frameLength, ok := values[ltvFrameLength]
	if !ok || len(frameLength) != 4 {
		return nil, errors.New("LC3 frame length capability missing")
	}
	frequencyMask := binary.LittleEndian.Uint16(frequency)
	minimum := int(binary.LittleEndian.Uint16(frameLength[:2]))
	maximum := int(binary.LittleEndian.Uint16(frameLength[2:]))
	if minimum == 0 || minimum > maximum {
		return nil, errors.New("invalid LC3 frame length range")
	}
	for _, candidate := range conversationalPresets {
		if frequencyMask&candidate.frequencyBit != 0 && duration[0]&candidate.durationBit != 0 &&
			candidate.octets >= minimum && candidate.octets <= maximum {
			return buildConfiguration(candidate, allocation), nil
		}
	}
	return nil, errors.New("no supported conversational LC3 preset")
}

func ParseConfiguration(configuration []byte) (CodecConfig, error) {
	values, err := parseLTV(configuration)
	if err != nil {
		return CodecConfig{}, err
	}
	frequency, ok := values[ltvFrequency]
	if !ok || len(frequency) != 1 {
		return CodecConfig{}, errors.New("LC3 frequency configuration missing")
	}
	duration, ok := values[ltvDuration]
	if !ok || len(duration) != 1 || duration[0] > 0x01 {
		return CodecConfig{}, errors.New("unsupported LC3 frame duration")
	}
	frameLength, ok := values[ltvFrameLength]
	if !ok || len(frameLength) != 2 {
		return CodecConfig{}, errors.New("LC3 frame length configuration missing")
	}
	if frameBlocks, present := values[ltvFrameBlocksPerSDU]; present {
		if len(frameBlocks) != 1 || frameBlocks[0] != 1 {
			return CodecConfig{}, errors.New("LC3 configuration requires one frame block per SDU")
		}
	}
	allocation := FrontLeftLocation
	if raw, present := values[ltvChannelAllocation]; present {
		if len(raw) != 4 {
			return CodecConfig{}, errors.New("invalid LC3 channel allocation")
		}
		allocation = binary.LittleEndian.Uint32(raw)
		if allocation == 0 || bits.OnesCount32(allocation) != 1 {
			return CodecConfig{}, errors.New("LC3 configuration must be mono")
		}
	}
	octets := int(binary.LittleEndian.Uint16(frameLength))
	for _, candidate := range conversationalPresets {
		if frequency[0] == candidate.frequencyCode && duration[0] == candidate.durationCode {
			if octets != candidate.octets {
				return CodecConfig{}, fmt.Errorf("LC3 %s requires %d octets", candidate.name, candidate.octets)
			}
			return CodecConfig{
				SampleRate: candidate.sampleRate, FrameDuration: candidate.frameDuration,
				OctetsPerFrame: octets, ChannelAllocation: allocation,
			}, nil
		}
	}
	return CodecConfig{}, errors.New("unsupported LC3 conversational preset")
}

func buildConfiguration(candidate preset, allocation uint32) []byte {
	return []byte{
		0x02, ltvFrequency, candidate.frequencyCode,
		0x02, ltvDuration, candidate.durationCode,
		0x05, ltvChannelAllocation, byte(allocation), byte(allocation >> 8), byte(allocation >> 16), byte(allocation >> 24),
		0x03, ltvFrameLength, byte(candidate.octets), byte(candidate.octets >> 8),
	}
}

func parseLTV(raw []byte) (map[byte][]byte, error) {
	if len(raw) == 0 || len(raw) > 64 {
		return nil, errors.New("LC3 LTV length outside bound")
	}
	values := make(map[byte][]byte)
	for offset := 0; offset < len(raw); {
		length := int(raw[offset])
		if length < 2 || offset+1+length > len(raw) {
			return nil, errors.New("malformed LC3 LTV")
		}
		typeID := raw[offset+1]
		if _, duplicate := values[typeID]; duplicate {
			return nil, errors.New("duplicate LC3 LTV type")
		}
		value := make([]byte, length-1)
		copy(value, raw[offset+2:offset+1+length])
		values[typeID] = value
		offset += length + 1
	}
	return values, nil
}
