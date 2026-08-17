package protocol

import (
	"encoding/binary"
	"errors"
	"net"
	"strings"
)

const (
	// Version 2 carries the caller identity on incoming state messages. There
	// is no negotiation: a version 1 peer rejects the header outright, which is
	// the intended behaviour. Both binaries have to be replaced together, and a
	// mismatch fails loudly instead of dropping identities silently.
	Version     = byte(2)
	PacketSize  = 128
	PayloadSize = PacketSize - 50

	// Identity layout inside a state payload:
	//
	//   [0]              uri length   0..IdentityURIMax
	//   [1 .. n]         uri          scheme stripped, ASCII digits and '+'
	//   [1+n]            name length  0..IdentityNameMax
	//   [2+n .. ]        name         UTF-8, cut on a character boundary
	//
	// 1 + 32 + 1 + 44 is exactly PayloadSize.
	IdentityURIMax  = 32
	IdentityNameMax = 44

	TypeState   = byte(1)
	TypeCommand = byte(2)
	TypeAck     = byte(3)
	TypeResult  = byte(4)
	TypeReady   = byte(5)

	FlagSnapshot = byte(1 << 0)
	FlagLast     = byte(1 << 1)

	NoCallState = byte(0xff)

	OpcodeAccept        = byte(0x00)
	OpcodeTerminate     = byte(0x01)
	OpcodeLocalHold     = byte(0x02)
	OpcodeLocalRetrieve = byte(0x03)
	OpcodeOriginate     = byte(0x04)
	OpcodeJoin          = byte(0x05)

	AckAccepted    = byte(0)
	AckStale       = byte(1)
	AckUnavailable = byte(2)
	AckInvalid     = byte(3)
	AckWriteFailed = byte(4)
)

var magic = [4]byte{'G', 'G', 'C', 'C'}

type Message struct {
	Type     byte
	Flags    byte
	Sequence uint64
	Token    uint64
	Device   string
	Index    byte
	Code     byte
	Value    byte
	Payload  []byte
}

// Identity is the caller of one call, as far as the phone will say.
//
// URI is empty when the number is withheld; Name is empty when the caller is
// not in the phone's contacts, which is the common case.  Name is never a copy
// of the number: a phone that answers the friendly-name read with the number
// itself is treated as having no name, so nothing renders "tel:+82..." where a
// name belongs.
type Identity struct {
	URI  string
	Name string
}

func (i Identity) empty() bool { return i.URI == "" && i.Name == "" }

// EncodeIdentity lays an identity out for a state payload.  Callers that have
// nothing to say send no payload at all.
func EncodeIdentity(identity Identity) ([]byte, error) {
	if identity.empty() {
		return nil, nil
	}
	uri := []byte(identity.URI)
	name := []byte(identity.Name)
	if len(uri) > IdentityURIMax || len(name) > IdentityNameMax {
		return nil, errors.New("caller identity exceeds its bound")
	}
	payload := make([]byte, 0, 2+len(uri)+len(name))
	payload = append(payload, byte(len(uri)))
	payload = append(payload, uri...)
	payload = append(payload, byte(len(name)))
	payload = append(payload, name...)
	return payload, nil
}

// DecodeIdentity reads back what EncodeIdentity wrote.  An empty payload is a
// call with no identity, not an error.
func DecodeIdentity(payload []byte) (Identity, error) {
	if len(payload) == 0 {
		return Identity{}, nil
	}
	uriLength := int(payload[0])
	if uriLength > IdentityURIMax || 1+uriLength >= len(payload) {
		return Identity{}, errors.New("caller identity uri is out of bounds")
	}
	nameOffset := 1 + uriLength
	nameLength := int(payload[nameOffset])
	if nameLength > IdentityNameMax || nameOffset+1+nameLength != len(payload) {
		return Identity{}, errors.New("caller identity name is out of bounds")
	}
	return Identity{
		URI:  string(payload[1 : 1+uriLength]),
		Name: string(payload[nameOffset+1 : nameOffset+1+nameLength]),
	}, nil
}

func (m Message) Encode() ([]byte, error) {
	if err := validate(m); err != nil {
		return nil, err
	}
	packet := make([]byte, PacketSize)
	copy(packet[:4], magic[:])
	packet[4] = Version
	packet[5] = m.Type
	packet[6] = m.Flags
	binary.LittleEndian.PutUint64(packet[8:16], m.Sequence)
	binary.LittleEndian.PutUint64(packet[16:24], m.Token)
	packet[24] = m.Index
	packet[25] = m.Code
	packet[26] = m.Value
	binary.LittleEndian.PutUint16(packet[28:30], uint16(len(m.Payload)))
	copy(packet[32:49], m.Device)
	copy(packet[50:], m.Payload)
	return packet, nil
}

func Decode(packet []byte) (Message, error) {
	if len(packet) != PacketSize || string(packet[:4]) != string(magic[:]) ||
		packet[4] != Version || packet[7] != 0 || packet[27] != 0 ||
		binary.LittleEndian.Uint16(packet[30:32]) != 0 || packet[49] != 0 {
		return Message{}, errors.New("invalid LE call-control packet header")
	}
	payloadLength := int(binary.LittleEndian.Uint16(packet[28:30]))
	if payloadLength > PayloadSize {
		return Message{}, errors.New("LE call-control payload exceeds bound")
	}
	for _, value := range packet[50+payloadLength:] {
		if value != 0 {
			return Message{}, errors.New("nonzero LE call-control packet padding")
		}
	}
	deviceEnd := 32
	for deviceEnd < 49 && packet[deviceEnd] != 0 {
		deviceEnd++
	}
	message := Message{
		Type: packet[5], Flags: packet[6], Sequence: binary.LittleEndian.Uint64(packet[8:16]),
		Token: binary.LittleEndian.Uint64(packet[16:24]), Device: string(packet[32:deviceEnd]),
		Index: packet[24], Code: packet[25], Value: packet[26],
		Payload: append([]byte(nil), packet[50:50+payloadLength]...),
	}
	if err := validate(message); err != nil {
		return Message{}, err
	}
	return message, nil
}

func validate(message Message) error {
	if message.Type < TypeState || message.Type > TypeReady || message.Flags&^(FlagSnapshot|FlagLast) != 0 ||
		message.Sequence == 0 || len(message.Payload) > PayloadSize {
		return errors.New("invalid LE call-control message")
	}
	if normalizedDevice(message.Device) == "" {
		return errors.New("invalid LE call-control device address")
	}
	switch message.Type {
	case TypeState:
		if message.Flags&FlagSnapshot == 0 {
			return errors.New("call state is not a full snapshot")
		}
		if message.Code == NoCallState {
			if message.Index != 0 || message.Token != 0 || message.Value != 0 ||
				len(message.Payload) != 0 {
				return errors.New("invalid empty call snapshot")
			}
		} else if message.Index == 0 || message.Token == 0 || message.Code > 6 || message.Value&^byte(0x07) != 0 {
			return errors.New("invalid call state message")
		} else if _, err := DecodeIdentity(message.Payload); err != nil {
			return err
		}
	case TypeCommand:
		if message.Flags != 0 {
			return errors.New("command carries flags")
		}
		if message.Code > OpcodeJoin {
			return errors.New("invalid call-control opcode")
		}
		if message.Code == OpcodeOriginate {
			if message.Index != 0 || message.Token != 0 || !ValidURI(string(message.Payload)) {
				return errors.New("invalid originate command")
			}
		} else if message.Index == 0 || message.Token == 0 || len(message.Payload) != 0 {
			return errors.New("invalid indexed call command")
		}
	case TypeAck:
		if message.Flags != 0 || message.Code > OpcodeJoin || message.Value > AckWriteFailed || len(message.Payload) != 0 ||
			!validCallReference(message.Code, message.Index, message.Token) {
			return errors.New("invalid call-control acknowledgement")
		}
	case TypeResult:
		if message.Flags != 0 || message.Code > OpcodeJoin || !validCallReference(message.Code, message.Index, message.Token) ||
			!ValidResult(message.Value) || len(message.Payload) != 0 {
			return errors.New("invalid call-control result")
		}
	case TypeReady:
		if message.Flags != 0 || message.Index != 0 || message.Token != 0 || message.Code > 1 || message.Value != 0 || len(message.Payload) != 0 {
			return errors.New("invalid readiness message")
		}
	}
	return nil
}

func validCallReference(opcode, index byte, token uint64) bool {
	if opcode == OpcodeOriginate {
		return (index == 0 && token == 0) || (index != 0 && token != 0)
	}
	return index != 0 && token != 0
}

func ValidResult(result byte) bool {
	return result <= 0x04 || result == 0x06
}

func NormalizeDevice(address string) string {
	return normalizedDevice(address)
}

func normalizedDevice(address string) string {
	hardware, err := net.ParseMAC(strings.TrimSpace(address))
	if err != nil || len(hardware) != 6 {
		return ""
	}
	return strings.ToUpper(hardware.String())
}

func ValidURI(uri string) bool {
	if len(uri) < 5 || len(uri) > PayloadSize || !strings.HasPrefix(uri, "tel:") {
		return false
	}
	for _, character := range uri[4:] {
		if (character < '0' || character > '9') && character != '+' && character != '*' && character != '#' {
			return false
		}
	}
	return true
}
