package obex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf16"
)

const (
	ResponseContinue       = 0x90
	ResponseOK             = 0xA0
	ResponseBadRequest     = 0xC0
	ResponseNotImplemented = 0xD1
)

type Transport interface {
	ReadFull([]byte) error
	WriteAll([]byte) error
}

type Packet struct {
	Code    byte
	Payload []byte
}

type Header struct {
	ID    byte
	Value []byte
}

func ReadPacket(conn Transport, maximum int) (Packet, error) {
	if maximum < 3 || maximum > 0xffff {
		return Packet{}, errors.New("invalid OBEX packet bound")
	}
	header := make([]byte, 3)
	if err := conn.ReadFull(header); err != nil {
		return Packet{}, err
	}
	length := int(binary.BigEndian.Uint16(header[1:]))
	if length < 3 || length > maximum {
		return Packet{}, errors.New("OBEX packet length outside bound")
	}
	payload := make([]byte, length-3)
	if err := conn.ReadFull(payload); err != nil {
		return Packet{}, err
	}
	return Packet{Code: header[0], Payload: payload}, nil
}

func WritePacket(conn Transport, code byte, payload []byte) error {
	if len(payload)+3 > 0xffff {
		return errors.New("OBEX packet too large")
	}
	packet := make([]byte, len(payload)+3)
	packet[0] = code
	binary.BigEndian.PutUint16(packet[1:3], uint16(len(packet)))
	copy(packet[3:], payload)
	return conn.WriteAll(packet)
}

func ParseHeaders(data []byte, offset int) ([]Header, error) {
	if offset < 0 || offset > len(data) {
		return nil, errors.New("invalid OBEX header offset")
	}
	var headers []Header
	for offset < len(data) {
		id := data[offset]
		offset++
		switch id & 0xc0 {
		case 0x00, 0x40:
			if offset+2 > len(data) {
				return nil, errors.New("truncated OBEX length header")
			}
			length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			if length < 3 || offset+length-1 > len(data) {
				return nil, errors.New("invalid OBEX header length")
			}
			value := append([]byte(nil), data[offset+2:offset+length-1]...)
			offset += length - 1
			headers = append(headers, Header{ID: id, Value: value})
		case 0x80:
			if offset >= len(data) {
				return nil, errors.New("truncated OBEX byte header")
			}
			headers = append(headers, Header{ID: id, Value: []byte{data[offset]}})
			offset++
		case 0xc0:
			if offset+4 > len(data) {
				return nil, errors.New("truncated OBEX uint32 header")
			}
			headers = append(headers, Header{ID: id, Value: append([]byte(nil), data[offset:offset+4]...)})
			offset += 4
		}
	}
	return headers, nil
}

func BuildHeader(id byte, value []byte) ([]byte, error) {
	switch id & 0xc0 {
	case 0x00, 0x40:
		if len(value)+3 > 0xffff {
			return nil, errors.New("OBEX header too large")
		}
		result := make([]byte, len(value)+3)
		result[0] = id
		binary.BigEndian.PutUint16(result[1:3], uint16(len(result)))
		copy(result[3:], value)
		return result, nil
	case 0x80:
		if len(value) != 1 {
			return nil, errors.New("OBEX byte header requires one byte")
		}
		return []byte{id, value[0]}, nil
	case 0xc0:
		if len(value) != 4 {
			return nil, errors.New("OBEX uint32 header requires four bytes")
		}
		return append([]byte{id}, value...), nil
	default:
		return nil, errors.New("unknown OBEX header kind")
	}
}

func MustHeader(id byte, value []byte) []byte {
	result, err := BuildHeader(id, value)
	if err != nil {
		panic(err)
	}
	return result
}

func Uint32Header(id byte, value uint32) []byte {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, value)
	return MustHeader(id, raw)
}

func NameHeader(id byte, value string) []byte {
	units := utf16.Encode([]rune(value + "\x00"))
	raw := make([]byte, len(units)*2)
	for index, unit := range units {
		binary.BigEndian.PutUint16(raw[index*2:], unit)
	}
	return MustHeader(id, raw)
}

func DecodeName(value []byte) (string, error) {
	if len(value)%2 != 0 {
		return "", errors.New("invalid OBEX Unicode header")
	}
	units := make([]uint16, len(value)/2)
	for index := range units {
		units[index] = binary.BigEndian.Uint16(value[index*2:])
	}
	if len(units) > 0 && units[len(units)-1] == 0 {
		units = units[:len(units)-1]
	}
	return string(utf16.Decode(units)), nil
}

func Find(headers []Header, id byte) ([]byte, bool) {
	for _, header := range headers {
		if header.ID == id {
			return header.Value, true
		}
	}
	return nil, false
}

func Uint32(value []byte) (uint32, error) {
	if len(value) != 4 {
		return 0, fmt.Errorf("invalid uint32 header length %d", len(value))
	}
	return binary.BigEndian.Uint32(value), nil
}

func CollectBody(headers []Header, bodyID, endID byte) []byte {
	var result []byte
	for _, header := range headers {
		if header.ID == bodyID || header.ID == endID {
			result = append(result, header.Value...)
		}
	}
	return result
}
