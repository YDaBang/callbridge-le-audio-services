package obex

import (
	"bytes"
	"testing"
)

func TestHeadersRoundTrip(t *testing.T) {
	raw := append(MustHeader(0x42, []byte("type\x00")), NameHeader(0x01, "한글")...)
	raw = append(raw, Uint32Header(0xcb, 0x01020304)...)
	raw = append(raw, MustHeader(0x80, []byte{7})...)
	headers, err := ParseHeaders(raw, 0)
	if err != nil || len(headers) != 4 {
		t.Fatalf("ParseHeaders()=%d,%v", len(headers), err)
	}
	name, err := DecodeName(headers[1].Value)
	if err != nil || name != "한글" {
		t.Fatalf("DecodeName()=%q,%v", name, err)
	}
	value, err := Uint32(headers[2].Value)
	if err != nil || value != 0x01020304 {
		t.Fatalf("Uint32()=%x,%v", value, err)
	}
}

func TestParseHeadersRejectsTruncation(t *testing.T) {
	for _, raw := range [][]byte{{0x42}, {0x42, 0, 2}, {0xcb, 1, 2}, {0x80}} {
		if _, err := ParseHeaders(raw, 0); err == nil {
			t.Fatalf("accepted truncated header %x", raw)
		}
	}
}

type memoryTransport struct {
	read  *bytes.Reader
	write bytes.Buffer
}

func (m *memoryTransport) ReadFull(buffer []byte) error {
	_, err := m.read.Read(buffer)
	return err
}
func (m *memoryTransport) WriteAll(buffer []byte) error {
	_, err := m.write.Write(buffer)
	return err
}

func TestPacketReadWriteAndBound(t *testing.T) {
	transport := &memoryTransport{read: bytes.NewReader([]byte{ResponseOK, 0, 5, 1, 2})}
	packet, err := ReadPacket(transport, 5)
	if err != nil || packet.Code != ResponseOK || !bytes.Equal(packet.Payload, []byte{1, 2}) {
		t.Fatalf("ReadPacket()=%#v,%v", packet, err)
	}
	if err := WritePacket(transport, ResponseContinue, []byte{3, 4}); err != nil {
		t.Fatal(err)
	}
	if got := transport.write.Bytes(); !bytes.Equal(got, []byte{ResponseContinue, 0, 5, 3, 4}) {
		t.Fatalf("written packet=%x", got)
	}
	oversized := &memoryTransport{read: bytes.NewReader([]byte{ResponseOK, 0, 6, 1, 2, 3})}
	if _, err := ReadPacket(oversized, 5); err == nil {
		t.Fatal("accepted oversized packet")
	}
}
