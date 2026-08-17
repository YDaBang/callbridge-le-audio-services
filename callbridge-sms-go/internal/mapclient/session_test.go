package mapclient

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"callbridge.local/callbridge-sms-go/internal/obex"
)

type automaticTransport struct {
	reads   bytes.Buffer
	writes  [][]byte
	listing []byte
}

func (t *automaticTransport) ReadFull(buffer []byte) error {
	_, err := io.ReadFull(&t.reads, buffer)
	return err
}

func (t *automaticTransport) WriteAll(buffer []byte) error {
	packet := append([]byte(nil), buffer...)
	t.writes = append(t.writes, packet)
	code := obex.ResponseOK
	payload := []byte(nil)
	switch packet[0] {
	case opPut:
		code = obex.ResponseContinue
	case opGetFinal:
		payload = obex.MustHeader(headerEndOfBody, t.listing)
	}
	response := make([]byte, len(payload)+3)
	response[0] = byte(code)
	binary.BigEndian.PutUint16(response[1:3], uint16(len(response)))
	copy(response[3:], payload)
	_, _ = t.reads.Write(response)
	return nil
}

func (t *automaticTransport) Close() error { return nil }

func TestPushBMessageFragmentsWithinNegotiatedPacket(t *testing.T) {
	transport := &automaticTransport{}
	session := &Session{conn: transport, connection: 7, packetMax: 96, currentPath: "telecom/msg/outbox"}
	body := bytes.Repeat([]byte("x"), 400)
	if err := session.PushBMessage("telecom/msg/outbox", string(body), false, true); err != nil {
		t.Fatal(err)
	}
	if len(transport.writes) < 3 || transport.writes[0][0] != opPut || transport.writes[len(transport.writes)-1][0] != opPutFinal {
		t.Fatalf("unexpected PUT sequence count=%d", len(transport.writes))
	}
	var reconstructed []byte
	for index, packet := range transport.writes {
		if len(packet) > session.packetMax {
			t.Fatalf("packet %d exceeds negotiated maximum: %d", index, len(packet))
		}
		headers, err := obex.ParseHeaders(packet[3:], 0)
		if err != nil {
			t.Fatal(err)
		}
		reconstructed = append(reconstructed, obex.CollectBody(headers, headerBody, headerEndOfBody)...)
		_, hasType := obex.Find(headers, headerType)
		if (index == 0) != hasType {
			t.Fatalf("type header presence on packet %d is %t", index, hasType)
		}
	}
	if !bytes.Equal(reconstructed, body) {
		t.Fatal("fragmentation changed bMessage")
	}
}

func TestFolderCacheAvoidsRepeatedSetPath(t *testing.T) {
	transport := &automaticTransport{listing: []byte(`<?xml version="1.0"?><MAP-msg-listing><msg handle="ABCD" type="SMS_GSM"/></MAP-msg-listing>`)}
	session := &Session{conn: transport, connection: 7, packetMax: 0xffff}
	for i := 0; i < 2; i++ {
		items, err := session.ListMessages("telecom/msg/inbox", 20, 0)
		if err != nil || len(items) != 1 || items[0].Handle != "ABCD" {
			t.Fatalf("ListMessages()=%#v,%v", items, err)
		}
	}
	setPaths := 0
	gets := 0
	for _, packet := range transport.writes {
		switch packet[0] {
		case opSetPath:
			setPaths++
		case opGetFinal:
			gets++
		}
	}
	if setPaths != 3 || gets != 2 {
		t.Fatalf("setpaths=%d gets=%d", setPaths, gets)
	}
}

func TestGetMessageRequestsAttachments(t *testing.T) {
	transport := &automaticTransport{listing: []byte("BEGIN:BMSG\r\nEND:BMSG\r\n")}
	session := &Session{conn: transport, connection: 7, packetMax: 0xffff, currentPath: "telecom/msg/inbox"}
	if _, err := session.GetMessage("telecom/msg/inbox", "ABCD"); err != nil {
		t.Fatal(err)
	}
	if len(transport.writes) != 1 || transport.writes[0][0] != opGetFinal {
		t.Fatalf("unexpected GET packets: %d", len(transport.writes))
	}
	headers, err := obex.ParseHeaders(transport.writes[0][3:], 0)
	if err != nil {
		t.Fatal(err)
	}
	params, ok := obex.Find(headers, headerAppParams)
	if !ok || !bytes.Contains(params, []byte{tagAttachment, 0x01, 0x01}) {
		t.Fatalf("attachment application parameter missing: %x", params)
	}
}

func TestTransportErrorClassification(t *testing.T) {
	if !IsTransportError(io.ErrUnexpectedEOF) {
		t.Fatal("EOF must reconnect")
	}
	if IsTransportError(io.ErrNoProgress) {
		t.Fatal("logical/local error must not force reconnect")
	}
}
