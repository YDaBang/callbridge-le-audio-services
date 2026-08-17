package mns

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestServiceRecord(t *testing.T) {
	record := serviceRecord(19)
	for _, fragment := range []string{`<uuid value="0x1133"/>`, `<uuid value="0x1134"/>`, `<uint8 value="0x13"/>`, `<uint16 value="0x0104"/>`} {
		if !strings.Contains(record, fragment) {
			t.Fatalf("service record missing %q", fragment)
		}
	}
	var parsed any
	if err := xml.Unmarshal([]byte(record), &parsed); err != nil {
		t.Fatalf("invalid service record XML: %v", err)
	}
}

func TestParseEvent(t *testing.T) {
	raw := []byte(`noise<?xml version="1.0"?><MAP-event-report><event type="NewMessage" handle="00AB12" folder="telecom/msg/inbox" msg_type="MMS" datetime="20260723T102719"/></MAP-event-report>`)
	event, err := parseEvent(raw)
	if err != nil || event.EventType != "NewMessage" || event.Handle != "00AB12" || event.MessageType != "MMS" {
		t.Fatalf("parseEvent()=%#v,%v", event, err)
	}
	if _, err := parseEvent([]byte(`<event type="NewMessage" handle="../../bad"/>`)); err == nil {
		t.Fatal("accepted unsafe handle")
	}
}
