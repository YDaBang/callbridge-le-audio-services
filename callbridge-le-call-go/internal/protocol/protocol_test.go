package protocol

import (
	"bytes"
	"testing"
)

func TestStateRoundTrip(t *testing.T) {
	want := Message{Type: TypeState, Flags: FlagSnapshot, Sequence: 9, Token: 44,
		Device: "00:11:22:33:44:55", Index: 3, Code: 0, Value: 0}
	packet, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(packet)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.Sequence != want.Sequence || got.Token != want.Token ||
		got.Device != want.Device || got.Index != want.Index || got.Code != want.Code {
		t.Fatalf("round trip=%#v", got)
	}
}

func TestOriginateRoundTrip(t *testing.T) {
	want := Message{Type: TypeCommand, Sequence: 10, Device: "00:11:22:33:44:55",
		Code: OpcodeOriginate, Payload: []byte("tel:+821000000000")}
	packet, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(packet)
	if err != nil || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip=%#v err=%v", got, err)
	}
}

func TestOriginateResultAllowsAndroidUnassignedIndex(t *testing.T) {
	want := Message{Type: TypeResult, Sequence: 11,
		Device: "00:11:22:33:44:55", Code: OpcodeOriginate, Value: 6}
	packet, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(packet)
	if err != nil || got.Index != 0 || got.Token != 0 || got.Code != OpcodeOriginate {
		t.Fatalf("round trip=%#v err=%v", got, err)
	}
}

func TestProtocolRejectsStaleAndMalformedInputs(t *testing.T) {
	invalid := []Message{
		{Type: TypeState, Sequence: 1, Device: "bad", Index: 1, Code: 0, Token: 1},
		{Type: TypeState, Sequence: 1, Device: "00:11:22:33:44:55", Code: NoCallState, Token: 1},
		{Type: TypeCommand, Sequence: 1, Device: "00:11:22:33:44:55", Code: OpcodeAccept},
		{Type: TypeCommand, Sequence: 1, Device: "00:11:22:33:44:55", Code: OpcodeOriginate, Payload: []byte("http://bad")},
		{Type: TypeAck, Sequence: 1, Device: "00:11:22:33:44:55", Code: OpcodeAccept},
		{Type: TypeResult, Sequence: 1, Device: "00:11:22:33:44:55", Index: 1, Code: OpcodeOriginate, Value: 0},
		{Type: TypeResult, Sequence: 1, Device: "00:11:22:33:44:55", Index: 1, Code: OpcodeAccept, Token: 1, Value: 0x05},
	}
	for _, message := range invalid {
		if _, err := message.Encode(); err == nil {
			t.Fatalf("accepted %#v", message)
		}
	}
	packet, err := (Message{Type: TypeReady, Sequence: 1, Device: "00:11:22:33:44:55", Code: 1}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	packet[127] = 1
	if _, err := Decode(packet); err == nil {
		t.Fatal("accepted nonzero padding")
	}
}
