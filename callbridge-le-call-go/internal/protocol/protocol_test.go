package protocol

import (
	"bytes"
	"strings"
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

func TestIdentityRoundTripAndBounds(t *testing.T) {
	for _, identity := range []Identity{
		{},
		{URI: "+821000000000"},
		{URI: "+821000000000", Name: "테스트발신자"},
		{Name: "이름만"},
		{URI: strings.Repeat("9", IdentityURIMax), Name: strings.Repeat("가", IdentityNameMax/3)},
	} {
		payload, err := EncodeIdentity(identity)
		if err != nil {
			t.Fatalf("encode %#v: %v", identity, err)
		}
		if len(payload) > PayloadSize {
			t.Fatalf("payload %d exceeds %d for %#v", len(payload), PayloadSize, identity)
		}
		got, err := DecodeIdentity(payload)
		if err != nil || got != identity {
			t.Fatalf("round trip %#v -> %#v err=%v", identity, got, err)
		}
	}

	if _, err := EncodeIdentity(Identity{URI: strings.Repeat("9", IdentityURIMax+1)}); err == nil {
		t.Fatal("accepted an over-long uri")
	}
	if _, err := EncodeIdentity(Identity{Name: strings.Repeat("x", IdentityNameMax+1)}); err == nil {
		t.Fatal("accepted an over-long name")
	}
	for _, payload := range [][]byte{
		{1},                  // uri length runs off the end
		{0},                  // no name length byte
		{0, 1},               // name length runs off the end
		{0, 0, 0},            // trailing byte
		{IdentityURIMax + 1}, // uri length above the bound
	} {
		if _, err := DecodeIdentity(payload); err == nil {
			t.Fatalf("accepted malformed identity %v", payload)
		}
	}
}

func TestStateMessageCarriesIdentityButAnEmptySnapshotDoesNot(t *testing.T) {
	payload, err := EncodeIdentity(Identity{URI: "+821000000000", Name: "이름"})
	if err != nil {
		t.Fatal(err)
	}
	withIdentity := Message{Type: TypeState, Flags: FlagSnapshot | FlagLast,
		Sequence: 1, Token: 7, Device: "00:11:22:33:44:55", Index: 3,
		Code: 0, Payload: payload}
	packet, err := withIdentity.Encode()
	if err != nil {
		t.Fatalf("state with identity rejected: %v", err)
	}
	if packet[4] != Version || Version != 2 {
		t.Fatalf("packet version is %d", packet[4])
	}
	decoded, err := Decode(packet)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeIdentity(decoded.Payload)
	if err != nil || got.URI != "+821000000000" || got.Name != "이름" {
		t.Fatalf("identity survived as %#v err=%v", got, err)
	}

	empty := Message{Type: TypeState, Flags: FlagSnapshot | FlagLast, Sequence: 1,
		Device: "00:11:22:33:44:55", Code: NoCallState, Payload: payload}
	if _, err := empty.Encode(); err == nil {
		t.Fatal("an empty snapshot must not carry an identity")
	}
}
