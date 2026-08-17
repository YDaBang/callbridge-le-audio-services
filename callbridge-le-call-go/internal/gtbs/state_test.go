package gtbs

import (
	"testing"

	"callbridge.local/callbridge-sms-go/lecall/internal/protocol"
)

func TestStorePreservesTokenAcrossStatesAndRotatesAfterRemoval(t *testing.T) {
	store := NewStore()
	initial := store.Snapshot()
	first, err := store.Apply([]byte{7, StateIncoming, 0})
	if err != nil || len(first.Calls) != 1 || first.Calls[0].Token == 0 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if initial.Sequence == 0 || first.Sequence <= initial.Sequence {
		t.Fatalf("snapshot sequence did not advance: initial=%d first=%d",
			initial.Sequence, first.Sequence)
	}
	token := first.Calls[0].Token
	active, err := store.Apply([]byte{7, StateActive, 0})
	if err != nil || active.Calls[0].Token != token {
		t.Fatalf("active=%#v err=%v", active, err)
	}
	if !store.ValidateIndexedCommand(7, token, protocol.OpcodeTerminate) ||
		store.ValidateIndexedCommand(7, token, protocol.OpcodeAccept) {
		t.Fatal("active call command validation is wrong")
	}
	if _, err := store.Apply(nil); err != nil {
		t.Fatal(err)
	}
	second, err := store.Apply([]byte{7, StateIncoming, 0})
	if err != nil || second.Calls[0].Token == token {
		t.Fatalf("reused lifecycle token: %#v err=%v", second, err)
	}
}

func TestStoreRefusesAmbiguousMediaCorrelation(t *testing.T) {
	store := NewStore()
	if _, err := store.Apply([]byte{1, StateActive, 0, 2, StateIncoming, 0}); err != nil {
		t.Fatal(err)
	}
	if token, ok := store.CurrentCallToken(); ok || token != 0 {
		t.Fatalf("ambiguous token=%d ok=%v", token, ok)
	}
}

func TestParseCallStateRejectsMalformedEntries(t *testing.T) {
	for _, value := range [][]byte{
		{1},
		{0, StateIncoming, 0},
		{1, 7, 0},
		{1, StateIncoming, 8},
		{1, StateIncoming, 0, 1, StateActive, 0},
	} {
		if _, err := ParseCallState(value); err == nil {
			t.Fatalf("accepted %x", value)
		}
	}
}
