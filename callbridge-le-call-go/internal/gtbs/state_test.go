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

func TestStoreRefusesMediaForAHandsetAnsweredCall(t *testing.T) {
	store := NewStore()
	ringing, err := store.Apply([]byte{4, StateIncoming, 0})
	if err != nil {
		t.Fatal(err)
	}
	token := ringing.Calls[0].Token
	if got, ok := store.CurrentCallToken(); !ok || got != token {
		t.Fatalf("a ringing call must still correlate: token=%d ok=%v", got, ok)
	}

	// Active with no Accept from this side: the handset answered.
	if _, err := store.Apply([]byte{4, StateActive, 0}); err != nil {
		t.Fatal(err)
	}
	if got, ok := store.CurrentCallToken(); ok || got != 0 {
		t.Fatalf("handset answer must refuse media: token=%d ok=%v", got, ok)
	}

	// The same call once this side accepts it.
	store.ClaimCall(4, token)
	if got, ok := store.CurrentCallToken(); !ok || got != token {
		t.Fatalf("accepted call must correlate: token=%d ok=%v", got, ok)
	}

	// A claim cannot survive the call going away and the index returning.
	if _, err := store.Apply(nil); err != nil {
		t.Fatal(err)
	}
	next, err := store.Apply([]byte{4, StateActive, 0})
	if err != nil {
		t.Fatal(err)
	}
	if next.Calls[0].Token == token {
		t.Fatal("lifecycle token was reused")
	}
	if got, ok := store.CurrentCallToken(); ok || got != 0 {
		t.Fatalf("stale claim leaked to a new call: token=%d ok=%v", got, ok)
	}
}

func TestHandsetAnsweredReportsOnlyAnUnclaimedIncomingCall(t *testing.T) {
	store := NewStore()
	ringing, err := store.Apply([]byte{3, StateIncoming, 0})
	if err != nil {
		t.Fatal(err)
	}
	token := ringing.Calls[0].Token
	if got := store.HandsetAnswered(); got != 0 {
		t.Fatalf("a ringing call is not answered yet: %d", got)
	}
	if _, err := store.Apply([]byte{3, StateActive, 0}); err != nil {
		t.Fatal(err)
	}
	if got := store.HandsetAnswered(); got != token {
		t.Fatalf("unclaimed active call: got=%d want=%d", got, token)
	}
	store.ClaimCall(3, token)
	if got := store.HandsetAnswered(); got != 0 {
		t.Fatalf("an accepted call must not look like a handset answer: %d", got)
	}

	// Outgoing calls reach Active without any Accept and must never match.
	outgoing := NewStore()
	if _, err := outgoing.Apply([]byte{5, StateActive, CallFlagOutgoing}); err != nil {
		t.Fatal(err)
	}
	if got := outgoing.HandsetAnswered(); got != 0 {
		t.Fatalf("outgoing call reported as a handset answer: %d", got)
	}
}

func TestStoreCorrelatesOutgoingCallsWithoutAClaim(t *testing.T) {
	store := NewStore()
	dialing, err := store.Apply([]byte{9, StateDialing, CallFlagOutgoing})
	if err != nil {
		t.Fatal(err)
	}
	token := dialing.Calls[0].Token
	for _, state := range []byte{StateAlerting, StateActive} {
		if _, err := store.Apply([]byte{9, state, CallFlagOutgoing}); err != nil {
			t.Fatal(err)
		}
		if got, ok := store.CurrentCallToken(); !ok || got != token {
			t.Fatalf("outgoing state=%d token=%d ok=%v", state, got, ok)
		}
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
