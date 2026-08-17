package gtbs

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"callbridge.local/callbridge-sms-go/lecall/internal/protocol"
)

const (
	StateIncoming             = byte(0x00)
	StateDialing              = byte(0x01)
	StateAlerting             = byte(0x02)
	StateActive               = byte(0x03)
	StateLocallyHeld          = byte(0x04)
	StateRemotelyHeld         = byte(0x05)
	StateLocallyRemotelyHeld  = byte(0x06)
	CallFlagOutgoing          = byte(0x01)
	CallFlagWithheldByServer  = byte(0x02)
	CallFlagWithheldByNetwork = byte(0x04)
)

type Call struct {
	Index    byte
	State    byte
	Flags    byte
	Token    uint64
	Identity protocol.Identity
}

type Snapshot struct {
	Sequence uint64
	Calls    []Call
}

type Store struct {
	mu         sync.RWMutex
	calls      map[byte]Call
	claimed    map[byte]uint64
	identities map[byte]protocol.Identity
	sequence   uint64
	seed       uint64
	tokens     atomic.Uint64
}

// errNoCallIdentity means the characteristic held nothing to attribute, which
// is what a phone with no call in progress answers a read with.  It is a
// routine state, not a malformed value.
var errNoCallIdentity = errors.New("no GTBS call identity")

// errIdentityUnchanged means the value repeated what is already stored, so
// there is no new snapshot to emit. Re-sending one would reuse the sequence
// number and the peer would discard it as stale.
var errIdentityUnchanged = errors.New("GTBS call identity unchanged")

// ParseCallIdentifier splits the [call index][value] shape GTBS Incoming Call
// (0x2BC1) uses.
func ParseCallIdentifier(value []byte) (byte, string, error) {
	if len(value) == 0 || value[0] == 0 {
		return 0, "", errNoCallIdentity
	}
	return value[0], string(value[1:]), nil
}

// StripURIScheme reduces a GTBS caller URI to the part worth showing.
//
// The phone advertises which schemes it supports, so "tel:" is not the only
// possibility and the scheme has to be found rather than assumed.  Anything
// that is not a plausible dialable string is dropped: a caller ID is not worth
// rendering arbitrary text into a SIP header.
func StripURIScheme(uri string) string {
	trimmed := strings.TrimSpace(uri)
	if index := strings.IndexByte(trimmed, ':'); index >= 0 {
		trimmed = trimmed[index+1:]
	}
	if index := strings.IndexAny(trimmed, ";?"); index >= 0 {
		trimmed = trimmed[:index]
	}
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" || len(trimmed) > protocol.IdentityURIMax {
		return ""
	}
	for i := 0; i < len(trimmed); i++ {
		switch c := trimmed[i]; {
		case c >= '0' && c <= '9', c == '+', c == '*', c == '#', c == '-':
		default:
			return ""
		}
	}
	return trimmed
}

func NewStore() *Store {
	seed := uint64(time.Now().UnixNano())
	if seed == 0 {
		seed = 1
	}
	store := &Store{calls: make(map[byte]Call), claimed: make(map[byte]uint64),
		identities: make(map[byte]protocol.Identity), sequence: seed, seed: seed}
	store.tokens.Store(seed)
	return store
}

func ParseCallState(value []byte) ([]Call, error) {
	if len(value)%3 != 0 || len(value) > 3*255 {
		return nil, errors.New("invalid GTBS Call State length")
	}
	calls := make([]Call, 0, len(value)/3)
	seen := make(map[byte]bool)
	for offset := 0; offset < len(value); offset += 3 {
		call := Call{Index: value[offset], State: value[offset+1], Flags: value[offset+2]}
		if call.Index == 0 || call.State > StateLocallyRemotelyHeld ||
			call.Flags&^(CallFlagOutgoing|CallFlagWithheldByServer|CallFlagWithheldByNetwork) != 0 ||
			seen[call.Index] {
			return nil, errors.New("invalid GTBS Call State entry")
		}
		seen[call.Index] = true
		calls = append(calls, call)
	}
	sort.Slice(calls, func(i, j int) bool { return calls[i].Index < calls[j].Index })
	return calls, nil
}

func (s *Store) Apply(value []byte) (Snapshot, error) {
	parsed, err := ParseCallState(value)
	if err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[byte]Call, len(parsed))
	for _, call := range parsed {
		if prior, present := s.calls[call.Index]; present {
			call.Token = prior.Token
		} else {
			call.Token = s.nextToken()
		}
		next[call.Index] = call
	}
	s.calls = next
	for index, token := range s.claimed {
		if call, present := next[index]; !present || call.Token != token {
			delete(s.claimed, index)
		}
	}
	// A caller identity belongs to one call. Leaving it behind would show the
	// previous caller's name on whatever call reuses the index.
	for index := range s.identities {
		if _, present := next[index]; !present {
			delete(s.identities, index)
		}
	}
	s.sequence++
	if s.sequence == 0 {
		s.sequence = 1
	}
	return snapshotLocked(s.sequence, s.calls, s.identities), nil
}

// ApplyIncomingCall records the caller URI reported for one call.
func (s *Store) ApplyIncomingCall(value []byte) error {
	index, raw, err := ParseCallIdentifier(value)
	if err != nil {
		return err
	}
	uri := StripURIScheme(raw)
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := s.identities[index]
	identity.URI = uri
	if !s.setIdentityLocked(index, identity) {
		return errIdentityUnchanged
	}
	return nil
}

// The caller holds s.mu.
//
// Returns whether anything changed, so an unchanged identity does not spend a
// sequence number. The peer treats a repeated sequence as a stale snapshot and
// discards it, so every snapshot this side emits has to be a new one.
func (s *Store) setIdentityLocked(index byte, identity protocol.Identity) bool {
	previous, had := s.identities[index]
	if identity.URI == "" && identity.Name == "" {
		if !had {
			return false
		}
		delete(s.identities, index)
	} else {
		if had && previous == identity {
			return false
		}
		s.identities[index] = identity
	}
	s.sequence++
	if s.sequence == 0 {
		s.sequence = 1
	}
	return true
}

// Identity reports what is known about one call's caller: the number, or
// nothing when it is withheld. The name is deliberately left empty -- the
// softphone syncs the same contacts and resolves it locally.
func (s *Store) Identity(index byte) protocol.Identity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.identities[index]
}

// HandsetAnswered reports the token of a call the phone answered by itself: an
// incoming call that left the Incoming state without an Accept from this side.
// Returns 0 when there is no such call.
func (s *Store) HandsetAnswered() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, call := range s.calls {
		if call.Token == 0 || call.Flags&CallFlagOutgoing != 0 ||
			call.State == StateIncoming {
			continue
		}
		if s.claimed[call.Index] != call.Token {
			return call.Token
		}
	}
	return 0
}

// ClaimCall records that this side asked for the call, so the media path may
// carry it. Only an Accept we sent counts; the phone answering on its own does
// not, and neither does a stale index whose token has since been reissued.
func (s *Store) ClaimCall(index byte, token uint64) {
	if token == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if call, present := s.calls[index]; present && call.Token == token {
		s.claimed[index] = token
	}
}

func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return snapshotLocked(s.sequence, s.calls, s.identities)
}

func snapshotLocked(sequence uint64, calls map[byte]Call,
	identities map[byte]protocol.Identity) Snapshot {
	snapshot := Snapshot{Sequence: sequence, Calls: make([]Call, 0, len(calls))}
	for _, call := range calls {
		call.Identity = identities[call.Index]
		snapshot.Calls = append(snapshot.Calls, call)
	}
	sort.Slice(snapshot.Calls, func(i, j int) bool { return snapshot.Calls[i].Index < snapshot.Calls[j].Index })
	return snapshot
}

func (s *Store) CurrentCallToken() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.calls) != 1 {
		return 0, false
	}
	for _, call := range s.calls {
		if call.Token == 0 {
			return 0, false
		}
		// An incoming call that reached a connected state without an Accept
		// from this side was answered on the handset. The phone routes call
		// audio to whichever LE Audio device accepts a stream, so refusing
		// the correlation here keeps the media broker from acquiring the
		// transport and leaves the audio where the user answered it.
		//
		// Outgoing calls are untouched: this side originated them.
		if call.Flags&CallFlagOutgoing == 0 && call.State != StateIncoming {
			if s.claimed[call.Index] != call.Token {
				return 0, false
			}
		}
		return call.Token, true
	}
	return 0, false
}

func (s *Store) ValidateIndexedCommand(index byte, token uint64, opcode byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	call, present := s.calls[index]
	if !present || token == 0 || call.Token != token {
		return false
	}
	switch opcode {
	case protocol.OpcodeAccept:
		return call.State == StateIncoming
	case protocol.OpcodeTerminate:
		return true
	case protocol.OpcodeLocalHold:
		return call.State == StateIncoming || call.State == StateActive || call.State == StateRemotelyHeld
	case protocol.OpcodeLocalRetrieve:
		return call.State == StateLocallyHeld || call.State == StateLocallyRemotelyHeld
	default:
		return false
	}
}

func (s *Store) TokenForIndex(index byte) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	call, present := s.calls[index]
	return call.Token, present && call.Token != 0
}

func (s *Store) nextToken() uint64 {
	for {
		token := s.tokens.Add(1)
		if token != 0 && token != s.seed {
			return token
		}
	}
}
