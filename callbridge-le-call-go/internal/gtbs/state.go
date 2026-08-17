package gtbs

import (
	"errors"
	"sort"
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
	Index byte
	State byte
	Flags byte
	Token uint64
}

type Snapshot struct {
	Sequence uint64
	Calls    []Call
}

type Store struct {
	mu       sync.RWMutex
	calls    map[byte]Call
	sequence uint64
	seed     uint64
	tokens   atomic.Uint64
}

func NewStore() *Store {
	seed := uint64(time.Now().UnixNano())
	if seed == 0 {
		seed = 1
	}
	store := &Store{calls: make(map[byte]Call), sequence: seed, seed: seed}
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
	s.sequence++
	if s.sequence == 0 {
		s.sequence = 1
	}
	return snapshotLocked(s.sequence, s.calls), nil
}

func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return snapshotLocked(s.sequence, s.calls)
}

func snapshotLocked(sequence uint64, calls map[byte]Call) Snapshot {
	snapshot := Snapshot{Sequence: sequence, Calls: make([]Call, 0, len(calls))}
	for _, call := range calls {
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
		return call.Token, call.Token != 0
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
