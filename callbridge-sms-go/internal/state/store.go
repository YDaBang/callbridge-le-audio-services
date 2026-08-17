package state

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"callbridge.local/callbridge-sms-go/internal/model"
)

const (
	stateVersion       = 1
	maximumSeenHandles = 4096
	maximumOutbound    = 128
	maximumLegacyBytes = 32 << 20
)

type LegacyPaths struct {
	SeenFile      string
	MessageLog    string
	ForwardState  string
	OutboundQueue string
}

type diskState struct {
	Version     int              `json:"version"`
	Initialized bool             `json:"initialized"`
	SeenHandles []string         `json:"seen_handles"`
	Outbound    []model.Outbound `json:"outbound"`
}

type Store struct {
	mu   sync.Mutex
	path string
	data diskState
	seen map[string]struct{}
}

func Open(path string, legacy LegacyPaths) (*Store, bool, error) {
	store := &Store{path: path, seen: make(map[string]struct{})}
	if err := store.load(); err == nil {
		return store, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, err
	}
	imported, err := importLegacy(legacy)
	if err != nil {
		return nil, false, err
	}
	store.data = imported
	store.rebuildSeen()
	if err := store.saveLocked(); err != nil {
		return nil, false, err
	}
	return store, true, nil
}

func (s *Store) IsInitialized() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Initialized
}

func (s *Store) SetInitialized() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Initialized {
		return nil
	}
	s.data.Initialized = true
	return s.saveLocked()
}

func (s *Store) HasSeen(handle string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.seen[handle]
	return ok
}

func (s *Store) MarkSeen(handle string) error {
	return s.MarkSeenMany([]string{handle})
}

func (s *Store) MarkSeenMany(handles []string) error {
	if len(handles) == 0 {
		return nil
	}
	for _, handle := range handles {
		if !validHandle(handle) {
			return errors.New("invalid message handle")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	original := append([]string(nil), s.data.SeenHandles...)
	originalSeen := s.seen
	s.seen = make(map[string]struct{}, len(originalSeen)+len(handles))
	for handle := range originalSeen {
		s.seen[handle] = struct{}{}
	}
	changed := false
	for _, handle := range handles {
		if _, ok := s.seen[handle]; ok {
			continue
		}
		s.data.SeenHandles = append(s.data.SeenHandles, handle)
		s.seen[handle] = struct{}{}
		changed = true
	}
	if !changed {
		return nil
	}
	if extra := len(s.data.SeenHandles) - maximumSeenHandles; extra > 0 {
		for _, old := range s.data.SeenHandles[:extra] {
			delete(s.seen, old)
		}
		s.data.SeenHandles = append([]string(nil), s.data.SeenHandles[extra:]...)
	}
	if err := s.saveLocked(); err != nil {
		s.data.SeenHandles = original
		s.seen = originalSeen
		return err
	}
	return nil
}

func (s *Store) AddOutbound(item model.Outbound) error {
	if !validOutbound(item) {
		return errors.New("invalid outbound item")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data.Outbound) >= maximumOutbound {
		return errors.New("outbound queue is full")
	}
	for _, existing := range s.data.Outbound {
		if existing.ID == item.ID {
			return nil
		}
	}
	s.data.Outbound = append(s.data.Outbound, item)
	if err := s.saveLocked(); err != nil {
		s.data.Outbound = s.data.Outbound[:len(s.data.Outbound)-1]
		return err
	}
	return nil
}

// TakeOutbound persists removal before returning the item. The caller must
// attempt it at most once and must not reinsert it after an ambiguous failure.
func (s *Store) TakeOutbound() (model.Outbound, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data.Outbound) == 0 {
		return model.Outbound{}, false, nil
	}
	item := s.data.Outbound[0]
	original := s.data.Outbound
	s.data.Outbound = append([]model.Outbound(nil), original[1:]...)
	if err := s.saveLocked(); err != nil {
		s.data.Outbound = original
		return model.Outbound{}, false, err
	}
	return item, true, nil
}

func (s *Store) Counts() (seen, outbound int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen), len(s.data.Outbound)
}

func (s *Store) load() error {
	file, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > maximumLegacyBytes {
		return errors.New("state file exceeds bound")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("state file permissions invalid")
	}
	decoder := json.NewDecoder(io.LimitReader(file, maximumLegacyBytes+1))
	if err := decoder.Decode(&s.data); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("state file has trailing data")
	}
	if s.data.Version != stateVersion || len(s.data.SeenHandles) > maximumSeenHandles || len(s.data.Outbound) > maximumOutbound {
		return errors.New("state schema or bounds invalid")
	}
	for _, item := range s.data.Outbound {
		if !validOutbound(item) {
			return errors.New("state contains invalid outbound item")
		}
	}
	s.rebuildSeen()
	return nil
}

func (s *Store) rebuildSeen() {
	s.seen = make(map[string]struct{}, len(s.data.SeenHandles))
	clean := s.data.SeenHandles[:0]
	for _, handle := range s.data.SeenHandles {
		if !validHandle(handle) {
			continue
		}
		if _, exists := s.seen[handle]; exists {
			continue
		}
		s.seen[handle] = struct{}{}
		clean = append(clean, handle)
	}
	s.data.SeenHandles = clean
}

func (s *Store) saveLocked() error {
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, "."+filepath.Base(s.path)+".tmp.")
	if err != nil {
		return err
	}
	temporary := file.Name()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(temporary)
		return err
	}
	cleanup := true
	defer func() {
		_ = file.Close()
		if cleanup {
			_ = os.Remove(temporary)
		}
	}()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(s.data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, s.path); err != nil {
		return err
	}
	cleanup = false
	if directoryFD, err := os.Open(directory); err == nil {
		_ = directoryFD.Sync()
		_ = directoryFD.Close()
	}
	return nil
}

func importLegacy(paths LegacyPaths) (diskState, error) {
	data := diskState{Version: stateVersion}
	seen := make(map[string]struct{})
	seenOrder := make([]string, 0, maximumSeenHandles)
	legacyExists := false
	add := func(handle string) {
		if !validHandle(handle) {
			return
		}
		if _, exists := seen[handle]; exists {
			return
		}
		seen[handle] = struct{}{}
		seenOrder = append(seenOrder, handle)
		if len(seenOrder) > maximumSeenHandles {
			delete(seen, seenOrder[0])
			seenOrder = seenOrder[1:]
		}
	}
	if raw, ok, err := readBounded(paths.SeenFile); err != nil {
		return data, err
	} else if ok {
		legacyExists = true
		var envelope struct {
			Handles []string `json:"handles"`
		}
		if json.Unmarshal(raw, &envelope) == nil {
			for _, handle := range envelope.Handles {
				add(handle)
			}
		} else {
			var values []string
			if json.Unmarshal(raw, &values) == nil {
				for _, handle := range values {
					add(handle)
				}
			}
		}
	}
	if file, err := os.Open(paths.MessageLog); err == nil {
		legacyExists = true
		scanner := bufio.NewScanner(io.LimitReader(file, maximumLegacyBytes))
		scanner.Buffer(make([]byte, 4096), 2<<20)
		for scanner.Scan() {
			var row struct {
				Event struct {
					Handle string `json:"handle"`
				} `json:"event"`
			}
			if json.Unmarshal(scanner.Bytes(), &row) == nil {
				add(row.Event.Handle)
			}
		}
		_ = file.Close()
		if err := scanner.Err(); err != nil {
			return data, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return data, err
	}
	if raw, ok, err := readBounded(paths.ForwardState); err != nil {
		return data, err
	} else if ok {
		legacyExists = true
		add(strings.TrimSpace(string(raw)))
	}
	data.SeenHandles = append(data.SeenHandles, seenOrder...)
	if file, err := os.Open(paths.OutboundQueue); err == nil {
		legacyExists = true
		scanner := bufio.NewScanner(io.LimitReader(file, maximumLegacyBytes))
		for index := 0; scanner.Scan() && len(data.Outbound) < maximumOutbound; index++ {
			var row struct{ To, Text string }
			if json.Unmarshal(scanner.Bytes(), &row) != nil {
				continue
			}
			digest := sha256.Sum256(append(scanner.Bytes(), byte(index)))
			item := model.Outbound{
				ID: "legacy-" + hex.EncodeToString(digest[:8]), To: row.To, Text: row.Text, QueuedAt: time.Now().UTC(),
			}
			if validOutbound(item) {
				data.Outbound = append(data.Outbound, item)
			}
		}
		_ = file.Close()
		if err := scanner.Err(); err != nil {
			return data, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return data, err
	}
	data.Initialized = legacyExists
	return data, nil
}

func readBounded(path string) ([]byte, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximumLegacyBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(raw) > maximumLegacyBytes {
		return nil, false, errors.New("legacy state exceeds bound")
	}
	return raw, true, nil
}

func validHandle(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

func validOutbound(item model.Outbound) bool {
	if item.ID == "" || len(item.ID) > 128 || strings.ContainsAny(item.ID, "\x00\r\n") {
		return false
	}
	if item.Text == "" || len([]byte(item.Text)) > 16<<10 {
		return false
	}
	digits := strings.TrimPrefix(item.To, "+")
	if len(digits) < 3 || len(digits) > 20 {
		return false
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return false
		}
	}
	return item.To == digits || item.To == "+"+digits
}
