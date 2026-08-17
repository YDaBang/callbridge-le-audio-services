package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"callbridge.local/callbridge-sms-go/internal/model"
)

func TestLegacyImportAndAtMostOnceOutbound(t *testing.T) {
	directory := t.TempDir()
	seen := filepath.Join(directory, "seen.json")
	messageLog := filepath.Join(directory, "messages.jsonl")
	forward := filepath.Join(directory, "forward.state")
	outbound := filepath.Join(directory, "out.jsonl")
	mustWrite(t, seen, `{"version":1,"handles":["AA","BB"]}`+"\n", 0o600)
	mustWrite(t, messageLog, `{"event":{"handle":"CC"}}`+"\n", 0o600)
	mustWrite(t, forward, "DD\n", 0o600)
	mustWrite(t, outbound, `{"To":"01000000000","Text":"one shot"}`+"\n", 0o600)
	path := filepath.Join(directory, "state.json")
	store, imported, err := Open(path, LegacyPaths{seen, messageLog, forward, outbound})
	if err != nil || !imported || !store.IsInitialized() {
		t.Fatalf("Open() imported=%t initialized=%t err=%v", imported, store.IsInitialized(), err)
	}
	for _, handle := range []string{"AA", "BB", "CC", "DD"} {
		if !store.HasSeen(handle) {
			t.Fatalf("missing imported handle %s", handle)
		}
	}
	item, ok, err := store.TakeOutbound()
	if err != nil || !ok || item.Text != "one shot" {
		t.Fatalf("TakeOutbound()=%#v,%t,%v", item, ok, err)
	}
	reopened, _, err := Open(path, LegacyPaths{})
	if err != nil {
		t.Fatal(err)
	}
	_, depth := reopened.Counts()
	if depth != 0 {
		t.Fatalf("consumed outbound item reappeared; depth=%d", depth)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestStorePersistsSeenAndBoundsOutbound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, _, err := Open(path, LegacyPaths{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSeen("A1B2"); err != nil {
		t.Fatal(err)
	}
	valid := model.Outbound{ID: "id-1", To: "+821000000000", Text: "hello", QueuedAt: time.Now()}
	if err := store.AddOutbound(valid); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.ID = "id-2"
	invalid.Text = string(make([]byte, (16<<10)+1))
	if err := store.AddOutbound(invalid); err == nil {
		t.Fatal("accepted oversized outbound text")
	}
	reopened, _, err := Open(path, LegacyPaths{})
	if err != nil || !reopened.HasSeen("A1B2") {
		t.Fatalf("seen handle did not persist: %v", err)
	}
}

func TestStoreRejectsInsecureExistingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	mustWrite(t, path, `{"version":1,"initialized":true,"seen_handles":[],"outbound":[]}`+"\n", 0o644)
	if _, _, err := Open(path, LegacyPaths{}); err == nil {
		t.Fatal("accepted group/world-readable state")
	}
}

func mustWrite(t *testing.T, path, value string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}
