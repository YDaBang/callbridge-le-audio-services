package gtbs

import (
	"errors"
	"testing"

	"callbridge.local/callbridge-sms-go/lecall/internal/protocol"
	"github.com/godbus/dbus/v5"
)

func TestClassifyCommandError(t *testing.T) {
	cases := []struct {
		err  error
		want byte
	}{
		{nil, protocol.AckAccepted},
		{ErrStale, protocol.AckStale},
		{ErrUnavailable, protocol.AckUnavailable},
		{ErrInvalid, protocol.AckInvalid},
		{errors.New("dbus"), protocol.AckWriteFailed},
	}
	for _, test := range cases {
		if got := ClassifyCommandError(test.err); got != test.want {
			t.Fatalf("error=%v got=%d want=%d", test.err, got, test.want)
		}
	}
}

func TestRedactedErrorHidesOriginateURI(t *testing.T) {
	if got := RedactedError(errors.New("write tel:+821000000000 failed")); got != "GTBS command failed with redacted URI" {
		t.Fatalf("redaction=%q", got)
	}
}

func TestOriginateResultWithUnassignedIndexIsDeliveredOnce(t *testing.T) {
	store := NewStore()
	var opcode, index, result byte
	var token uint64
	client := &Client{store: store, pending: make(map[[2]byte]uint64),
		pendingOriginate: true, cbs: Callbacks{Result: func(gotOpcode, gotIndex,
			gotResult byte, gotToken uint64) {
			opcode, index, result, token = gotOpcode, gotIndex, gotResult, gotToken
		}}}
	if err := client.handleControlResult([]byte{protocol.OpcodeOriginate, 0, 6}); err != nil {
		t.Fatal(err)
	}
	if opcode != protocol.OpcodeOriginate || index != 0 || result != 6 || token != 0 {
		t.Fatalf("result opcode=%d index=%d result=%d token=%d", opcode, index, result, token)
	}
	if err := client.handleControlResult([]byte{protocol.OpcodeOriginate, 0, 6}); err == nil {
		t.Fatal("accepted duplicate originate result")
	}
}

func TestLEBearerDisconnectInvalidatesGTBSSession(t *testing.T) {
	path := dbus.ObjectPath("/org/bluez/hci0/dev_00_11_22_33_44_55")
	client := &Client{path: path}
	inventory := Inventory{DevicePath: path}
	signal := &dbus.Signal{
		Name: PropertiesIface + ".PropertiesChanged",
		Path: path,
		Body: []interface{}{
			leBearerIface,
			map[string]dbus.Variant{"Connected": dbus.MakeVariant(false)},
			[]string{},
		},
	}
	if err := client.handleSignal(signal, inventory); !errors.Is(err, errLEDisconnected) {
		t.Fatalf("disconnect error=%v", err)
	}
	signal.Body[1] = map[string]dbus.Variant{"Connected": dbus.MakeVariant(true)}
	if err := client.handleSignal(signal, inventory); err != nil {
		t.Fatalf("connected signal error=%v", err)
	}
	signal.Path = dbus.ObjectPath(string(path) + "/service005a")
	signal.Body[1] = map[string]dbus.Variant{"Connected": dbus.MakeVariant(false)}
	if err := client.handleSignal(signal, inventory); err != nil {
		t.Fatalf("child path changed GTBS readiness: %v", err)
	}
}
