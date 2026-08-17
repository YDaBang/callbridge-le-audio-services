package bluez

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/godbus/dbus/v5"
)

type fakeSinkNudgeRegistrar struct {
	calls             []string
	path              dbus.ObjectPath
	properties        map[string]dbus.Variant
	registerErr       error
	unregisterErr     error
	afterRegister     func()
	unregisterContext error
}

func (f *fakeSinkNudgeRegistrar) Register(_ context.Context, path dbus.ObjectPath, properties map[string]dbus.Variant) error {
	f.calls = append(f.calls, "register")
	f.path = path
	f.properties = properties
	if f.afterRegister != nil {
		f.afterRegister()
	}
	return f.registerErr
}

func (f *fakeSinkNudgeRegistrar) Unregister(ctx context.Context, path dbus.ObjectPath) error {
	f.calls = append(f.calls, "unregister")
	f.path = path
	f.unregisterContext = ctx.Err()
	return f.unregisterErr
}

func TestPerformSinkNudgeRegistersThenImmediatelyRemovesSink(t *testing.T) {
	registrar := &fakeSinkNudgeRegistrar{}
	if err := performSinkNudge(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(registrar.calls, []string{"register", "unregister"}) {
		t.Fatalf("calls=%v", registrar.calls)
	}
	if registrar.path != sinkNudgeEndpointPath {
		t.Fatalf("path=%q", registrar.path)
	}
	if got := registrar.properties["UUID"].Value(); got != PACSinkUUID {
		t.Fatalf("UUID=%#v", got)
	}
	if got := registrar.properties["Context"].Value(); got != MediaContext {
		t.Fatalf("Context=%#v", got)
	}
	if got := registrar.properties["SupportedContext"].Value(); got != MediaContext {
		t.Fatalf("SupportedContext=%#v", got)
	}
}

func TestSinkNudgeForcesContextChangeBackToRealEndpoints(t *testing.T) {
	if MediaContext&SupportedContexts != 0 {
		t.Fatalf("temporary context %#04x overlaps real contexts %#04x", MediaContext, SupportedContexts)
	}
	transient := MediaContext | SupportedContexts
	if transient != 0x0007 {
		t.Fatalf("transient aggregate=%#04x", transient)
	}
	if final := transient &^ MediaContext; final != SupportedContexts {
		t.Fatalf("final aggregate=%#04x", final)
	}
}

func TestSinkNudgeEndpointPathIsValid(t *testing.T) {
	if !sinkNudgeEndpointPath.IsValid() {
		t.Fatalf("invalid D-Bus object path %q", sinkNudgeEndpointPath)
	}
	if sinkNudgeEndpointPath != "/callbridge/leaudio/sink_nudge" {
		t.Fatalf("unexpected D-Bus object path %q", sinkNudgeEndpointPath)
	}
}

func TestPerformSinkNudgeDoesNotUnregisterAfterRegistrationFailure(t *testing.T) {
	want := errors.New("register failed")
	registrar := &fakeSinkNudgeRegistrar{registerErr: want}
	err := performSinkNudge(context.Background(), registrar)
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(registrar.calls, []string{"register"}) {
		t.Fatalf("calls=%v", registrar.calls)
	}
}

func TestPerformSinkNudgeFinishesUnregisterAfterParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	registrar := &fakeSinkNudgeRegistrar{afterRegister: cancel}
	if err := performSinkNudge(ctx, registrar); err != nil {
		t.Fatal(err)
	}
	if registrar.unregisterContext != nil {
		t.Fatalf("unregister context=%v", registrar.unregisterContext)
	}
}

func TestPerformSinkNudgeReportsUnregisterFailure(t *testing.T) {
	want := errors.New("unregister failed")
	registrar := &fakeSinkNudgeRegistrar{unregisterErr: want}
	err := performSinkNudge(context.Background(), registrar)
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(registrar.calls, []string{"register", "unregister"}) {
		t.Fatalf("calls=%v", registrar.calls)
	}
}

func TestSinkNudgeSelectorValidation(t *testing.T) {
	for _, adapter := range []string{"hci0", "hci12"} {
		if !validAdapterName(adapter) {
			t.Fatalf("rejected adapter %q", adapter)
		}
	}
	for _, adapter := range []string{"", "hci", "hci-1", "hci0/other"} {
		if validAdapterName(adapter) {
			t.Fatalf("accepted adapter %q", adapter)
		}
	}
	for _, address := range []string{"00:11:22:33:44:55", "aa:BB:cc:DD:ee:FF"} {
		if !validBluetoothAddress(address) {
			t.Fatalf("rejected address %q", address)
		}
	}
	for _, address := range []string{"", "00:11:22:33:44", "00-11-22-33-44-55", "GG:11:22:33:44:55"} {
		if validBluetoothAddress(address) {
			t.Fatalf("accepted address %q", address)
		}
	}
}

func TestSinkNudgeEndpointRejectsMediaConfiguration(t *testing.T) {
	endpoint := &sinkNudgeEndpoint{}
	if _, err := endpoint.SelectConfiguration(nil); err == nil || err.Name != "org.bluez.Error.NotSupported" {
		t.Fatalf("SelectConfiguration error=%v", err)
	}
	if _, err := endpoint.SelectProperties(nil); err == nil || err.Name != "org.bluez.Error.NotSupported" {
		t.Fatalf("SelectProperties error=%v", err)
	}
	if err := endpoint.SetConfiguration("/transport", nil); err == nil || err.Name != "org.bluez.Error.NotSupported" {
		t.Fatalf("SetConfiguration error=%v", err)
	}
}
