package gtbs

import (
	"testing"

	"github.com/godbus/dbus/v5"
)

func TestDiscoverRequiresWritableNotifiableGTBSControl(t *testing.T) {
	device := dbus.ObjectPath("/org/bluez/hci0/dev_6C_AC_C2_0D_40_88")
	service := dbus.ObjectPath(string(device) + "/service001")
	callState := dbus.ObjectPath(string(service) + "/char001")
	control := dbus.ObjectPath(string(service) + "/char002")
	objects := ManagedObjects{
		device: {DeviceIface: {}},
		service: {GattServiceIface: {
			"UUID": dbus.MakeVariant(UUIDGTBS), "Device": dbus.MakeVariant(device),
		}},
		callState: {GattCharacteristicIF: {
			"UUID": dbus.MakeVariant(UUIDCallState), "Service": dbus.MakeVariant(service),
			"Flags": dbus.MakeVariant([]string{"read", "notify"}),
		}},
		control: {GattCharacteristicIF: {
			"UUID": dbus.MakeVariant(UUIDControlPoint), "Service": dbus.MakeVariant(service),
			"Flags": dbus.MakeVariant([]string{"write", "notify"}),
		}},
	}
	inventory, err := Discover(objects, device)
	if err != nil || inventory.ServicePath != service {
		t.Fatalf("inventory=%#v err=%v", inventory, err)
	}
	delete(objects, control)
	if _, err := Discover(objects, device); err == nil {
		t.Fatal("accepted GTBS without control point")
	}
}

func TestDevicePath(t *testing.T) {
	path, err := DevicePath("hci0", "00:11:22:33:44:55")
	if err != nil || path != "/org/bluez/hci0/dev_6C_AC_C2_0D_40_88" {
		t.Fatalf("path=%q err=%v", path, err)
	}
}

func TestResolveDevicePathUsesStableAddressProperty(t *testing.T) {
	want := dbus.ObjectPath("/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2")
	objects := ManagedObjects{
		want: {DeviceIface: {
			"Address": dbus.MakeVariant("00:11:22:33:44:55"),
			"Adapter": dbus.MakeVariant(dbus.ObjectPath("/org/bluez/hci0")),
		}},
	}
	got, err := ResolveDevicePath(objects, "hci0", "00:11:22:33:44:55")
	if err != nil || got != want {
		t.Fatalf("path=%q err=%v", got, err)
	}
}

func TestResolveDevicePathRejectsAmbiguousIdentity(t *testing.T) {
	properties := map[string]dbus.Variant{
		"Address": dbus.MakeVariant("00:11:22:33:44:55"),
		"Adapter": dbus.MakeVariant(dbus.ObjectPath("/org/bluez/hci0")),
	}
	objects := ManagedObjects{
		"/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2": {DeviceIface: properties},
		"/org/bluez/hci0/dev_6C_AC_C2_0D_40_88": {DeviceIface: properties},
	}
	if _, err := ResolveDevicePath(objects, "hci0", "00:11:22:33:44:55"); err == nil {
		t.Fatal("accepted ambiguous BlueZ identity")
	}
}
