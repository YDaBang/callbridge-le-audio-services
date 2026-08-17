package gtbs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/godbus/dbus/v5"
)

const (
	BlueZBusName         = "org.bluez"
	ObjectManagerIface   = "org.freedesktop.DBus.ObjectManager"
	PropertiesIface      = "org.freedesktop.DBus.Properties"
	GattServiceIface     = "org.bluez.GattService1"
	GattCharacteristicIF = "org.bluez.GattCharacteristic1"
	DeviceIface          = "org.bluez.Device1"

	UUIDGTBS            = "0000184c-0000-1000-8000-00805f9b34fb"
	UUIDCurrentCalls    = "00002bb9-0000-1000-8000-00805f9b34fb"
	UUIDCallState       = "00002bbd-0000-1000-8000-00805f9b34fb"
	UUIDControlPoint    = "00002bbe-0000-1000-8000-00805f9b34fb"
	UUIDOptionalOpcodes = "00002bbf-0000-1000-8000-00805f9b34fb"
	UUIDTermination     = "00002bc0-0000-1000-8000-00805f9b34fb"
	UUIDIncomingCall    = "00002bc1-0000-1000-8000-00805f9b34fb"
	UUIDFriendlyName    = "00002bc2-0000-1000-8000-00805f9b34fb"
)

type ManagedObjects map[dbus.ObjectPath]map[string]map[string]dbus.Variant

type Characteristic struct {
	Path  dbus.ObjectPath `json:"path"`
	UUID  string          `json:"uuid"`
	Flags []string        `json:"flags"`
}

type Inventory struct {
	DevicePath      dbus.ObjectPath           `json:"device_path"`
	ServicePath     dbus.ObjectPath           `json:"service_path"`
	Characteristics map[string]Characteristic `json:"characteristics"`
}

func DevicePath(adapter, address string) (dbus.ObjectPath, error) {
	if !strings.HasPrefix(adapter, "hci") || address == "" {
		return "", errors.New("invalid BlueZ device selector")
	}
	path := dbus.ObjectPath("/org/bluez/" + adapter + "/dev_" + strings.ReplaceAll(strings.ToUpper(address), ":", "_"))
	if !path.IsValid() {
		return "", errors.New("invalid BlueZ device path")
	}
	return path, nil
}

// ResolveDevicePath selects a BlueZ device by its stable public Address
// property instead of assuming that the D-Bus object path embeds that address.
// BlueZ can retain the resolvable private address used during LE pairing in the
// object path even after identity resolution exposes the public Address.
func ResolveDevicePath(objects ManagedObjects, adapter, address string) (dbus.ObjectPath, error) {
	if _, err := DevicePath(adapter, address); err != nil {
		return "", err
	}
	adapterPath := dbus.ObjectPath("/org/bluez/" + adapter)
	matches := make([]dbus.ObjectPath, 0, 1)
	for path, interfaces := range objects {
		properties, present := interfaces[DeviceIface]
		if !present {
			continue
		}
		actualAddress, addressOK := variantString(properties["Address"])
		actualAdapter, adapterOK := variantPath(properties["Adapter"])
		if addressOK && adapterOK && strings.EqualFold(actualAddress, address) &&
			actualAdapter == adapterPath {
			matches = append(matches, path)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	if len(matches) != 1 {
		return "", fmt.Errorf("expected one BlueZ device for selector, found %d", len(matches))
	}
	return matches[0], nil
}

func ReadManagedObjects(ctx context.Context, conn *dbus.Conn) (ManagedObjects, error) {
	if conn == nil {
		return nil, errors.New("nil D-Bus connection")
	}
	objects := make(ManagedObjects)
	call := conn.Object(BlueZBusName, dbus.ObjectPath("/")).CallWithContext(ctx,
		ObjectManagerIface+".GetManagedObjects", 0)
	if call.Err != nil {
		return nil, fmt.Errorf("read BlueZ managed objects: %w", call.Err)
	}
	if err := call.Store(&objects); err != nil {
		return nil, fmt.Errorf("decode BlueZ managed objects: %w", err)
	}
	return objects, nil
}

func Discover(objects ManagedObjects, devicePath dbus.ObjectPath) (Inventory, error) {
	deviceInterfaces, present := objects[devicePath]
	if !present {
		return Inventory{}, errors.New("expected BlueZ device object is absent")
	}
	if _, present := deviceInterfaces[DeviceIface]; !present {
		return Inventory{}, errors.New("expected BlueZ device interface is absent")
	}
	servicePaths := make([]dbus.ObjectPath, 0, 1)
	for path, interfaces := range objects {
		properties, present := interfaces[GattServiceIface]
		if !present {
			continue
		}
		uuid, uuidOK := variantString(properties["UUID"])
		device, deviceOK := variantPath(properties["Device"])
		if uuidOK && deviceOK && strings.EqualFold(uuid, UUIDGTBS) && device == devicePath {
			servicePaths = append(servicePaths, path)
		}
	}
	if len(servicePaths) != 1 {
		return Inventory{}, fmt.Errorf("expected one GTBS service on the bonded device, found %d", len(servicePaths))
	}
	servicePath := servicePaths[0]
	result := Inventory{DevicePath: devicePath, ServicePath: servicePath,
		Characteristics: make(map[string]Characteristic)}
	for path, interfaces := range objects {
		properties, present := interfaces[GattCharacteristicIF]
		if !present {
			continue
		}
		service, serviceOK := variantPath(properties["Service"])
		uuid, uuidOK := variantString(properties["UUID"])
		flags, flagsOK := variantStrings(properties["Flags"])
		if !serviceOK || service != servicePath || !uuidOK || !flagsOK {
			continue
		}
		uuid = strings.ToLower(uuid)
		if _, duplicate := result.Characteristics[uuid]; duplicate {
			return Inventory{}, fmt.Errorf("duplicate GTBS characteristic %s", uuid)
		}
		sort.Strings(flags)
		result.Characteristics[uuid] = Characteristic{Path: path, UUID: uuid, Flags: flags}
	}
	if err := validateInventory(result); err != nil {
		return Inventory{}, err
	}
	return result, nil
}

func validateInventory(inventory Inventory) error {
	required := []struct {
		uuid  string
		flags []string
	}{
		{UUIDCallState, []string{"read", "notify"}},
		{UUIDControlPoint, []string{"notify"}},
	}
	for _, expectation := range required {
		characteristic, present := inventory.Characteristics[expectation.uuid]
		if !present {
			return fmt.Errorf("required GTBS characteristic %s is absent", expectation.uuid)
		}
		for _, flag := range expectation.flags {
			if !contains(characteristic.Flags, flag) {
				return fmt.Errorf("GTBS characteristic %s lacks %s", expectation.uuid, flag)
			}
		}
	}
	control := inventory.Characteristics[UUIDControlPoint]
	if !contains(control.Flags, "write") && !contains(control.Flags, "write-without-response") {
		return errors.New("GTBS Call Control Point is not writable")
	}
	return nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func variantString(variant dbus.Variant) (string, bool) {
	value, ok := variant.Value().(string)
	return value, ok
}

func variantPath(variant dbus.Variant) (dbus.ObjectPath, bool) {
	value, ok := variant.Value().(dbus.ObjectPath)
	return value, ok
}

func variantStrings(variant dbus.Variant) ([]string, bool) {
	value, ok := variant.Value().([]string)
	return append([]string(nil), value...), ok
}
