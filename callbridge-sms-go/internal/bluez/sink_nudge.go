package bluez

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	sinkNudgeEndpointPath   = dbus.ObjectPath("/callbridge/leaudio/sink_nudge")
	sinkNudgeCleanupTimeout = 3 * time.Second
)

// SinkNudge registers and immediately removes one temporary Sink PAC. BlueZ
// 5.87 recomputes and notifies Sink PACS contexts on the removal path, so this
// can refresh a connected client without replacing the long-running endpoint
// owner or patching BlueZ.
func SinkNudge(ctx context.Context, adapter, device string, logger *log.Logger) error {
	if ctx == nil || logger == nil || !validAdapterName(adapter) || !validBluetoothAddress(device) {
		return errors.New("invalid LE Audio Sink nudge configuration")
	}
	if !sinkNudgeEndpointPath.IsValid() {
		return errors.New("invalid temporary Sink endpoint object path")
	}
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return fmt.Errorf("connect system bus for Sink nudge: %w", err)
	}
	defer conn.Close()

	devicePath, err := resolveDevicePath(ctx, conn, adapter, device)
	if err != nil {
		return fmt.Errorf("resolve Sink nudge target: %w", err)
	}
	for _, property := range []string{"Paired", "Bonded", "Trusted"} {
		ready, err := readDeviceBoolean(ctx, conn, devicePath, property)
		if err != nil {
			return fmt.Errorf("read Sink nudge baseline %s: %w", property, err)
		}
		if !ready {
			return fmt.Errorf("Sink nudge baseline %s is false", property)
		}
	}
	leConnected, err := readLEBearerConnected(conn, devicePath)
	if err != nil {
		return fmt.Errorf("read Sink nudge LE bearer: %w", err)
	}
	if !leConnected {
		return errors.New("Sink nudge baseline LE bearer is disconnected")
	}

	if err := conn.ExportAll(&sinkNudgeEndpoint{}, sinkNudgeEndpointPath, endpointInterface); err != nil {
		return fmt.Errorf("export temporary Sink endpoint: %w", err)
	}
	media := dbusSinkNudgeRegistrar{
		object: conn.Object(bluezBusName, dbus.ObjectPath("/org/bluez/"+adapter)),
	}
	if err := performSinkNudge(ctx, media); err != nil {
		return err
	}
	logger.Printf("le audio Sink nudge complete adapter=%s endpoint=%s", adapter, sinkNudgeEndpointPath)
	return nil
}

func validAdapterName(adapter string) bool {
	if !strings.HasPrefix(adapter, "hci") || len(adapter) == len("hci") {
		return false
	}
	_, err := strconv.ParseUint(strings.TrimPrefix(adapter, "hci"), 10, 31)
	return err == nil
}

func validBluetoothAddress(address string) bool {
	parts := strings.Split(address, ":")
	if len(parts) != 6 {
		return false
	}
	for _, part := range parts {
		if len(part) != 2 {
			return false
		}
		if _, err := strconv.ParseUint(part, 16, 8); err != nil {
			return false
		}
	}
	return true
}

func readDeviceBoolean(ctx context.Context, conn *dbus.Conn, path dbus.ObjectPath, property string) (bool, error) {
	var value dbus.Variant
	call := conn.Object(bluezBusName, path).CallWithContext(ctx, propertiesInterface+".Get", 0,
		deviceInterface, property)
	if call.Err != nil {
		return false, call.Err
	}
	if err := call.Store(&value); err != nil {
		return false, err
	}
	enabled, ok := value.Value().(bool)
	if !ok {
		return false, errors.New("BlueZ device property is not boolean")
	}
	return enabled, nil
}

type sinkNudgeRegistrar interface {
	Register(context.Context, dbus.ObjectPath, map[string]dbus.Variant) error
	Unregister(context.Context, dbus.ObjectPath) error
}

type dbusSinkNudgeRegistrar struct {
	object dbus.BusObject
}

func (r dbusSinkNudgeRegistrar) Register(ctx context.Context, path dbus.ObjectPath, properties map[string]dbus.Variant) error {
	return r.object.CallWithContext(ctx, mediaInterface+".RegisterEndpoint", 0, path, properties).Err
}

func (r dbusSinkNudgeRegistrar) Unregister(ctx context.Context, path dbus.ObjectPath) error {
	return r.object.CallWithContext(ctx, mediaInterface+".UnregisterEndpoint", 0, path).Err
}

func performSinkNudge(ctx context.Context, registrar sinkNudgeRegistrar) error {
	if registrar == nil {
		return errors.New("nil Sink nudge registrar")
	}
	if err := registrar.Register(ctx, sinkNudgeEndpointPath, sinkNudgeEndpointProperties()); err != nil {
		return fmt.Errorf("register temporary Sink endpoint: %w", err)
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sinkNudgeCleanupTimeout)
	defer cancel()
	if err := registrar.Unregister(cleanupCtx, sinkNudgeEndpointPath); err != nil {
		return fmt.Errorf("unregister temporary Sink endpoint: %w", err)
	}
	return nil
}

func sinkNudgeEndpointProperties() map[string]dbus.Variant {
	properties := endpointProperties(PACSinkUUID)
	// BlueZ only notifies PACS contexts when the aggregate changes. Give the
	// temporary PAC one safe, disjoint standard context so registration moves
	// the aggregate from 0x0003 to 0x0007 and removal moves it back to the real
	// endpoints' 0x0003. The endpoint rejects every configuration request.
	properties["Context"] = dbus.MakeVariant(MediaContext)
	properties["SupportedContext"] = dbus.MakeVariant(MediaContext)
	return properties
}

// The temporary PAC exists only to exercise BlueZ's Sink removal path. It
// rejects every media configuration request so it cannot become a transient
// audio transport if a client races the immediate unregister.
type sinkNudgeEndpoint struct{}

func (*sinkNudgeEndpoint) SelectConfiguration([]byte) ([]byte, *dbus.Error) {
	return nil, sinkNudgeNotSupported()
}

func (*sinkNudgeEndpoint) SelectProperties(map[string]dbus.Variant) (map[string]dbus.Variant, *dbus.Error) {
	return nil, sinkNudgeNotSupported()
}

func (*sinkNudgeEndpoint) SetConfiguration(dbus.ObjectPath, map[string]dbus.Variant) *dbus.Error {
	return sinkNudgeNotSupported()
}

func (*sinkNudgeEndpoint) ClearConfiguration(dbus.ObjectPath) {}
func (*sinkNudgeEndpoint) Release()                           {}

func (*sinkNudgeEndpoint) Reconfigure(map[string]dbus.Variant) *dbus.Error {
	return sinkNudgeNotSupported()
}

func sinkNudgeNotSupported() *dbus.Error {
	return dbus.NewError("org.bluez.Error.NotSupported", []interface{}{"temporary Sink nudge endpoint"})
}
