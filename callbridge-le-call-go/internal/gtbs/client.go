package gtbs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"callbridge.local/callbridge-sms-go/lecall/internal/protocol"
	"github.com/godbus/dbus/v5"
)

var (
	ErrUnavailable    = errors.New("GTBS control unavailable")
	ErrStale          = errors.New("stale GTBS call command")
	ErrInvalid        = errors.New("invalid GTBS call command")
	errLEDisconnected = errors.New("LE bearer disconnected")
)

const leBearerIface = "org.bluez.Bearer.LE1"

type ProbeReport struct {
	DevicePath      dbus.ObjectPath  `json:"device_path"`
	ServicePath     dbus.ObjectPath  `json:"service_path"`
	Characteristics []Characteristic `json:"characteristics"`
	Calls           []ProbeCall      `json:"calls"`
	OptionalOpcodes []byte           `json:"optional_opcodes,omitempty"`
}

type ProbeCall struct {
	Index byte `json:"index"`
	State byte `json:"state"`
	Flags byte `json:"flags"`
}

type Callbacks struct {
	Snapshot func(Snapshot)
	Ready    func(bool)
	Result   func(opcode, index, result byte, token uint64)
}

type Client struct {
	adapter string
	device  string
	path    dbus.ObjectPath
	store   *Store
	logger  *log.Logger
	cbs     Callbacks

	mu               sync.RWMutex
	conn             *dbus.Conn
	inventory        Inventory
	ready            bool
	pending          map[[2]byte]uint64
	pendingOriginate bool
}

func NewClient(adapter, device string, store *Store, logger *log.Logger, callbacks Callbacks) (*Client, error) {
	path, err := DevicePath(adapter, device)
	if err != nil || store == nil || logger == nil || protocol.NormalizeDevice(device) == "" {
		return nil, errors.New("invalid GTBS client configuration")
	}
	return &Client{adapter: adapter, device: protocol.NormalizeDevice(device), path: path,
		store: store, logger: logger, cbs: callbacks, pending: make(map[[2]byte]uint64)}, nil
}

func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second
	for ctx.Err() == nil {
		established, err := c.runOnce(ctx)
		c.setReady(false)
		if ctx.Err() != nil {
			return nil
		}
		if established {
			backoff = time.Second
		}
		c.logger.Printf("gtbs session unavailable established=%t retry_in=%s reason=%s",
			established, backoff, RedactedError(err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return nil
}

func (c *Client) runOnce(ctx context.Context) (bool, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return false, fmt.Errorf("connect system bus: %w", err)
	}
	defer conn.Close()
	objects, err := ReadManagedObjects(ctx, conn)
	if err != nil {
		return false, err
	}
	path, err := ResolveDevicePath(objects, c.adapter, c.device)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	c.path = path
	c.mu.Unlock()
	if err := conn.AddMatchSignal(dbus.WithMatchInterface(PropertiesIface),
		dbus.WithMatchMember("PropertiesChanged"), dbus.WithMatchPathNamespace(path)); err != nil {
		return false, fmt.Errorf("subscribe GTBS properties: %w", err)
	}
	signals := make(chan *dbus.Signal, 64)
	conn.Signal(signals)
	defer conn.RemoveSignal(signals)
	connected, err := readLEBearerConnected(ctx, conn, path)
	if err != nil {
		return false, fmt.Errorf("read LE bearer state: %w", err)
	}
	if !connected {
		return false, errLEDisconnected
	}

	objects, err = ReadManagedObjects(ctx, conn)
	if err != nil {
		return false, err
	}
	inventory, err := Discover(objects, path)
	if err != nil {
		return false, err
	}
	paths := notificationPaths(inventory)
	started := make([]dbus.ObjectPath, 0, len(paths))
	for _, path := range paths {
		call := conn.Object(BlueZBusName, path).CallWithContext(ctx,
			GattCharacteristicIF+".StartNotify", 0)
		if call.Err != nil {
			return false, fmt.Errorf("start GTBS notification %s: %w", path, call.Err)
		}
		started = append(started, path)
	}
	defer func() {
		for _, path := range started {
			_ = conn.Object(BlueZBusName, path).Call(GattCharacteristicIF+".StopNotify", 0).Err
		}
	}()

	value, err := readValue(ctx, conn, inventory.Characteristics[UUIDCallState].Path)
	if err != nil {
		return false, fmt.Errorf("read GTBS Call State: %w", err)
	}
	snapshot, err := c.store.Apply(value)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	c.conn = conn
	c.inventory = inventory
	c.pending = make(map[[2]byte]uint64)
	c.pendingOriginate = false
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
			c.inventory = Inventory{}
			c.pending = make(map[[2]byte]uint64)
			c.pendingOriginate = false
		}
		c.mu.Unlock()
	}()
	c.emitSnapshot(snapshot)
	c.setReady(true)
	c.logger.Printf("gtbs ready service=%s call_state_notify=true control_point_notify=true", inventory.ServicePath)

	for {
		select {
		case <-ctx.Done():
			return true, nil
		case <-conn.Context().Done():
			return true, errors.New("BlueZ system bus disconnected")
		case signal := <-signals:
			if signal == nil {
				return true, errors.New("BlueZ GTBS signal channel closed")
			}
			if err := c.handleSignal(signal, inventory); errors.Is(err, errLEDisconnected) {
				return true, err
			} else if err != nil {
				c.logger.Printf("gtbs notification rejected reason=%s", RedactedError(err))
			}
		}
	}
}

func (c *Client) handleSignal(signal *dbus.Signal, inventory Inventory) error {
	if signal.Name != PropertiesIface+".PropertiesChanged" || len(signal.Body) < 2 {
		return nil
	}
	iface, ok := signal.Body[0].(string)
	if !ok {
		return errors.New("invalid BlueZ PropertiesChanged interface")
	}
	if iface == leBearerIface && signal.Path == inventory.DevicePath {
		changed, ok := signal.Body[1].(map[string]dbus.Variant)
		if !ok {
			return errors.New("invalid LE bearer PropertiesChanged body")
		}
		variant, present := changed["Connected"]
		if !present {
			return nil
		}
		connected, ok := variant.Value().(bool)
		if !ok {
			return errors.New("invalid LE bearer Connected property")
		}
		if !connected {
			return errLEDisconnected
		}
		return nil
	}
	if iface != GattCharacteristicIF {
		return nil
	}
	changed, ok := signal.Body[1].(map[string]dbus.Variant)
	if !ok {
		return errors.New("invalid GTBS PropertiesChanged body")
	}
	variant, present := changed["Value"]
	if !present {
		return nil
	}
	value, ok := variant.Value().([]byte)
	if !ok {
		return errors.New("invalid GTBS characteristic value")
	}
	switch signal.Path {
	case inventory.Characteristics[UUIDCallState].Path:
		snapshot, err := c.store.Apply(value)
		if err != nil {
			return err
		}
		c.emitSnapshot(snapshot)
	case inventory.Characteristics[UUIDControlPoint].Path:
		return c.handleControlResult(value)
	}
	return nil
}

func (c *Client) handleControlResult(value []byte) error {
	if len(value) != 3 || value[0] > protocol.OpcodeJoin ||
		(value[0] != protocol.OpcodeOriginate && value[1] == 0) ||
		!protocol.ValidResult(value[2]) {
		return errors.New("invalid GTBS Call Control Point result")
	}
	if value[0] == protocol.OpcodeOriginate {
		c.mu.Lock()
		pending := c.pendingOriginate
		c.pendingOriginate = false
		c.mu.Unlock()
		if !pending {
			return errors.New("unexpected GTBS originate result")
		}
		token, _ := c.store.TokenForIndex(value[1])
		if c.cbs.Result != nil {
			c.cbs.Result(value[0], value[1], value[2], token)
		}
		return nil
	}
	key := [2]byte{value[0], value[1]}
	c.mu.Lock()
	token := c.pending[key]
	delete(c.pending, key)
	c.mu.Unlock()
	if token == 0 {
		return errors.New("unexpected GTBS indexed result")
	}
	if c.cbs.Result != nil {
		c.cbs.Result(value[0], value[1], value[2], token)
	}
	return nil
}

func (c *Client) Command(ctx context.Context, message protocol.Message) error {
	if message.Type != protocol.TypeCommand || protocol.NormalizeDevice(message.Device) != c.device {
		return ErrInvalid
	}
	if message.Code == protocol.OpcodeOriginate {
		if !protocol.ValidURI(string(message.Payload)) {
			return ErrInvalid
		}
	} else if !c.store.ValidateIndexedCommand(message.Index, message.Token, message.Code) {
		return ErrStale
	}
	c.mu.RLock()
	conn := c.conn
	control, present := c.inventory.Characteristics[UUIDControlPoint]
	ready := c.ready
	c.mu.RUnlock()
	if conn == nil || !ready || !present {
		return ErrUnavailable
	}
	value := []byte{message.Code}
	if message.Code == protocol.OpcodeOriginate {
		value = append(value, message.Payload...)
	} else {
		value = append(value, message.Index)
	}
	options := map[string]dbus.Variant{"type": dbus.MakeVariant("request")}
	key := [2]byte{message.Code, message.Index}
	c.mu.Lock()
	if message.Code == protocol.OpcodeOriginate {
		if c.pendingOriginate {
			c.mu.Unlock()
			return ErrStale
		}
		c.pendingOriginate = true
	} else {
		c.pending[key] = message.Token
	}
	c.mu.Unlock()
	call := conn.Object(BlueZBusName, control.Path).CallWithContext(ctx,
		GattCharacteristicIF+".WriteValue", 0, value, options)
	if call.Err != nil {
		c.mu.Lock()
		if message.Code == protocol.OpcodeOriginate {
			c.pendingOriginate = false
		} else if c.pending[key] == message.Token {
			delete(c.pending, key)
		}
		c.mu.Unlock()
		return fmt.Errorf("write GTBS Call Control Point: %w", call.Err)
	}
	return nil
}

func Probe(ctx context.Context, adapter, device string) (ProbeReport, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return ProbeReport{}, fmt.Errorf("connect system bus: %w", err)
	}
	defer conn.Close()
	objects, err := ReadManagedObjects(ctx, conn)
	if err != nil {
		return ProbeReport{}, err
	}
	path, err := ResolveDevicePath(objects, adapter, device)
	if err != nil {
		return ProbeReport{}, err
	}
	inventory, err := Discover(objects, path)
	if err != nil {
		return ProbeReport{}, err
	}
	callValue, err := readValue(ctx, conn, inventory.Characteristics[UUIDCallState].Path)
	if err != nil {
		return ProbeReport{}, fmt.Errorf("read GTBS Call State: %w", err)
	}
	calls, err := ParseCallState(callValue)
	if err != nil {
		return ProbeReport{}, err
	}
	report := ProbeReport{DevicePath: path, ServicePath: inventory.ServicePath,
		Calls: make([]ProbeCall, 0, len(calls))}
	for _, characteristic := range inventory.Characteristics {
		report.Characteristics = append(report.Characteristics, characteristic)
	}
	sort.Slice(report.Characteristics, func(i, j int) bool {
		return report.Characteristics[i].UUID < report.Characteristics[j].UUID
	})
	for _, call := range calls {
		report.Calls = append(report.Calls, ProbeCall{Index: call.Index, State: call.State, Flags: call.Flags})
	}
	if characteristic, present := inventory.Characteristics[UUIDOptionalOpcodes]; present {
		value, readErr := readValue(ctx, conn, characteristic.Path)
		if readErr == nil {
			report.OptionalOpcodes = append([]byte(nil), value...)
		}
	}
	return report, nil
}

func readValue(ctx context.Context, conn *dbus.Conn, path dbus.ObjectPath) ([]byte, error) {
	var value []byte
	call := conn.Object(BlueZBusName, path).CallWithContext(ctx,
		GattCharacteristicIF+".ReadValue", 0, map[string]dbus.Variant{})
	if call.Err != nil {
		return nil, call.Err
	}
	if err := call.Store(&value); err != nil {
		return nil, err
	}
	return append([]byte(nil), value...), nil
}

func readLEBearerConnected(ctx context.Context, conn *dbus.Conn, path dbus.ObjectPath) (bool, error) {
	var value dbus.Variant
	call := conn.Object(BlueZBusName, path).CallWithContext(ctx,
		PropertiesIface+".Get", 0, leBearerIface, "Connected")
	if call.Err != nil {
		return false, call.Err
	}
	if err := call.Store(&value); err != nil {
		return false, err
	}
	connected, ok := value.Value().(bool)
	if !ok {
		return false, errors.New("LE bearer Connected property is not boolean")
	}
	return connected, nil
}

func notificationPaths(inventory Inventory) []dbus.ObjectPath {
	uuids := []string{UUIDCallState, UUIDControlPoint}
	paths := make([]dbus.ObjectPath, 0, len(uuids))
	for _, uuid := range uuids {
		characteristic, present := inventory.Characteristics[uuid]
		if present && contains(characteristic.Flags, "notify") {
			paths = append(paths, characteristic.Path)
		}
	}
	return paths
}

func (c *Client) setReady(ready bool) {
	c.mu.Lock()
	changed := c.ready != ready
	c.ready = ready
	c.mu.Unlock()
	if changed && c.cbs.Ready != nil {
		c.cbs.Ready(ready)
	}
}

func (c *Client) emitSnapshot(snapshot Snapshot) {
	if c.cbs.Snapshot != nil {
		c.cbs.Snapshot(snapshot)
	}
}

func (c *Client) Device() string {
	return c.device
}

func classifyCommandError(err error) byte {
	switch {
	case err == nil:
		return protocol.AckAccepted
	case errors.Is(err, ErrStale):
		return protocol.AckStale
	case errors.Is(err, ErrUnavailable):
		return protocol.AckUnavailable
	case errors.Is(err, ErrInvalid):
		return protocol.AckInvalid
	default:
		return protocol.AckWriteFailed
	}
}

func ClassifyCommandError(err error) byte {
	return classifyCommandError(err)
}

func RedactedError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if strings.Contains(text, "tel:") {
		return "GTBS command failed with redacted URI"
	}
	return text
}
