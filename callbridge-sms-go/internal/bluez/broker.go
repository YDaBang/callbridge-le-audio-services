package bluez

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/godbus/dbus/v5/prop"
	"golang.org/x/sys/unix"
)

const (
	bluezBusName               = "org.bluez"
	mediaInterface             = "org.bluez.Media1"
	endpointInterface          = "org.bluez.MediaEndpoint1"
	transportInterface         = "org.bluez.MediaTransport1"
	advertisingManager         = "org.bluez.LEAdvertisingManager1"
	advertisementIface         = "org.bluez.LEAdvertisement1"
	leBearerInterface          = "org.bluez.Bearer.LE1"
	deviceInterface            = "org.bluez.Device1"
	objectManagerInterface     = "org.freedesktop.DBus.ObjectManager"
	propertiesInterface        = "org.freedesktop.DBus.Properties"
	sinkEndpointPath           = dbus.ObjectPath("/callbridge/leaudio/sink")
	sourceEndpointPath         = dbus.ObjectPath("/callbridge/leaudio/source")
	advertisementPath          = dbus.ObjectPath("/callbridge/leaudio/advertisement")
	pairAcquireTimeout         = 5 * time.Second
	callCorrelationTimeout     = 3 * time.Second
	callCorrelationPoll        = 20 * time.Millisecond
	transportReleaseRetries    = 3
	transportReleaseRetryDelay = 100 * time.Millisecond
	mediaProgressStartTimeout  = 60 * time.Second
	mediaProgressLeaseTimeout  = 5 * time.Second
	remoteASEReleasingWatchdog = 3 * time.Second
)

var errHandoffStopped = errors.New("LE Audio handoff server stopped")

type qosValidationError struct {
	reason  string
	message string
}

func (e *qosValidationError) Error() string { return e.message }

func newQoSValidationError(reason, message string) error {
	return &qosValidationError{reason: reason, message: message}
}

type transportDetailError struct {
	reason string
	cause  error
}

func (e *transportDetailError) Error() string { return e.cause.Error() }
func (e *transportDetailError) Unwrap() error { return e.cause }

func newTransportDetailError(reason string, cause error) error {
	return &transportDetailError{reason: reason, cause: cause}
}

// transportDetailReason deliberately returns only fixed identifiers. D-Bus
// errors can include object paths, which contain the peer identity.
func transportDetailReason(err error) string {
	var qosErr *qosValidationError
	if errors.As(err, &qosErr) {
		return qosErr.reason
	}
	var detailErr *transportDetailError
	if errors.As(err, &detailErr) {
		return detailErr.reason
	}
	return "transport_detail_unknown"
}

type Broker struct {
	adapter         string
	expectedAddress string
	expectedDevice  string
	logger          *log.Logger
	handoff         *HandoffServer

	mu                     sync.Mutex
	connection             *dbus.Conn
	configured             map[dbus.ObjectPath]transportConfig
	acquiring              map[dbus.ObjectPath]bool
	acquired               map[dbus.ObjectPath]bool
	pairing                map[dbus.ObjectPath]bool
	pending                map[dbus.ObjectPath]*pendingTransport
	registered             bool
	advertising            bool
	discoverable           bool
	nextConfig             uint64
	lastGen                uint64
	callToken              func() (uint64, bool)
	requireToken           bool
	activeSession          *activeTransportSession
	releaseInvoker         transportReleaseInvoker
	releaseFallback        transportReleaseFallback
	releaseRetryDelay      time.Duration
	progressStartLease     time.Duration
	progressLease          time.Duration
	normalReleaseWait      time.Duration
	releaseAttempts        uint64
	releaseSuccesses       uint64
	releaseErrors          uint64
	releaseFallbacks       uint64
	lifecycleProgress      uint64
	lifecycleStale         uint64
	lifecycleHUPs          uint64
	lifecycleTimeouts      uint64
	lifecycleStartTimeouts uint64
	lifecycleNormalEnds    uint64
	normalReleaseWaits     uint64
	normalReleaseIdle      uint64
	normalReleaseTimeouts  uint64
	transportIdleComplete  uint64
}

type Snapshot struct {
	EndpointsRegistered       bool
	Advertising               bool
	Discoverable              bool
	ExtendedAdvertising       bool
	BAPAnnouncement           bool
	CAPAnnouncement           bool
	TMAPAnnouncement          bool
	HeadsetAppearance         bool
	SinkConfigured            bool
	SourceConfigured          bool
	SinkAcquired              bool
	SourceAcquired            bool
	BidirectionalCIS          bool
	TransportReleaseAttempts  uint64
	TransportReleaseSuccesses uint64
	TransportReleaseErrors    uint64
	TransportReleaseFallbacks uint64
	LifecycleProgressEvents   uint64
	LifecycleStaleEvents      uint64
	LifecycleHUPs             uint64
	LifecycleLeaseExpirations uint64
	LifecycleStartExpirations uint64
	LifecycleNormalEndEvents  uint64
	NormalReleaseWaits        uint64
	NormalReleaseRemoteIdle   uint64
	NormalReleaseTimeouts     uint64
	TransportIdleCompletions  uint64
}

type transportReleaseInvoker func(*dbus.Conn, dbus.ObjectPath) error
type transportReleaseFallback func(*dbus.Conn) error

type activeTransportSession struct {
	connection        *dbus.Conn
	releasePaths      []dbus.ObjectPath
	sinkPath          dbus.ObjectPath
	sourcePath        dbus.ObjectPath
	sinkGeneration    uint64
	sourceGeneration  uint64
	bundleID          uint64
	callToken         uint64
	lifecycleFD       int
	lifecycleDone     chan struct{}
	lifecycleDoneOnce sync.Once
	startedAt         time.Time
	progressArmed     bool
	lastProgress      time.Time
	normalEndSeen     bool
	normalWaitStarted bool
	idlePaths         map[dbus.ObjectPath]bool
	remoteIdleSeen    bool
	releasing         bool
	releaseExhausted  bool
}

func activeSessionForHandoff(conn *dbus.Conn, sink, source *pendingTransport,
	sinkDescriptor, sourceDescriptor Descriptor, lifecycleFD int,
	startedAt time.Time) *activeTransportSession {
	return &activeTransportSession{
		connection:       conn,
		releasePaths:     pendingReleasePaths(sink, source),
		sinkPath:         sink.path,
		sourcePath:       source.path,
		sinkGeneration:   sinkDescriptor.Generation,
		sourceGeneration: sourceDescriptor.Generation,
		bundleID:         sinkDescriptor.BundleID,
		callToken:        sinkDescriptor.CallControlToken,
		lifecycleFD:      lifecycleFD,
		lifecycleDone:    make(chan struct{}),
		startedAt:        startedAt,
		idlePaths:        make(map[dbus.ObjectPath]bool),
	}
}

type transportConfig struct {
	direction  Direction
	generation uint64
	codec      CodecConfig
	qos        TransportQoS
}

type TransportQoS struct {
	CIG                 byte
	CIS                 byte
	IntervalUS          uint32
	Framing             byte
	PHY                 byte
	SDU                 uint16
	Retransmissions     byte
	LatencyMS           uint16
	PresentationDelayUS uint32
	TargetLatency       byte
}

type pendingTransport struct {
	path     dbus.ObjectPath
	release  dbus.ObjectPath
	config   transportConfig
	qos      TransportQoS
	links    []dbus.ObjectPath
	fd       int
	readMTU  uint16
	writeMTU uint16
}

type endpoint struct {
	broker    *Broker
	direction Direction
}

type advertisement struct {
	broker *Broker
}

func NewBroker(adapter, device, socket string, peerUID int, logger *log.Logger) (*Broker, error) {
	if logger == nil || !strings.HasPrefix(adapter, "hci") || device == "" {
		return nil, errors.New("invalid LE Audio broker configuration")
	}
	handoff, err := NewHandoffServer(socket, peerUID)
	if err != nil {
		return nil, err
	}
	expected := "/org/bluez/" + adapter + "/dev_" + strings.ReplaceAll(strings.ToUpper(device), ":", "_")
	broker := &Broker{
		adapter: adapter, expectedAddress: strings.ToUpper(device), expectedDevice: expected,
		logger: logger, handoff: handoff,
		configured: make(map[dbus.ObjectPath]transportConfig), acquiring: make(map[dbus.ObjectPath]bool),
		acquired: make(map[dbus.ObjectPath]bool), pairing: make(map[dbus.ObjectPath]bool),
		pending:            make(map[dbus.ObjectPath]*pendingTransport),
		releaseInvoker:     invokeTransportRelease,
		releaseFallback:    closeTransportOwnerConnection,
		releaseRetryDelay:  transportReleaseRetryDelay,
		progressStartLease: mediaProgressStartTimeout,
		progressLease:      mediaProgressLeaseTimeout,
		// No production default is allowed here. The live value must be derived
		// from the measured remote-RELEASING to Go-wait-start delay and supplied
		// explicitly before Broker.Run.
		normalReleaseWait: 0,
	}
	if err := handoff.SetDisconnectHandler(func() {
		broker.requestActiveSessionRelease("asterisk_handoff_disconnected")
	}); err != nil {
		return nil, err
	}
	return broker, nil
}

// ConfigureMediaLeases must run before Broker.Run. The startup bound prevents
// a call that never produces media from retaining BlueZ ownership forever;
// the active bound is armed only after a generation-matching real media frame.
func (b *Broker) ConfigureMediaLeases(startup, active time.Duration) error {
	if b == nil || startup <= 0 || active <= 0 {
		return errors.New("invalid LE Audio media lease bounds")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.connection != nil || b.activeSession != nil {
		return errors.New("LE Audio media lease bounds changed after start")
	}
	b.progressStartLease = startup
	b.progressLease = active
	return nil
}

// ConfigureNormalReleaseWait sets the bounded interval for Android to finish
// the remote ASCS transition after a generation-scoped normal-end marker. It
// is injectable so the race between remote IDLE and the forced fallback is
// exercised without sleeping for the production interval.
func (b *Broker) ConfigureNormalReleaseWait(wait time.Duration) error {
	if b == nil || wait <= 0 || wait >= remoteASEReleasingWatchdog {
		return errors.New("normal LE Audio release wait must be positive and below the remote three-second watchdog")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.connection != nil || b.activeSession != nil {
		return errors.New("normal LE Audio release wait changed after broker start")
	}
	b.normalReleaseWait = wait
	return nil
}

type managedObjects map[dbus.ObjectPath]map[string]map[string]dbus.Variant

// resolveDevicePath follows the stable Device1.Address property. BlueZ may
// retain the RPA used during LE pairing in the object path after exposing the
// resolved public identity through Device1.Address.
func resolveDevicePath(ctx context.Context, conn *dbus.Conn, adapter, address string) (dbus.ObjectPath, error) {
	if conn == nil {
		return "", errors.New("nil D-Bus connection")
	}
	objects := make(managedObjects)
	call := conn.Object(bluezBusName, dbus.ObjectPath("/")).CallWithContext(ctx,
		objectManagerInterface+".GetManagedObjects", 0)
	if call.Err != nil {
		return "", fmt.Errorf("read BlueZ managed objects: %w", call.Err)
	}
	if err := call.Store(&objects); err != nil {
		return "", fmt.Errorf("decode BlueZ managed objects: %w", err)
	}
	return selectDevicePath(objects, adapter, address)
}

func selectDevicePath(objects managedObjects, adapter, address string) (dbus.ObjectPath, error) {
	adapterPath := dbus.ObjectPath("/org/bluez/" + adapter)
	matches := make([]dbus.ObjectPath, 0, 1)
	for path, interfaces := range objects {
		properties, present := interfaces[deviceInterface]
		if !present {
			continue
		}
		addressValue, addressOK := properties["Address"].Value().(string)
		adapterValue, adapterOK := properties["Adapter"].Value().(dbus.ObjectPath)
		if addressOK && adapterOK && strings.EqualFold(addressValue, address) &&
			adapterValue == adapterPath {
			matches = append(matches, path)
		}
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("expected one BlueZ device for selector, found %d", len(matches))
	}
	return matches[0], nil
}

// ConfigureCallControl installs the call-token source used to correlate a
// bidirectional CIS with one GTBS call. It must be called before Run. The
// default remains backward-compatible: descriptors are uncorrelated unless a
// provider is configured. When required is true, media is returned to BlueZ
// instead of being handed to Asterisk without a live call token.
func (b *Broker) ConfigureCallControl(provider func() (uint64, bool), required bool) error {
	if provider == nil && required {
		return errors.New("required LE Audio call-token provider is nil")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.connection != nil || b.registered {
		return errors.New("LE Audio call control must be configured before Run")
	}
	b.callToken = provider
	b.requireToken = required
	return nil
}

func (b *Broker) Run(ctx context.Context) error {
	handoffErr := make(chan error, 1)
	go func() { handoffErr <- b.handoff.Run(ctx) }()
	backoff := time.Second
	for ctx.Err() == nil {
		err := b.runBlueZOnce(ctx, handoffErr)
		if ctx.Err() != nil {
			if errors.Is(err, errHandoffStopped) {
				return nil
			}
			return waitHandoffExit(handoffErr)
		}
		if errors.Is(err, errHandoffStopped) {
			return err
		}
		b.logger.Printf("le audio BlueZ registration unavailable reason=%T", err)
		select {
		case <-ctx.Done():
			return waitHandoffExit(handoffErr)
		case handoffFailure := <-handoffErr:
			return wrapHandoffFailure(handoffFailure)
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return waitHandoffExit(handoffErr)
}

func waitHandoffExit(result <-chan error) error {
	select {
	case err := <-result:
		if err != nil {
			return fmt.Errorf("stop LE Audio handoff server: %w", err)
		}
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("LE Audio handoff server shutdown timed out")
	}
}

func (b *Broker) runBlueZOnce(ctx context.Context, handoffErr <-chan error) error {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return fmt.Errorf("connect system bus: %w", err)
	}
	defer conn.Close()
	if !conn.SupportsUnixFDs() {
		return errors.New("system bus lacks Unix FD passing")
	}
	devicePath, err := resolveDevicePath(ctx, conn, b.adapter, b.expectedAddress)
	if err != nil {
		return fmt.Errorf("resolve LE Audio device: %w", err)
	}
	b.mu.Lock()
	b.expectedDevice = string(devicePath)
	b.connection = conn
	b.configured = make(map[dbus.ObjectPath]transportConfig)
	b.acquiring = make(map[dbus.ObjectPath]bool)
	b.acquired = make(map[dbus.ObjectPath]bool)
	b.pairing = make(map[dbus.ObjectPath]bool)
	signalActiveSessionDone(b.activeSession)
	b.activeSession = nil
	b.closePendingLocked()
	b.pending = make(map[dbus.ObjectPath]*pendingTransport)
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.connection = nil
		b.mu.Unlock()
	}()

	sink := &endpoint{broker: b, direction: DirectionSink}
	source := &endpoint{broker: b, direction: DirectionSource}
	if err := conn.ExportAll(sink, sinkEndpointPath, endpointInterface); err != nil {
		return fmt.Errorf("export LE Audio sink: %w", err)
	}
	if err := conn.ExportAll(source, sourceEndpointPath, endpointInterface); err != nil {
		return fmt.Errorf("export LE Audio source: %w", err)
	}
	media := conn.Object(bluezBusName, dbus.ObjectPath("/org/bluez/"+b.adapter))
	if call := media.Call(mediaInterface+".RegisterEndpoint", 0, sinkEndpointPath, endpointProperties(PACSinkUUID)); call.Err != nil {
		return fmt.Errorf("register LE Audio sink: %w", call.Err)
	}
	defer media.Call(mediaInterface+".UnregisterEndpoint", 0, sinkEndpointPath)
	if call := media.Call(mediaInterface+".RegisterEndpoint", 0, sourceEndpointPath, endpointProperties(PACSourceUUID)); call.Err != nil {
		return fmt.Errorf("register LE Audio source: %w", call.Err)
	}
	defer media.Call(mediaInterface+".UnregisterEndpoint", 0, sourceEndpointPath)
	b.mu.Lock()
	b.registered = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.registered = false
		b.advertising = false
		b.discoverable = false
		b.configured = make(map[dbus.ObjectPath]transportConfig)
		b.acquiring = make(map[dbus.ObjectPath]bool)
		b.acquired = make(map[dbus.ObjectPath]bool)
		b.pairing = make(map[dbus.ObjectPath]bool)
		signalActiveSessionDone(b.activeSession)
		b.activeSession = nil
		b.closePendingLocked()
		b.pending = make(map[dbus.ObjectPath]*pendingTransport)
		b.mu.Unlock()
	}()

	ad := &advertisement{broker: b}
	if err := conn.ExportAll(ad, advertisementPath, advertisementIface); err != nil {
		return fmt.Errorf("export LE Audio advertisement: %w", err)
	}
	if _, err := prop.Export(conn, advertisementPath, advertisementProperties()); err != nil {
		return fmt.Errorf("export LE Audio advertisement properties: %w", err)
	}
	if err := conn.AddMatchSignal(
		dbus.WithMatchInterface(propertiesInterface), dbus.WithMatchMember("PropertiesChanged"),
		dbus.WithMatchPathNamespace(devicePath),
	); err != nil {
		return fmt.Errorf("subscribe to LE Audio transport state: %w", err)
	}
	/* bluetoothd can exit while the system bus connection stays up, so nothing
	 * below notices on its own: the endpoint objects stay exported and
	 * b.registered stays true while BlueZ has forgotten the registration. Watch
	 * the bus name so the serve loop can return and let Run() rebuild it.
	 */
	if err := conn.AddMatchSignal(
		dbus.WithMatchInterface("org.freedesktop.DBus"),
		dbus.WithMatchMember("NameOwnerChanged"),
		dbus.WithMatchArg(0, bluezBusName),
	); err != nil {
		return fmt.Errorf("subscribe to BlueZ bus name: %w", err)
	}
	signals := make(chan *dbus.Signal, 32)
	conn.Signal(signals)
	defer conn.RemoveSignal(signals)

	manager := conn.Object(bluezBusName, dbus.ObjectPath("/org/bluez/"+b.adapter))
	advertisementRegistered := false
	setAdvertisement := func(enable bool) error {
		if enable == advertisementRegistered {
			return nil
		}
		method := advertisingManager + ".RegisterAdvertisement"
		args := []interface{}{advertisementPath, map[string]dbus.Variant{}}
		if !enable {
			method = advertisingManager + ".UnregisterAdvertisement"
			args = []interface{}{advertisementPath}
		}
		if call := manager.Call(method, 0, args...); call.Err != nil {
			return call.Err
		}
		advertisementRegistered = enable
		b.mu.Lock()
		b.advertising = enable
		b.discoverable = enable
		b.mu.Unlock()
		return nil
	}
	defer func() { _ = setAdvertisement(false) }()
	leConnected, err := readLEBearerConnected(conn, devicePath)
	if err != nil {
		return fmt.Errorf("read initial LE bearer state: %w", err)
	}
	if leConnected {
		/* bluetoothd owns the BLE link, not this process, so a restart here
		 * leaves the phone connected and unaware that the endpoints it was
		 * paired against have gone.  It keeps its group ACTIVE and never
		 * offers a CIS again, which presents as a call that connects with no
		 * audio and no error anywhere.  Dropping the link makes the phone
		 * observe the loss and rebuild its state on reconnect.
		 */
		if err := resetLEBearer(conn, devicePath); err != nil {
			b.logger.Printf("le audio bearer reset skipped reason=%v", err)
		} else {
			b.logger.Printf("le audio bearer reset to realign phone state")
			leConnected = false
		}
	}
	if !leConnected {
		if err := setAdvertisement(true); err != nil {
			return fmt.Errorf("register targeted LE Audio advertisement: %w", err)
		}
	}
	b.logger.Printf("le audio endpoints registered policy=le-canary adapter=%s advertising=%t discoverable=%t local_name=Callbridge-Asterisk appearance=headset extended=true bap_announcement=targeted_idle cap_announcement=targeted tmap_role=ct pairable=false", b.adapter, !leConnected, !leConnected)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-handoffErr:
			return wrapHandoffFailure(err)
		case <-conn.Context().Done():
			return errors.New("BlueZ system bus disconnected")
		case signal := <-signals:
			if signal == nil {
				return errors.New("BlueZ signal channel closed")
			}
			if bluezNameLost(signal) {
				return errors.New("BlueZ bus name disappeared")
			}
			if connected, matched := leBearerConnectionChanged(signal, string(devicePath)); matched {
				if connected {
					if err := setAdvertisement(false); err != nil {
						return fmt.Errorf("stop targeted advertisement after LE connect: %w", err)
					}
					b.logger.Printf("le audio targeted advertisement stopped reason=le_connected")
				} else {
					b.handleLEBearerDisconnected()
					if err := setAdvertisement(true); err != nil {
						return fmt.Errorf("rearm targeted advertisement after LE disconnect: %w", err)
					}
					b.logger.Printf("le audio targeted advertisement rearmed reason=le_disconnected")
				}
				continue
			}
			b.handleSignal(signal)
		}
	}
}

// bluezNameLost reports whether org.bluez released its bus name. The GTBS client
// recovers on its own because it retries, but the media endpoints are registered
// once per runBlueZOnce call, so without this the two halves drift apart: GTBS
// reconnects and logs "gtbs ready" while PACS stays unregistered and the phone
// sees a device with no audio capability.
func bluezNameLost(signal *dbus.Signal) bool {
	if signal == nil || signal.Name != "org.freedesktop.DBus.NameOwnerChanged" ||
		len(signal.Body) < 3 {
		return false
	}
	name, ok := signal.Body[0].(string)
	if !ok || name != bluezBusName {
		return false
	}
	newOwner, ok := signal.Body[2].(string)
	return ok && newOwner == ""
}

func (a *advertisement) Release() {
	a.broker.mu.Lock()
	a.broker.advertising = false
	a.broker.discoverable = false
	a.broker.mu.Unlock()
}

func advertisementProperties() prop.Map {
	bapAnnouncement := []byte{
		0x01,       // Targeted Announcement
		0x00, 0x00, // Available Sink Contexts: idle reconnect
		0x00, 0x00, // Available Source Contexts: idle reconnect
		0x00, // Metadata_Length
	}
	return prop.Map{
		advertisementIface: {
			"Type": {
				Value: "peripheral",
				Emit:  prop.EmitConst,
			},
			"ServiceUUIDs": {
				Value: []string{ASCSUUID, PACSUUID, CASUUID, TMASUUID, VCSUUID},
				Emit:  prop.EmitConst,
			},
			"ServiceData": {
				Value: map[string]dbus.Variant{
					ASCSUUID: dbus.MakeVariant(bapAnnouncement),
					CASUUID:  dbus.MakeVariant([]byte{0x01}),       // CAP Targeted Announcement
					TMASUUID: dbus.MakeVariant([]byte{0x02, 0x00}), // TMAP Call Terminal role
				},
				Emit: prop.EmitConst,
			},
			"Appearance": {
				Value: HeadsetAppearance,
				Emit:  prop.EmitConst,
			},
			"Discoverable": {
				Value: true,
				Emit:  prop.EmitConst,
			},
			"DiscoverableTimeout": {
				Value: uint16(0),
				Emit:  prop.EmitConst,
			},
			"LocalName": {
				Value: "Callbridge-Asterisk",
				Emit:  prop.EmitConst,
			},
			"SecondaryChannel": {
				Value: "1M",
				Emit:  prop.EmitConst,
			},
		},
	}
}

func readLEBearerConnected(conn *dbus.Conn, path dbus.ObjectPath) (bool, error) {
	var value dbus.Variant
	call := conn.Object(bluezBusName, path).Call(propertiesInterface+".Get", 0,
		leBearerInterface, "Connected")
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

/* Disconnect the device so the phone observes the loss.  Reconnection is left
 * to the phone and to the targeted advertisement the caller registers next; a
 * Connect from here would race that and can strand the device half attached.
 */
func resetLEBearer(conn *dbus.Conn, path dbus.ObjectPath) error {
	call := conn.Object(bluezBusName, path).Call(deviceInterface+".Disconnect", 0)
	if call.Err != nil {
		return call.Err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		connected, err := readLEBearerConnected(conn, path)
		if err != nil {
			return err
		}
		if !connected {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("LE bearer still connected after Disconnect")
}

func leBearerConnectionChanged(signal *dbus.Signal, expectedDevice string) (bool, bool) {
	if signal == nil || signal.Name != propertiesInterface+".PropertiesChanged" ||
		string(signal.Path) != expectedDevice || len(signal.Body) < 2 {
		return false, false
	}
	iface, ok := signal.Body[0].(string)
	if !ok || iface != leBearerInterface {
		return false, false
	}
	changed, ok := signal.Body[1].(map[string]dbus.Variant)
	if !ok {
		return false, false
	}
	connected, ok := changed["Connected"].Value().(bool)
	return connected, ok
}

func wrapHandoffFailure(err error) error {
	if err == nil {
		return errHandoffStopped
	}
	return fmt.Errorf("%w: %v", errHandoffStopped, err)
}

func endpointProperties(uuid string) map[string]dbus.Variant {
	capabilities := append([]byte(nil), LC3Capabilities...)
	return map[string]dbus.Variant{
		"UUID":             dbus.MakeVariant(uuid),
		"Codec":            dbus.MakeVariant(LC3Codec),
		"Capabilities":     dbus.MakeVariant(capabilities),
		"Metadata":         dbus.MakeVariant([]byte{0x03, 0x01, 0x02, 0x00}),
		"Locations":        dbus.MakeVariant(FrontLeftLocation),
		"SupportedContext": dbus.MakeVariant(SupportedContexts),
		"Context":          dbus.MakeVariant(SupportedContexts),
		"Framing":          dbus.MakeVariant(byte(0x00)),
		"PHY":              dbus.MakeVariant(byte(0x02)),
		"Retransmissions":  dbus.MakeVariant(byte(0x02)),
		"SupportedFeatures": dbus.MakeVariant(map[string]dbus.Variant{
			TMASUUID: dbus.MakeVariant([]string{TMAPRoleCT}),
		}),
	}
}

func (b *Broker) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := Snapshot{
		EndpointsRegistered:       b.registered,
		Advertising:               b.advertising,
		Discoverable:              b.discoverable,
		ExtendedAdvertising:       b.advertising,
		BAPAnnouncement:           b.advertising,
		CAPAnnouncement:           b.advertising,
		TMAPAnnouncement:          b.advertising,
		HeadsetAppearance:         b.advertising,
		TransportReleaseAttempts:  b.releaseAttempts,
		TransportReleaseSuccesses: b.releaseSuccesses,
		TransportReleaseErrors:    b.releaseErrors,
		TransportReleaseFallbacks: b.releaseFallbacks,
		LifecycleProgressEvents:   b.lifecycleProgress,
		LifecycleStaleEvents:      b.lifecycleStale,
		LifecycleHUPs:             b.lifecycleHUPs,
		LifecycleLeaseExpirations: b.lifecycleTimeouts,
		LifecycleStartExpirations: b.lifecycleStartTimeouts,
		LifecycleNormalEndEvents:  b.lifecycleNormalEnds,
		NormalReleaseWaits:        b.normalReleaseWaits,
		NormalReleaseRemoteIdle:   b.normalReleaseIdle,
		NormalReleaseTimeouts:     b.normalReleaseTimeouts,
		TransportIdleCompletions:  b.transportIdleComplete,
	}
	for path, configuration := range b.configured {
		switch configuration.direction {
		case DirectionSink:
			result.SinkConfigured = true
			result.SinkAcquired = result.SinkAcquired || b.acquired[path]
		case DirectionSource:
			result.SourceConfigured = true
			result.SourceAcquired = result.SourceAcquired || b.acquired[path]
		}
	}
	result.BidirectionalCIS = result.SinkAcquired && result.SourceAcquired
	return result
}

func (e *endpoint) SelectConfiguration(capabilities []byte) ([]byte, *dbus.Error) {
	configuration, err := SelectConfiguration(capabilities)
	if err != nil {
		return nil, e.reject("SelectConfiguration", "capabilities", err)
	}
	return configuration, nil
}

func (e *endpoint) reject(method, stage string, err error) *dbus.Error {
	if e != nil && e.broker != nil && e.broker.logger != nil {
		e.broker.logger.Printf("le audio endpoint rejected method=%s stage=%s reason=%s", method, stage, err)
	}
	return invalidArguments(err)
}

func (e *endpoint) SelectProperties(properties map[string]dbus.Variant) (map[string]dbus.Variant, *dbus.Error) {
	capabilities, ok := variantBytes(properties["Capabilities"])
	if !ok {
		return nil, e.reject("SelectProperties", "capabilities", errors.New("LC3 capabilities missing"))
	}
	allocation, err := selectChannelAllocation(properties)
	if err != nil {
		return nil, e.reject("SelectProperties", "channel_allocation", err)
	}
	configuration, err := SelectConfigurationForAllocation(capabilities, allocation)
	if err != nil {
		return nil, e.reject("SelectProperties", "configuration", err)
	}
	codec, err := ParseConfiguration(configuration)
	if err != nil {
		return nil, e.reject("SelectProperties", "codec", err)
	}
	selectedQoS, err := selectQoS(properties, codec)
	if err != nil {
		return nil, e.reject("SelectProperties", "qos", err)
	}
	return map[string]dbus.Variant{
		"Capabilities": dbus.MakeVariant(configuration),
		"Metadata":     dbus.MakeVariant([]byte{0x03, 0x02, 0x02, 0x00}),
		"QoS":          dbus.MakeVariant(qosVariants(selectedQoS)),
	}, nil
}

func (e *endpoint) SetConfiguration(transport dbus.ObjectPath, properties map[string]dbus.Variant) *dbus.Error {
	configuration, ok := variantBytes(properties["Configuration"])
	if !ok {
		configuration, ok = variantBytes(properties["Capabilities"])
	}
	if !ok {
		return e.reject("SetConfiguration", "configuration", errors.New("LC3 configuration missing"))
	}
	codec, err := ParseConfiguration(configuration)
	if err != nil {
		return e.reject("SetConfiguration", "codec", err)
	}
	var parsedQoS TransportQoS
	if qosValue, present := properties["QoS"]; present {
		qos, ok := qosValue.Value().(map[string]dbus.Variant)
		if !ok {
			return e.reject("SetConfiguration", "qos", errors.New("invalid LC3 QoS dictionary"))
		}
		parsedQoS, err = parsePreliminaryConfiguredQoS(qos, codec)
		if err != nil {
			staleSDU, stale := boundedStaleConfiguredSDU(qos, codec, err)
			unallocatedCIG, unallocatedCIS, unallocated := byte(0), byte(0), false
			if !stale {
				unallocatedCIG, unallocatedCIS, unallocated = boundedUnallocatedConfiguredQoS(qos, codec, err)
			}
			if !stale && !unallocated {
				e.logQoSMismatch(codec, qos, err)
				return e.reject("SetConfiguration", "qos", err)
			}
			// During codec configuration BlueZ can attach either the previous
			// bounded conversational SDU or a genuinely unallocated QoS state
			// to the next callback. Neither is the final transport contract.
			// Keep a codec-matching provisional preset here; acquire() re-reads
			// the allocated transport and applies the strict final checks.
			parsedQoS, err = selectQoS(nil, codec)
			if err != nil {
				return e.reject("SetConfiguration", "qos_default", err)
			}
			if stale {
				e.broker.logger.Printf(
					"le audio endpoint deferred method=SetConfiguration stage=qos reason=stale_sdu codec_octets=%d qos_sdu=%d source=MediaTransport1",
					codec.OctetsPerFrame, staleSDU,
				)
			} else {
				e.broker.logger.Printf(
					"le audio endpoint deferred method=SetConfiguration stage=qos reason=unallocated_sdu codec_octets=%d qos_sdu=0 qos_cig=%d qos_cis=%d source=MediaTransport1",
					codec.OctetsPerFrame, unallocatedCIG, unallocatedCIS,
				)
			}
		}
	} else {
		// BlueZ creates the MediaTransport during the codec configuration
		// phase, before unicast QoS has been configured. Keep only a bounded
		// provisional preset here. acquire() re-reads MediaTransport1 and
		// requires allocated final QoS before any descriptor is handed off.
		parsedQoS, err = selectQoS(nil, codec)
		if err != nil {
			return e.reject("SetConfiguration", "qos_default", err)
		}
		e.broker.logger.Printf("le audio endpoint deferred method=SetConfiguration stage=qos source=MediaTransport1")
	}
	if err := e.broker.configure(transport, e.direction, codec, parsedQoS); err != nil {
		return e.reject("SetConfiguration", "transport", err)
	}
	return nil
}

func (e *endpoint) logQoSMismatch(codec CodecConfig, qos map[string]dbus.Variant, parseErr error) {
	if transportDetailReason(parseErr) != "qos_sdu_mismatch" || e == nil || e.broker == nil || e.broker.logger == nil {
		return
	}
	sdu, sduOK := variantUint16(qos["SDU"])
	cig, cigOK := variantByte(qos["CIG"])
	cis, cisOK := variantByte(qos["CIS"])
	if !sduOK || !cigOK || !cisOK {
		return
	}
	e.broker.logger.Printf(
		"le audio endpoint qos mismatch codec_octets=%d qos_sdu=%d qos_cig=%d qos_cis=%d source=MediaTransport1",
		codec.OctetsPerFrame, sdu, cig, cis,
	)
}

// boundedStaleConfiguredSDU accepts only a complete, otherwise-valid QoS
// dictionary whose SDU is one of the broker's known conversational frame
// sizes for the configured duration. It never relaxes final transport
// validation: parseFinalTransportQoS still requires the allocated SDU to
// match the current codec exactly before any ISO descriptor is published.
func boundedStaleConfiguredSDU(qos map[string]dbus.Variant, codec CodecConfig, parseErr error) (uint16, bool) {
	if transportDetailReason(parseErr) != "qos_sdu_mismatch" {
		return 0, false
	}
	staleSDU, ok := variantUint16(qos["SDU"])
	if !ok || staleSDU == 0 || int(staleSDU) == codec.OctetsPerFrame {
		return 0, false
	}
	known := false
	for _, candidate := range conversationalPresets {
		if candidate.frameDuration == codec.FrameDuration && candidate.octets == int(staleSDU) {
			known = true
			break
		}
	}
	if !known {
		return 0, false
	}
	staleCodec := codec
	staleCodec.OctetsPerFrame = int(staleSDU)
	if _, err := parsePreliminaryConfiguredQoS(qos, staleCodec); err != nil {
		return 0, false
	}
	return staleSDU, true
}

// parsePreliminaryConfiguredQoS validates every field BlueZ publishes in
// MediaTransport1.QoS. BlueZ 5.87 does not publish TargetLatency there, so the
// broker may carry forward only the bounded value it selected earlier. If a
// future BlueZ publishes TargetLatency, that value is validated as supplied.
func parsePreliminaryConfiguredQoS(qos map[string]dbus.Variant, codec CodecConfig) (TransportQoS, error) {
	if _, present := qos["TargetLatency"]; present {
		return parseConfiguredQoS(qos, codec, false)
	}
	selected, err := selectQoS(nil, codec)
	if err != nil {
		return TransportQoS{}, err
	}
	merged := make(map[string]dbus.Variant, len(qos)+1)
	for name, value := range qos {
		merged[name] = value
	}
	merged["TargetLatency"] = dbus.MakeVariant(selected.TargetLatency)
	return parseConfiguredQoS(merged, codec, false)
}

// boundedUnallocatedConfiguredQoS accepts only BlueZ's explicit unallocated
// unicast state: SDU zero with both CIG and CIS set to their 0xff sentinel.
// Every other published transport field must already satisfy the normal
// configured-QoS checks. BlueZ 5.87 omits TargetLatency from MediaTransport1,
// so only that previously selected value is carried forward for validation.
// Final transport validation is unchanged and still requires allocated IDs
// plus an exact codec-to-SDU match before any ISO descriptor is published.
func boundedUnallocatedConfiguredQoS(qos map[string]dbus.Variant, codec CodecConfig, parseErr error) (byte, byte, bool) {
	if transportDetailReason(parseErr) != "qos_sdu_mismatch" {
		return 0, 0, false
	}
	sdu, ok := variantUint16(qos["SDU"])
	if !ok || sdu != 0 {
		return 0, 0, false
	}
	cig, ok := variantByte(qos["CIG"])
	if !ok || cig != 0xff {
		return 0, 0, false
	}
	cis, ok := variantByte(qos["CIS"])
	if !ok || cis != 0xff {
		return 0, 0, false
	}
	selected, err := selectQoS(nil, codec)
	if err != nil {
		return 0, 0, false
	}
	merged := make(map[string]dbus.Variant, len(qos))
	for name, value := range qos {
		merged[name] = value
	}
	merged["SDU"] = dbus.MakeVariant(selected.SDU)
	if _, err := parsePreliminaryConfiguredQoS(merged, codec); err != nil {
		return 0, 0, false
	}
	return cig, cis, true
}

func (e *endpoint) ClearConfiguration(transport dbus.ObjectPath) {
	e.broker.clear(transport)
}

func (e *endpoint) Release() {}

func (e *endpoint) Reconfigure(_ map[string]dbus.Variant) *dbus.Error {
	return dbus.NewError("org.bluez.Error.NotSupported", []interface{}{"P0 candidate does not reconfigure active streams"})
}

func (b *Broker) configure(path dbus.ObjectPath, direction Direction, codec CodecConfig, qos TransportQoS) error {
	b.mu.Lock()
	if !path.IsValid() || !strings.HasPrefix(string(path), b.expectedDevice+"/") {
		b.mu.Unlock()
		return errors.New("unexpected BlueZ transport")
	}
	if b.nextConfig == ^uint64(0) {
		b.mu.Unlock()
		return errors.New("LE Audio configuration generation exhausted")
	}
	b.nextConfig++
	if held := b.pending[path]; held != nil {
		_ = unix.Close(held.fd)
		delete(b.pending, path)
	}
	b.configured[path] = transportConfig{direction: direction, generation: b.nextConfig, codec: codec, qos: qos}
	delete(b.acquiring, path)
	delete(b.acquired, path)
	delete(b.pairing, path)
	b.mu.Unlock()
	return nil
}

func (b *Broker) clear(path dbus.ObjectPath) {
	b.mu.Lock()
	if held := b.pending[path]; held != nil {
		_ = unix.Close(held.fd)
		delete(b.pending, path)
	}
	delete(b.configured, path)
	delete(b.acquiring, path)
	delete(b.acquired, path)
	delete(b.pairing, path)
	if active := b.activeSession; active != nil &&
		(path == active.sinkPath || path == active.sourcePath ||
			containsObjectPath(active.releasePaths, path)) {
		b.clearActiveSessionLocked(active)
	}
	b.mu.Unlock()
}

func (b *Broker) handleSignal(signal *dbus.Signal) {
	if signal == nil || signal.Name != propertiesInterface+".PropertiesChanged" || len(signal.Body) < 2 {
		return
	}
	b.mu.Lock()
	expectedDevice := b.expectedDevice
	b.mu.Unlock()
	if string(signal.Path) != expectedDevice && !strings.HasPrefix(string(signal.Path), expectedDevice+"/") {
		return
	}
	iface, ok := signal.Body[0].(string)
	if !ok || iface != transportInterface {
		return
	}
	changed, ok := signal.Body[1].(map[string]dbus.Variant)
	if !ok {
		return
	}
	state, ok := changed["State"].Value().(string)
	if !ok {
		return
	}
	if state == "pending" {
		go b.acquire(signal.Path)
	} else if state == "idle" {
		b.mu.Lock()
		delete(b.acquiring, signal.Path)
		delete(b.acquired, signal.Path)
		delete(b.pairing, signal.Path)
		remoteNormalIdle := false
		if active := b.activeSession; active != nil &&
			(signal.Path == active.sinkPath || signal.Path == active.sourcePath ||
				containsObjectPath(active.releasePaths, signal.Path)) {
			if active.idlePaths == nil {
				active.idlePaths = make(map[dbus.ObjectPath]bool)
			}
			active.idlePaths[signal.Path] = true
			allRemoteIdle := active.idlePaths[active.sinkPath] &&
				active.idlePaths[active.sourcePath]
			if allRemoteIdle {
				if active.releasing {
					/* A timeout/abnormal Release already owns termination. Keep
					 * the session visible until that invocation finishes, but
					 * remember that a failed call no longer needs to retain it. */
					active.remoteIdleSeen = true
				} else {
					remoteNormalIdle = active.normalEndSeen
					b.transportIdleComplete++
					if remoteNormalIdle {
						b.normalReleaseIdle++
					}
					b.clearActiveSessionLocked(active)
				}
			}
		}
		logger := b.logger
		b.mu.Unlock()
		if remoteNormalIdle && logger != nil {
			logger.Printf("le audio normal teardown outcome=remote_idle event_unix_ns=%d",
				time.Now().UnixNano())
		}
	}
}

func (b *Broker) acquire(path dbus.ObjectPath) {
	b.mu.Lock()
	configuration, ok := b.configured[path]
	conn := b.connection
	if !ok || conn == nil || b.acquiring[path] || b.acquired[path] || b.pairing[path] {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	links, linksErr := readTransportLinks(conn, path)
	if linksErr != nil {
		b.logger.Printf("le audio transport acquire deferred reason=links_read_failed")
		return
	}
	peerPath, linked, leader, planErr := linkedAcquirePlan(path, links)
	if planErr != nil {
		b.logger.Printf("le audio transport acquire rejected reason=invalid_linked_cis")
		return
	}
	if linked && !leader {
		return
	}

	b.mu.Lock()
	configuration, ok = b.configured[path]
	conn = b.connection
	if !ok || conn == nil || b.acquiring[path] || b.acquired[path] || b.pairing[path] {
		b.mu.Unlock()
		return
	}
	var peerConfiguration transportConfig
	if linked {
		var peerOK bool
		peerConfiguration, peerOK = b.configured[peerPath]
		if !peerOK || b.acquiring[peerPath] || b.acquired[peerPath] || b.pairing[peerPath] {
			b.mu.Unlock()
			return
		}
		b.acquiring[peerPath] = true
	}
	b.acquiring[path] = true
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.acquiring, path)
		if linked {
			delete(b.acquiring, peerPath)
		}
		b.mu.Unlock()
	}()
	var fd dbus.UnixFD
	var readMTU, writeMTU uint16
	call := conn.Object(bluezBusName, path).Call(transportInterface+".TryAcquire", 0)
	if call.Err != nil {
		name, message := boundedDBusError(call.Err, b.expectedDevice, b.expectedAddress)
		b.logger.Printf("le audio transport acquire failed method=TryAcquire error_name=%s error_message=%q", name, message)
		return
	}
	if storeErr := call.Store(&fd, &readMTU, &writeMTU); storeErr != nil {
		b.logger.Printf("le audio transport acquire failed method=TryAcquire error_name=reply_decode error_message=%q", storeErr.Error())
		return
	}
	rawFD := int(fd)
	finalQoS, links, detailErr := readTransportDetails(conn, path, configuration.codec, configuration.qos)
	if detailErr != nil {
		b.releaseAcquiredTransport(conn, path, rawFD, "final_transport_validation_failed")
		b.logger.Printf("le audio final transport validation failed reason=%s", transportDetailReason(detailErr))
		return
	}
	if linked && !containsObjectPath(links, peerPath) {
		b.releaseAcquiredTransport(conn, path, rawFD, "linked_peer_missing")
		b.logger.Printf("le audio transport acquire rejected reason=linked_peer_missing")
		return
	}
	var linkedPeer *pendingTransport
	if linked {
		peerQoS, peerLinks, peerDetailErr := readTransportDetails(conn, peerPath,
			peerConfiguration.codec, peerConfiguration.qos)
		if peerDetailErr != nil {
			b.releaseAcquiredTransport(conn, path, rawFD, "linked_peer_validation_failed")
			b.logger.Printf("le audio linked peer validation failed reason=%s", transportDetailReason(peerDetailErr))
			return
		}
		if !containsObjectPath(peerLinks, path) {
			b.releaseAcquiredTransport(conn, path, rawFD, "linked_primary_missing")
			b.logger.Printf("le audio transport acquire rejected reason=linked_primary_missing")
			return
		}
		peerFD, duplicateErr := duplicateLinkedFD(rawFD)
		if duplicateErr != nil {
			b.releaseAcquiredTransport(conn, path, rawFD, "linked_fd_duplication_failed")
			b.logger.Printf("le audio linked transport duplication failed reason=%T", duplicateErr)
			return
		}
		linkedPeer = &pendingTransport{
			path: peerPath, release: path, config: peerConfiguration, qos: peerQoS,
			links: peerLinks, fd: peerFD, readMTU: readMTU, writeMTU: writeMTU,
		}
	}
	b.mu.Lock()
	current, stillConfigured := b.configured[path]
	if !stillConfigured || current.generation != configuration.generation {
		b.mu.Unlock()
		if linkedPeer != nil {
			_ = unix.Close(linkedPeer.fd)
		}
		b.releaseAcquiredTransport(conn, path, rawFD, "configuration_changed")
		return
	}
	if linked {
		peerCurrent, peerStillConfigured := b.configured[peerPath]
		if !peerStillConfigured || peerCurrent.generation != peerConfiguration.generation {
			b.mu.Unlock()
			_ = unix.Close(linkedPeer.fd)
			b.releaseAcquiredTransport(conn, path, rawFD, "linked_configuration_changed")
			return
		}
	}
	if _, duplicate := b.pending[path]; duplicate {
		b.mu.Unlock()
		if linkedPeer != nil {
			_ = unix.Close(linkedPeer.fd)
		}
		b.releaseAcquiredTransport(conn, path, rawFD, "duplicate_pending_transport")
		return
	}
	held := &pendingTransport{
		path: path, release: path, config: configuration, qos: finalQoS, links: links,
		fd: rawFD, readMTU: readMTU, writeMTU: writeMTU,
	}
	b.pending[path] = held
	if linked {
		if _, duplicate := b.pending[peerPath]; duplicate {
			delete(b.pending, path)
			b.mu.Unlock()
			_ = unix.Close(linkedPeer.fd)
			b.releaseAcquiredTransport(conn, path, rawFD, "duplicate_linked_pending_transport")
			return
		}
		b.pending[peerPath] = linkedPeer
	}
	peer, pairErr := b.findPendingPeerLocked(held)
	if pairErr != nil {
		delete(b.pending, path)
		if peer != nil {
			delete(b.pending, peer.path)
		}
		b.mu.Unlock()
		b.releasePendingTransports(conn, "invalid_transport_pair", true,
			held, peer)
		b.logger.Printf("le audio transport pair rejected reason=%T", pairErr)
		return
	}
	if peer == nil {
		b.mu.Unlock()
		go b.expirePending(path, configuration.generation)
		return
	}
	delete(b.pending, held.path)
	delete(b.pending, peer.path)
	sink, source := held, peer
	if sink.config.direction != DirectionSink {
		sink, source = source, sink
	}
	b.pairing[sink.path] = true
	b.pairing[source.path] = true
	b.mu.Unlock()

	correlationTimeout := time.Duration(0)
	if b.requireToken {
		correlationTimeout = callCorrelationTimeout
	}
	callToken, callCorrelated := waitForCallToken(b.callToken, correlationTimeout)
	if b.requireToken && !callCorrelated {
		b.mu.Lock()
		b.clearPairingLocked(sink, source)
		b.mu.Unlock()
		b.releasePendingTransports(conn, "missing_call_correlation", true,
			sink, source)
		b.logger.Printf("le audio transport pair returned to BlueZ reason=missing_call_correlation")
		return
	}

	b.mu.Lock()
	if !b.pairCurrentLocked(conn, sink, source) {
		b.clearPairingLocked(sink, source)
		b.mu.Unlock()
		b.releasePendingTransports(conn, "stale_pair_after_correlation", true,
			sink, source)
		b.logger.Printf("le audio transport pair returned to BlueZ reason=stale_pair_after_correlation")
		return
	}
	sinkGeneration, generationErr := b.nextDescriptorGenerationLocked()
	if generationErr != nil {
		b.clearPairingLocked(sink, source)
		b.mu.Unlock()
		b.releasePendingTransports(conn, "sink_generation_failed", true,
			sink, source)
		b.logger.Printf("le audio transport generation failed reason=%T", generationErr)
		return
	}
	sourceGeneration, generationErr := b.nextDescriptorGenerationLocked()
	if generationErr != nil {
		b.clearPairingLocked(sink, source)
		b.mu.Unlock()
		b.releasePendingTransports(conn, "source_generation_failed", true,
			sink, source)
		b.logger.Printf("le audio transport generation failed reason=%T", generationErr)
		return
	}
	bundleID := sinkGeneration
	sinkDescriptor := descriptorForPending(sink, sinkGeneration, bundleID, callToken, callCorrelated)
	sourceDescriptor := descriptorForPending(source, sourceGeneration, bundleID, callToken, callCorrelated)
	lifecycleFD, publishErr := b.handoff.PublishPair(sink.fd, sinkDescriptor, source.fd, sourceDescriptor)
	var activeSession *activeTransportSession
	if publishErr == nil {
		b.acquired[sink.path] = true
		b.acquired[source.path] = true
		activeSession = activeSessionForHandoff(conn, sink, source,
			sinkDescriptor, sourceDescriptor, lifecycleFD, time.Now())
		b.activeSession = activeSession
	}
	b.clearPairingLocked(sink, source)
	b.mu.Unlock()
	_ = unix.Close(sink.fd)
	_ = unix.Close(source.fd)
	if publishErr != nil {
		b.releasePendingTransports(conn, "handoff_publish_failed", false,
			sink, source)
		b.logger.Printf("le audio transport pair returned to BlueZ reason=%T", publishErr)
		return
	}
	b.logger.Printf("le audio bidirectional CIS handed off rate=%d duration_us=%d cig=%d cis=%d",
		sink.config.codec.SampleRate, sink.config.codec.FrameDuration/time.Microsecond, sink.qos.CIG, sink.qos.CIS)
	go b.watchActiveSession(activeSession)
}

func linkedAcquirePlan(path dbus.ObjectPath, links []dbus.ObjectPath) (dbus.ObjectPath, bool, bool, error) {
	if len(links) == 0 {
		return "", false, true, nil
	}
	if len(links) != 1 || links[0] == "" || links[0] == path {
		return "", false, false, errors.New("invalid linked transport set")
	}
	peer := links[0]
	return peer, true, string(path) < string(peer), nil
}

func duplicateLinkedFD(fd int) (int, error) {
	duplicate, err := unix.Dup(fd)
	if err != nil {
		return -1, err
	}
	unix.CloseOnExec(duplicate)
	return duplicate, nil
}

func boundedDBusError(err error, expectedDevice, expectedAddress string) (string, string) {
	if err == nil {
		return "none", ""
	}
	name := "local.error"
	message := err.Error()
	switch value := err.(type) {
	case dbus.Error:
		name = value.Name
		if len(value.Body) > 0 {
			if text, ok := value.Body[0].(string); ok {
				message = text
			}
		}
	case *dbus.Error:
		name = value.Name
		if len(value.Body) > 0 {
			if text, ok := value.Body[0].(string); ok {
				message = text
			}
		}
	}
	if expectedDevice != "" {
		message = strings.ReplaceAll(message, expectedDevice, "[device]")
	}
	if expectedAddress != "" {
		message = strings.ReplaceAll(message, expectedAddress, "[address]")
	}
	return name, message
}

func invokeTransportRelease(conn *dbus.Conn, path dbus.ObjectPath) error {
	if conn == nil || path == "" {
		return errors.New("missing transport release target")
	}
	return conn.Object(bluezBusName, path).Call(transportInterface+".Release", 0).Err
}

func closeTransportOwnerConnection(conn *dbus.Conn) error {
	if conn == nil {
		return errors.New("missing transport owner connection")
	}
	return conn.Close()
}

func (b *Broker) releaseMediaTransport(conn *dbus.Conn, path dbus.ObjectPath,
	reason string) error {
	b.mu.Lock()
	b.releaseAttempts++
	invoker := b.releaseInvoker
	expectedDevice := b.expectedDevice
	expectedAddress := b.expectedAddress
	logger := b.logger
	b.mu.Unlock()
	if invoker == nil {
		invoker = invokeTransportRelease
	}
	err := invoker(conn, path)
	b.mu.Lock()
	if err == nil {
		b.releaseSuccesses++
	} else {
		b.releaseErrors++
	}
	b.mu.Unlock()
	if logger != nil {
		if err == nil {
			logger.Printf("le audio transport release reason=%s outcome=success", reason)
		} else {
			name, message := boundedDBusError(err, expectedDevice, expectedAddress)
			logger.Printf("le audio transport release reason=%s outcome=error error_name=%s error_message=%q",
				reason, name, message)
		}
	}
	return err
}

func (b *Broker) releaseTransportPaths(conn *dbus.Conn,
	paths []dbus.ObjectPath, reason string) error {
	succeeded := make(map[dbus.ObjectPath]bool, len(paths))
	for attempt := 0; attempt < transportReleaseRetries; attempt++ {
		allReleased := true
		for _, path := range paths {
			if succeeded[path] {
				continue
			}
			if err := b.releaseMediaTransport(conn, path, reason); err != nil {
				allReleased = false
			} else {
				succeeded[path] = true
			}
		}
		if allReleased {
			return nil
		}
		if attempt+1 < transportReleaseRetries && b.releaseRetryDelay > 0 {
			time.Sleep(b.releaseRetryDelay)
		}
	}
	return b.fallbackTransportOwner(conn, reason)
}

func (b *Broker) fallbackTransportOwner(conn *dbus.Conn, reason string) error {
	b.mu.Lock()
	fallback := b.releaseFallback
	logger := b.logger
	expectedDevice := b.expectedDevice
	expectedAddress := b.expectedAddress
	b.mu.Unlock()
	if fallback == nil {
		fallback = closeTransportOwnerConnection
	}
	err := fallback(conn)
	b.mu.Lock()
	b.releaseFallbacks++
	b.mu.Unlock()
	if logger != nil {
		if err == nil {
			logger.Printf("le audio transport owner fallback reason=%s outcome=connection_closed", reason)
		} else {
			name, message := boundedDBusError(err, expectedDevice, expectedAddress)
			logger.Printf("le audio transport owner fallback reason=%s outcome=error error_name=%s error_message=%q",
				reason, name, message)
		}
	}
	return err
}

func (b *Broker) releaseAcquiredTransport(conn *dbus.Conn,
	path dbus.ObjectPath, fd int, reason string) {
	_ = unix.Close(fd)
	_ = b.releaseTransportPaths(conn, []dbus.ObjectPath{path}, reason)
}

func pendingReleasePath(held *pendingTransport) dbus.ObjectPath {
	if held == nil {
		return ""
	}
	if held.release != "" {
		return held.release
	}
	return held.path
}

func pendingReleasePaths(transports ...*pendingTransport) []dbus.ObjectPath {
	seen := make(map[dbus.ObjectPath]bool, len(transports))
	paths := make([]dbus.ObjectPath, 0, len(transports))
	for _, held := range transports {
		path := pendingReleasePath(held)
		if path != "" && !seen[path] {
			seen[path] = true
			paths = append(paths, path)
		}
	}
	return paths
}

func signalActiveSessionDone(session *activeTransportSession) {
	if session == nil || session.lifecycleDone == nil {
		return
	}
	session.lifecycleDoneOnce.Do(func() { close(session.lifecycleDone) })
}

// The caller holds b.mu. The watcher owns lifecycleFD and closes it only after
// observing lifecycleDone. This avoids closing an fd in one goroutine after
// the kernel has already reused its integer in another session.
func (b *Broker) clearActiveSessionLocked(session *activeTransportSession) {
	if session == nil || b.activeSession != session {
		return
	}
	delete(b.acquired, session.sinkPath)
	delete(b.acquired, session.sourcePath)
	b.activeSession = nil
	signalActiveSessionDone(session)
}

func lifecycleProgressMatches(session *activeTransportSession,
	progress lifecycleProgress) bool {
	return session != nil && progress.BundleID == session.bundleID &&
		progress.SinkGeneration == session.sinkGeneration &&
		progress.SourceGeneration == session.sourceGeneration
}

func (b *Broker) recordLifecycleProgress(session *activeTransportSession,
	progress lifecycleProgress, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.activeSession != session || session.releasing || session.releaseExhausted {
		b.lifecycleStale++
		return false
	}
	if !lifecycleProgressMatches(session, progress) {
		b.lifecycleStale++
		return false
	}
	session.progressArmed = true
	session.lastProgress = now
	b.lifecycleProgress++
	return true
}

func (b *Broker) normalReleaseWaiting(session *activeTransportSession) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.activeSession == session && session.normalWaitStarted &&
		!session.releasing && !session.releaseExhausted
}

func (b *Broker) normalReleasePending(session *activeTransportSession) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.activeSession == session && session.normalEndSeen &&
		!session.releasing && !session.releaseExhausted
}

func (b *Broker) markNormalReleasePending(session *activeTransportSession,
	reason string, normalEndEvent bool) bool {
	if session == nil {
		return false
	}
	b.mu.Lock()
	if b.activeSession != session || session.releasing || session.releaseExhausted {
		b.mu.Unlock()
		return false
	}
	if normalEndEvent {
		b.lifecycleNormalEnds++
	}
	if session.normalEndSeen {
		b.mu.Unlock()
		return false
	}
	session.normalEndSeen = true
	logger := b.logger
	projectedWaitStart := int64(0)
	if session.progressArmed && !session.lastProgress.IsZero() && b.progressLease > 0 {
		projectedWaitStart = session.lastProgress.Add(b.progressLease).UnixNano()
	} else if !session.startedAt.IsZero() && b.progressStartLease > 0 {
		projectedWaitStart = session.startedAt.Add(b.progressStartLease).UnixNano()
	}
	b.mu.Unlock()
	if logger != nil {
		logger.Printf("le audio normal teardown pending reason=%s event_unix_ns=%d projected_wait_start_unix_ns=%d",
			reason, time.Now().UnixNano(), projectedWaitStart)
	}
	return true
}

func (b *Broker) beginNormalReleaseWait(session *activeTransportSession,
	reason string, normalEndEvent bool) bool {
	if session == nil {
		return false
	}
	b.mu.Lock()
	if b.activeSession != session || session.releasing || session.releaseExhausted {
		b.mu.Unlock()
		return false
	}
	if normalEndEvent {
		b.lifecycleNormalEnds++
	}
	if session.normalWaitStarted {
		b.mu.Unlock()
		return false
	}
	session.normalEndSeen = session.normalEndSeen || normalEndEvent
	wait := b.normalReleaseWait
	if wait <= 0 {
		logger := b.logger
		b.mu.Unlock()
		if logger != nil {
			logger.Printf("le audio normal teardown wait outcome=unconfigured severity=high")
		}
		b.requestSpecificSessionRelease(session, "normal_teardown_wait_unconfigured")
		return false
	}
	session.normalWaitStarted = true
	b.normalReleaseWaits++
	logger := b.logger
	b.mu.Unlock()
	if logger != nil {
		logger.Printf("le audio normal teardown wait reason=%s timeout_ms=%d event_unix_ns=%d",
			reason, wait/time.Millisecond, time.Now().UnixNano())
	}
	go b.waitNormalRelease(session, wait)
	return true
}

func (b *Broker) recordLifecycleNormalEnd(session *activeTransportSession,
	identity lifecycleProgress) bool {
	b.mu.Lock()
	if b.activeSession != session || session.releasing || session.releaseExhausted {
		b.lifecycleStale++
		b.mu.Unlock()
		return false
	}
	if !lifecycleProgressMatches(session, identity) {
		b.lifecycleStale++
		b.mu.Unlock()
		return false
	}
	b.mu.Unlock()
	return b.markNormalReleasePending(session, "asterisk_normal_end", true)
}

func (b *Broker) waitNormalRelease(session *activeTransportSession,
	wait time.Duration) {
	if wait <= 0 {
		wait = time.Nanosecond
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-session.lifecycleDone:
		return
	case <-timer.C:
	}
	b.forceNormalReleaseTimeout(session, wait)
}

func (b *Broker) forceNormalReleaseTimeout(session *activeTransportSession,
	wait time.Duration) bool {
	b.mu.Lock()
	if b.activeSession != session || !session.normalWaitStarted ||
		session.releasing || session.releaseExhausted {
		b.mu.Unlock()
		return false
	}
	session.releasing = true
	b.normalReleaseTimeouts++
	logger := b.logger
	b.mu.Unlock()
	if logger != nil {
		logger.Printf("le audio normal teardown outcome=forced_timeout timeout_ms=%d severity=high event_unix_ns=%d",
			wait/time.Millisecond, time.Now().UnixNano())
	}
	b.releaseActiveSession(session, "normal_teardown_timeout")
	return true
}

func (b *Broker) watchActiveSession(session *activeTransportSession) {
	if session == nil {
		return
	}
	lifecycleFD := session.lifecycleFD
	lifecycleOpen := lifecycleFD >= 0
	if lifecycleOpen {
		defer unix.Close(lifecycleFD)
	}
	pollTimeout := int(callCorrelationPoll / time.Millisecond)
	if pollTimeout < 1 {
		pollTimeout = 1
	}
	for {
		if session.lifecycleDone != nil {
			select {
			case <-session.lifecycleDone:
				return
			default:
			}
		}

		var poll []unix.PollFd
		if lifecycleOpen {
			poll = []unix.PollFd{{Fd: int32(lifecycleFD), Events: unix.POLLIN | unix.POLLRDHUP}}
		}
		if len(poll) == 0 {
			time.Sleep(callCorrelationPoll)
		} else {
			count, err := unix.Poll(poll, pollTimeout)
			if err != nil && !errors.Is(err, unix.EINTR) {
				b.requestSpecificSessionRelease(session, "lifecycle_poll_failed")
				return
			}
			if count > 0 && poll[0].Revents&(unix.POLLIN|unix.POLLRDHUP|unix.POLLHUP|unix.POLLERR|unix.POLLNVAL) != 0 {
				revents := poll[0].Revents
				peerClosed := false
				receiveFailed := false
				protocolFailed := false
				/* POLLIN and HUP commonly arrive together when Asterisk writes
				 * normal-end and immediately closes. Drain every queued packet
				 * through EOF before classifying the close. */
				for {
					packet := make([]byte, lifecycleMessageSize)
					n, _, flags, _, recvErr := unix.Recvmsg(lifecycleFD,
						packet, nil, unix.MSG_DONTWAIT)
					if errors.Is(recvErr, unix.EAGAIN) || errors.Is(recvErr, unix.EWOULDBLOCK) {
						break
					}
					if n == 0 && recvErr == nil {
						peerClosed = true
						break
					}
					if recvErr != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
						receiveFailed = true
						break
					}
					typeID, identity, decodeErr := decodeLifecycleMessage(packet[:n])
					if decodeErr != nil {
						protocolFailed = true
						break
					}
					switch typeID {
					case lifecycleTypeProgress:
						b.recordLifecycleProgress(session, identity, time.Now())
					case lifecycleTypeNormalEnd:
						b.recordLifecycleNormalEnd(session, identity)
					}
				}
				if receiveFailed {
					b.requestSpecificSessionRelease(session, "lifecycle_receive_failed")
					return
				}
				if protocolFailed {
					b.requestSpecificSessionRelease(session, "lifecycle_protocol_error")
					return
				}
				if revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
					b.requestSpecificSessionRelease(session, "lifecycle_poll_terminal")
					return
				}
				if peerClosed || revents&(unix.POLLRDHUP|unix.POLLHUP) != 0 {
					b.mu.Lock()
					if b.activeSession == session {
						b.lifecycleHUPs++
					}
					b.mu.Unlock()
					if b.normalReleasePending(session) {
						lifecycleOpen = false
					} else {
						b.requestSpecificSessionRelease(session, "asterisk_lifecycle_closed")
						return
					}
				}
			}
		}

		b.mu.Lock()
		current := b.activeSession == session && !session.releaseExhausted
		provider := b.callToken
		startLease := b.progressStartLease
		lease := b.progressLease
		armed := session.progressArmed
		startedAt := session.startedAt
		lastProgress := session.lastProgress
		logger := b.logger
		b.mu.Unlock()
		if !current {
			return
		}
		if b.normalReleaseWaiting(session) {
			continue
		}
		if !armed && startLease > 0 && !startedAt.IsZero() &&
			time.Since(startedAt) >= startLease {
			if b.normalReleasePending(session) {
				b.beginNormalReleaseWait(session, "media_progress_start_stopped", false)
				continue
			}
			b.mu.Lock()
			if b.activeSession == session && !session.releasing && !session.releaseExhausted {
				b.lifecycleStartTimeouts++
			}
			b.mu.Unlock()
			if logger != nil {
				logger.Printf("le audio media lease expired phase=startup reason=no_media_progress")
			}
			b.requestSpecificSessionRelease(session, "media_progress_start_timeout")
			return
		}
		if armed && lease > 0 && time.Since(lastProgress) >= lease {
			if b.normalReleasePending(session) {
				b.beginNormalReleaseWait(session, "media_progress_stopped", false)
				continue
			}
			b.mu.Lock()
			if b.activeSession == session && !session.releasing && !session.releaseExhausted {
				b.lifecycleTimeouts++
			}
			b.mu.Unlock()
			if logger != nil {
				logger.Printf("le audio media lease expired phase=active reason=no_media_progress")
			}
			b.requestSpecificSessionRelease(session, "media_progress_lease_expired")
			return
		}
		if session.callToken != 0 {
			token, present := uint64(0), false
			if provider != nil {
				token, present = provider()
			}
			if !present || token != session.callToken {
				b.markNormalReleasePending(session, "call_token_ended", false)
				continue
			}
		}
	}
}

func (b *Broker) requestActiveSessionRelease(reason string) bool {
	b.mu.Lock()
	session := b.activeSession
	b.mu.Unlock()
	return b.requestSpecificSessionRelease(session, reason)
}

func (b *Broker) handleLEBearerDisconnected() bool {
	return b.requestActiveSessionRelease("le_bearer_disconnected")
}

// ReleaseCallToken marks a normal channel termination. The token prevents an
// old channel from changing a newer call that reused the same transports. The
// D-Bus owner is kept only for the bounded remote-IDLE window; abnormal HUP,
// lease, protocol, and bearer failures still bypass this wait.
func (b *Broker) ReleaseCallToken(token uint64) bool {
	if token == 0 {
		return false
	}
	b.mu.Lock()
	session := b.activeSession
	b.mu.Unlock()
	if session == nil || session.callToken != token {
		return false
	}
	return b.markNormalReleasePending(session, "asterisk_terminate_requested", false)
}

func (b *Broker) requestSpecificSessionRelease(session *activeTransportSession,
	reason string) bool {
	if session == nil {
		return false
	}
	b.mu.Lock()
	if b.activeSession != session || session.releasing || session.releaseExhausted {
		b.mu.Unlock()
		return false
	}
	session.releasing = true
	b.mu.Unlock()
	go b.releaseActiveSession(session, reason)
	return true
}

func (b *Broker) releaseActiveSession(session *activeTransportSession,
	reason string) {
	err := b.releaseTransportPaths(session.connection, session.releasePaths, reason)
	if err != nil {
		b.mu.Lock()
		if b.activeSession == session {
			if session.remoteIdleSeen {
				b.transportIdleComplete++
				b.clearActiveSessionLocked(session)
			} else {
				session.releasing = false
				session.releaseExhausted = true
			}
		}
		b.mu.Unlock()
		return
	}
	b.completeActiveSession(session)
}

func (b *Broker) completeActiveSession(session *activeTransportSession) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clearActiveSessionLocked(session)
}

func waitForCallToken(provider func() (uint64, bool), timeout time.Duration) (uint64, bool) {
	deadline := time.Now().Add(timeout)
	for {
		if provider != nil {
			token, correlated := provider()
			if correlated && token != 0 {
				return token, true
			}
		}
		if timeout <= 0 || !time.Now().Before(deadline) {
			return 0, false
		}
		time.Sleep(callCorrelationPoll)
	}
}

// The caller holds b.mu. A cleared or reconfigured transport is never allowed
// to inherit an older pair's reservation.
func (b *Broker) pairCurrentLocked(conn *dbus.Conn, first, second *pendingTransport) bool {
	if b.connection != conn || !b.pairing[first.path] || !b.pairing[second.path] {
		return false
	}
	firstConfig, firstPresent := b.configured[first.path]
	secondConfig, secondPresent := b.configured[second.path]
	return firstPresent && secondPresent &&
		firstConfig.generation == first.config.generation &&
		secondConfig.generation == second.config.generation
}

// The caller holds b.mu.
func (b *Broker) clearPairingLocked(first, second *pendingTransport) {
	for _, held := range []*pendingTransport{first, second} {
		configuration, present := b.configured[held.path]
		if present && configuration.generation == held.config.generation {
			delete(b.pairing, held.path)
		}
	}
}

func readTransportDetails(conn *dbus.Conn, path dbus.ObjectPath, codec CodecConfig, selectedQoS TransportQoS) (TransportQoS, []dbus.ObjectPath, error) {
	properties := make(map[string]dbus.Variant)
	call := conn.Object(bluezBusName, path).Call(propertiesInterface+".GetAll", 0, transportInterface)
	if call.Err != nil {
		return TransportQoS{}, nil, newTransportDetailError("transport_properties_read", fmt.Errorf("read final transport properties: %w", call.Err))
	}
	if err := call.Store(&properties); err != nil {
		return TransportQoS{}, nil, newTransportDetailError("transport_properties_decode", fmt.Errorf("decode final transport properties: %w", err))
	}
	qosMap, ok := properties["QoS"].Value().(map[string]dbus.Variant)
	if !ok {
		return TransportQoS{}, nil, newTransportDetailError("transport_qos_missing", errors.New("final transport QoS missing"))
	}
	qos, err := parseFinalTransportQoS(qosMap, codec, selectedQoS)
	if err != nil {
		return TransportQoS{}, nil, err
	}
	var links []dbus.ObjectPath
	if value, present := properties["Links"]; present {
		var valid bool
		links, valid = value.Value().([]dbus.ObjectPath)
		if !valid {
			return TransportQoS{}, nil, newTransportDetailError("transport_links_invalid", errors.New("invalid transport Links property"))
		}
	}
	return qos, append([]dbus.ObjectPath(nil), links...), nil
}

func readTransportLinks(conn *dbus.Conn, path dbus.ObjectPath) ([]dbus.ObjectPath, error) {
	var value dbus.Variant
	call := conn.Object(bluezBusName, path).Call(propertiesInterface+".Get", 0,
		transportInterface, "Links")
	if call.Err != nil {
		return nil, fmt.Errorf("read transport Links: %w", call.Err)
	}
	if err := call.Store(&value); err != nil {
		return nil, fmt.Errorf("decode transport Links: %w", err)
	}
	links, ok := value.Value().([]dbus.ObjectPath)
	if !ok {
		return nil, errors.New("invalid transport Links")
	}
	return append([]dbus.ObjectPath(nil), links...), nil
}

func (b *Broker) findPendingPeerLocked(current *pendingTransport) (*pendingTransport, error) {
	for _, candidate := range b.pending {
		if candidate.path == current.path || candidate.config.direction == current.config.direction {
			continue
		}
		if err := validateTransportPair(current, candidate); err != nil {
			return candidate, err
		}
		return candidate, nil
	}
	return nil, nil
}

func validateTransportPair(first, second *pendingTransport) error {
	if first == nil || second == nil || first.config.direction == second.config.direction {
		return errors.New("LE Audio pair directions are incomplete")
	}
	a, b := first.config.codec, second.config.codec
	if a.SampleRate != b.SampleRate || a.FrameDuration != b.FrameDuration || a.OctetsPerFrame != b.OctetsPerFrame {
		return errors.New("LE Audio pair codec mismatch")
	}
	qa, qb := first.qos, second.qos
	if qa.CIG != qb.CIG || qa.CIS != qb.CIS || qa.IntervalUS != qb.IntervalUS ||
		qa.Framing != qb.Framing || qa.PHY != qb.PHY {
		return errors.New("LE Audio pair is not one bidirectional CIS")
	}
	if len(first.links) > 0 && !containsObjectPath(first.links, second.path) {
		return errors.New("first LE Audio transport does not link its peer")
	}
	if len(second.links) > 0 && !containsObjectPath(second.links, first.path) {
		return errors.New("second LE Audio transport does not link its peer")
	}
	return nil
}

func containsObjectPath(paths []dbus.ObjectPath, want dbus.ObjectPath) bool {
	for _, path := range paths {
		if path == want {
			return true
		}
	}
	return false
}

func descriptorForPending(transport *pendingTransport, generation, bundleID, callToken uint64, callCorrelated bool) Descriptor {
	return Descriptor{
		Direction: transport.config.direction, Generation: generation, BundleID: bundleID,
		CallControlToken: callToken, CallControlCorrelated: callCorrelated,
		OwnershipTransferred: true, Linked: true,
		LifecycleOwned:    transport.config.direction == DirectionSink,
		SampleRate:        uint32(transport.config.codec.SampleRate),
		FrameDurationUS:   uint32(transport.config.codec.FrameDuration / time.Microsecond),
		OctetsPerFrame:    uint16(transport.config.codec.OctetsPerFrame),
		ChannelAllocation: transport.config.codec.ChannelAllocation,
		IntervalUS:        transport.qos.IntervalUS, PresentationDelayUS: transport.qos.PresentationDelayUS,
		ReadMTU: transport.readMTU, WriteMTU: transport.writeMTU, LatencyMS: transport.qos.LatencyMS,
		Framing: transport.qos.Framing, PHY: transport.qos.PHY, Retransmissions: transport.qos.Retransmissions,
		TargetLatency: transport.qos.TargetLatency, CIG: transport.qos.CIG, CIS: transport.qos.CIS,
		Transport: string(transport.path),
	}
}

func (b *Broker) expirePending(path dbus.ObjectPath, generation uint64) {
	timer := time.NewTimer(pairAcquireTimeout)
	defer timer.Stop()
	<-timer.C
	b.mu.Lock()
	held := b.pending[path]
	conn := b.connection
	if held == nil || held.config.generation != generation {
		b.mu.Unlock()
		return
	}
	delete(b.pending, path)
	b.mu.Unlock()
	b.releasePendingTransports(conn, "unpaired_transport_expired", true, held)
	b.logger.Printf("le audio unpaired transport expired direction=%d", held.config.direction)
}

func (b *Broker) releasePendingTransports(conn *dbus.Conn, reason string,
	closeDescriptors bool, transports ...*pendingTransport) {
	for _, held := range transports {
		if held != nil && closeDescriptors {
			_ = unix.Close(held.fd)
		}
	}
	_ = b.releaseTransportPaths(conn, pendingReleasePaths(transports...), reason)
}

// The caller holds b.mu.
func (b *Broker) closePendingLocked() {
	for path, held := range b.pending {
		_ = unix.Close(held.fd)
		delete(b.pending, path)
	}
}

// Descriptor generations use CLOCK_BOOTTIME so they remain monotonic across a
// broker-only restart while Asterisk and its anti-replay state stay alive.
// The caller holds b.mu.
func (b *Broker) nextDescriptorGenerationLocked() (uint64, error) {
	var now unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &now); err != nil {
		return 0, fmt.Errorf("read LE Audio generation clock: %w", err)
	}
	if now.Sec < 0 || now.Nsec < 0 {
		return 0, errors.New("invalid LE Audio generation clock")
	}
	generation := uint64(now.Sec)*uint64(time.Second) + uint64(now.Nsec)
	if generation <= b.lastGen {
		if b.lastGen == ^uint64(0) {
			return 0, errors.New("LE Audio descriptor generation exhausted")
		}
		generation = b.lastGen + 1
	}
	b.lastGen = generation
	return generation, nil
}

func selectChannelAllocation(properties map[string]dbus.Variant) (uint32, error) {
	for _, key := range []string{"ChannelAllocation", "Locations"} {
		value, present := properties[key]
		if !present {
			continue
		}
		locations, ok := variantUint32(value)
		if !ok || locations == 0 {
			return 0, fmt.Errorf("invalid LC3 %s", key)
		}
		if locations&FrontLeftLocation != 0 {
			return FrontLeftLocation, nil
		}
		return locations & (^locations + 1), nil
	}
	return FrontLeftLocation, nil
}

func selectQoS(properties map[string]dbus.Variant, codec CodecConfig) (TransportQoS, error) {
	interval := uint32(codec.FrameDuration / time.Microsecond)
	latency := uint16(10)
	if interval == 7500 {
		latency = 8
	}
	selected := TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: interval, Framing: 0, PHY: 0x02,
		SDU: uint16(codec.OctetsPerFrame), Retransmissions: 2, LatencyMS: latency,
		PresentationDelayUS: 40000, TargetLatency: 2,
	}
	value, present := properties["QoS"]
	if !present {
		return selected, nil
	}
	bounds, ok := value.Value().(map[string]dbus.Variant)
	if !ok {
		return TransportQoS{}, errors.New("invalid remote LC3 QoS capabilities")
	}
	if value, present := bounds["Framing"]; present {
		framing, ok := variantByte(value)
		if !ok || framing > 1 {
			return TransportQoS{}, errors.New("invalid remote LC3 framing capability")
		}
		selected.Framing = framing
	}
	if value, present := bounds["PHY"]; present {
		phy, ok := variantByte(value)
		if !ok || phy&0x02 == 0 {
			return TransportQoS{}, errors.New("remote endpoint does not support mandatory LE 2M PHY")
		}
	}
	if value, present := bounds["MaximumLatency"]; present {
		maximum, ok := variantUint16(value)
		if !ok || maximum < selected.LatencyMS {
			return TransportQoS{}, errors.New("remote endpoint maximum latency is below TMAP LC3 preset")
		}
	}
	if value, present := bounds["Retransmissions"]; present {
		retransmissions, ok := variantByte(value)
		if !ok || retransmissions == 0 || retransmissions > 13 {
			return TransportQoS{}, errors.New("invalid remote retransmission preference")
		}
		selected.Retransmissions = retransmissions
	}
	delay, err := selectPresentationDelay(bounds, selected.PresentationDelayUS)
	if err != nil {
		return TransportQoS{}, err
	}
	selected.PresentationDelayUS = delay
	return selected, nil
}

func selectPresentationDelay(bounds map[string]dbus.Variant, preferred uint32) (uint32, error) {
	minimum, maximum := uint32(1), uint32(0x00ffffff)
	if value, present := bounds["MinimumDelay"]; present {
		var ok bool
		minimum, ok = variantDelay(value)
		if !ok || minimum == 0 || minimum > 0x00ffffff {
			return 0, errors.New("invalid minimum presentation delay")
		}
	}
	if value, present := bounds["MaximumDelay"]; present {
		var ok bool
		maximum, ok = variantDelay(value)
		if !ok || maximum == 0 || maximum > 0x00ffffff {
			return 0, errors.New("invalid maximum presentation delay")
		}
	}
	if minimum > maximum {
		return 0, errors.New("remote presentation delay range is inverted")
	}
	preferredMinimum, hasPreferredMinimum := uint32(0), false
	if value, present := bounds["PreferredMinimumDelay"]; present {
		var ok bool
		preferredMinimum, ok = variantDelay(value)
		if !ok || preferredMinimum > 0x00ffffff {
			return 0, errors.New("invalid preferred minimum presentation delay")
		}
		hasPreferredMinimum = preferredMinimum != 0
	}
	preferredMaximum, hasPreferredMaximum := uint32(0), false
	if value, present := bounds["PreferredMaximumDelay"]; present {
		var ok bool
		preferredMaximum, ok = variantDelay(value)
		if !ok || preferredMaximum > 0x00ffffff {
			return 0, errors.New("invalid preferred maximum presentation delay")
		}
		hasPreferredMaximum = preferredMaximum != 0
	}
	if hasPreferredMinimum && preferredMinimum > minimum {
		minimum = preferredMinimum
	}
	if hasPreferredMaximum && preferredMaximum < maximum {
		maximum = preferredMaximum
	}
	if minimum > maximum {
		return 0, errors.New("remote preferred presentation delay has no overlap")
	}
	if preferred < minimum {
		return minimum, nil
	}
	if preferred > maximum {
		return maximum, nil
	}
	return preferred, nil
}

func qosVariants(qos TransportQoS) map[string]dbus.Variant {
	return map[string]dbus.Variant{
		"CIG": dbus.MakeVariant(qos.CIG), "CIS": dbus.MakeVariant(qos.CIS),
		"Interval": dbus.MakeVariant(qos.IntervalUS), "Framing": dbus.MakeVariant(qos.Framing),
		"PHY": dbus.MakeVariant(qos.PHY), "SDU": dbus.MakeVariant(qos.SDU),
		"Retransmissions": dbus.MakeVariant(qos.Retransmissions), "Latency": dbus.MakeVariant(qos.LatencyMS),
		"PresentationDelay": dbus.MakeVariant(qos.PresentationDelayUS), "TargetLatency": dbus.MakeVariant(qos.TargetLatency),
	}
}

// BlueZ's MediaEndpoint selection QoS includes TargetLatency, but BlueZ 5.87
// does not expose that field again in MediaTransport1.QoS. Carry forward only
// the already validated selected value when the final property omits it. Every
// property BlueZ does expose, including allocated CIG/CIS identifiers, remains
// subject to the strict final checks below.
func parseFinalTransportQoS(qos map[string]dbus.Variant, codec CodecConfig, selected TransportQoS) (TransportQoS, error) {
	if _, present := qos["TargetLatency"]; present {
		return parseConfiguredQoS(qos, codec, true)
	}
	if selected.TargetLatency < 1 || selected.TargetLatency > 3 {
		return TransportQoS{}, newQoSValidationError(
			"qos_target_latency_fallback_invalid",
			"selected LC3 QoS target latency outside bound",
		)
	}
	merged := make(map[string]dbus.Variant, len(qos)+1)
	for name, value := range qos {
		merged[name] = value
	}
	merged["TargetLatency"] = dbus.MakeVariant(selected.TargetLatency)
	return parseConfiguredQoS(merged, codec, true)
}

func parseConfiguredQoS(qos map[string]dbus.Variant, codec CodecConfig, final bool) (TransportQoS, error) {
	result := TransportQoS{CIG: 0xff, CIS: 0xff}
	var ok bool
	if value, present := qos["CIG"]; present {
		result.CIG, ok = variantByte(value)
		if !ok {
			return TransportQoS{}, newQoSValidationError("qos_cig_invalid", "invalid LC3 QoS CIG")
		}
	} else if final {
		return TransportQoS{}, newQoSValidationError("qos_cig_missing", "final LC3 QoS CIG missing")
	}
	if value, present := qos["CIS"]; present {
		result.CIS, ok = variantByte(value)
		if !ok {
			return TransportQoS{}, newQoSValidationError("qos_cis_invalid", "invalid LC3 QoS CIS")
		}
	} else if final {
		return TransportQoS{}, newQoSValidationError("qos_cis_missing", "final LC3 QoS CIS missing")
	}
	if final && (result.CIG > 0xef || result.CIS > 0xef) {
		return TransportQoS{}, newQoSValidationError("qos_ids_unallocated", "BlueZ did not allocate final CIG/CIS identifiers")
	}
	result.IntervalUS, ok = variantUint32(qos["Interval"])
	if !ok || result.IntervalUS != uint32(codec.FrameDuration/time.Microsecond) {
		return TransportQoS{}, newQoSValidationError("qos_interval_mismatch", "LC3 QoS interval does not match frame duration")
	}
	result.SDU, ok = variantUint16(qos["SDU"])
	if !ok || int(result.SDU) != codec.OctetsPerFrame {
		return TransportQoS{}, newQoSValidationError("qos_sdu_mismatch", "LC3 QoS SDU does not match frame")
	}
	result.Framing, ok = variantByte(qos["Framing"])
	if !ok || result.Framing > 1 {
		return TransportQoS{}, newQoSValidationError("qos_framing_invalid", "invalid LC3 QoS framing")
	}
	result.PHY, ok = variantByte(qos["PHY"])
	if !ok || result.PHY&0x02 == 0 {
		return TransportQoS{}, newQoSValidationError("qos_phy_not_le_2m", "LC3 QoS must use mandatory LE 2M PHY")
	}
	result.Retransmissions, ok = variantByte(qos["Retransmissions"])
	if !ok || result.Retransmissions == 0 || result.Retransmissions > 13 {
		return TransportQoS{}, newQoSValidationError("qos_retransmissions_invalid", "LC3 QoS retransmissions outside bound")
	}
	result.LatencyMS, ok = variantUint16(qos["Latency"])
	if !ok || result.LatencyMS == 0 || result.LatencyMS > 100 {
		return TransportQoS{}, newQoSValidationError("qos_latency_invalid", "LC3 QoS latency outside bound")
	}
	result.PresentationDelayUS, ok = variantUint32(qos["PresentationDelay"])
	if !ok || result.PresentationDelayUS == 0 || result.PresentationDelayUS > 0x00ffffff {
		return TransportQoS{}, newQoSValidationError("qos_presentation_delay_invalid", "LC3 QoS presentation delay outside bound")
	}
	result.TargetLatency, ok = variantByte(qos["TargetLatency"])
	if !ok || result.TargetLatency < 1 || result.TargetLatency > 3 {
		return TransportQoS{}, newQoSValidationError("qos_target_latency_invalid", "LC3 QoS target latency outside bound")
	}
	return result, nil
}

func validateQoS(qos map[string]dbus.Variant, codec CodecConfig) error {
	_, err := parseConfiguredQoS(qos, codec, false)
	return err
}

func variantBytes(value dbus.Variant) ([]byte, bool) {
	raw, ok := value.Value().([]byte)
	return raw, ok
}

func variantByte(value dbus.Variant) (byte, bool) {
	raw, ok := value.Value().(byte)
	return raw, ok
}

func variantUint16(value dbus.Variant) (uint16, bool) {
	raw, ok := value.Value().(uint16)
	return raw, ok
}

func variantUint32(value dbus.Variant) (uint32, bool) {
	raw, ok := value.Value().(uint32)
	return raw, ok
}

func variantDelay(value dbus.Variant) (uint32, bool) {
	if raw, ok := value.Value().(uint32); ok {
		return raw, true
	}
	if raw, ok := value.Value().(uint16); ok {
		return uint32(raw), true
	}
	return 0, false
}

func invalidArguments(err error) *dbus.Error {
	return dbus.NewError("org.bluez.Error.InvalidArguments", []interface{}{err.Error()})
}
