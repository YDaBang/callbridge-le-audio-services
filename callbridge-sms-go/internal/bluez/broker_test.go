package bluez

import (
	"bytes"
	"errors"
	"log"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
	"golang.org/x/sys/unix"
)

func TestLinkedAcquirePlanElectsExactlyOneTransport(t *testing.T) {
	first := dbus.ObjectPath("/org/bluez/hci0/dev_test/pac_sink/fd0")
	second := dbus.ObjectPath("/org/bluez/hci0/dev_test/pac_source/fd1")

	peer, linked, leader, err := linkedAcquirePlan(first, []dbus.ObjectPath{second})
	if err != nil || !linked || !leader || peer != second {
		t.Fatalf("first plan peer=%q linked=%t leader=%t err=%v", peer, linked, leader, err)
	}
	peer, linked, leader, err = linkedAcquirePlan(second, []dbus.ObjectPath{first})
	if err != nil || !linked || leader || peer != first {
		t.Fatalf("second plan peer=%q linked=%t leader=%t err=%v", peer, linked, leader, err)
	}
	if _, _, _, err := linkedAcquirePlan(first, []dbus.ObjectPath{first}); err == nil {
		t.Fatal("accepted a self-linked transport")
	}
	if _, _, _, err := linkedAcquirePlan(first, []dbus.ObjectPath{second, "/third"}); err == nil {
		t.Fatal("accepted more than one linked peer")
	}
}

func TestDuplicateLinkedFDProvidesOneBidirectionalSocketToTwoDirections(t *testing.T) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(pair[0])
	defer unix.Close(pair[1])

	duplicate, err := duplicateLinkedFD(pair[0])
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(duplicate)
	if duplicate == pair[0] {
		t.Fatal("duplicate reused the original descriptor number")
	}

	if _, err := unix.Write(pair[1], []byte("incoming")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 16)
	count, err := unix.Read(duplicate, buffer)
	if err != nil || string(buffer[:count]) != "incoming" {
		t.Fatalf("duplicate read count=%d value=%q err=%v", count, buffer[:count], err)
	}
	if _, err := unix.Write(pair[0], []byte("outgoing")); err != nil {
		t.Fatal(err)
	}
	count, err = unix.Read(pair[1], buffer)
	if err != nil || string(buffer[:count]) != "outgoing" {
		t.Fatalf("original write count=%d value=%q err=%v", count, buffer[:count], err)
	}
}

func TestBoundedDBusErrorPreservesNameAndMessageButRedactsIdentity(t *testing.T) {
	err := dbus.Error{
		Name: "org.bluez.Error.NotAuthorized",
		Body: []interface{}{"Operation Not Authorized for /org/bluez/hci0/dev_TEST and 00:11:22:33:44:55"},
	}
	name, message := boundedDBusError(err, "/org/bluez/hci0/dev_TEST", "00:11:22:33:44:55")
	if name != "org.bluez.Error.NotAuthorized" {
		t.Fatalf("name=%q", name)
	}
	if message != "Operation Not Authorized for [device] and [address]" {
		t.Fatalf("message=%q", message)
	}
}

func TestEndpointPropertiesAdvertiseTMAPCallTerminalRole(t *testing.T) {
	properties := endpointProperties(PACSinkUUID)
	if properties["Locations"].Value() != FrontLeftLocation ||
		properties["SupportedContext"].Value() != SupportedContexts ||
		properties["Context"].Value() != SupportedContexts {
		t.Fatalf("endpoint location/context=%#v", properties)
	}
	metadata, ok := variantBytes(properties["Metadata"])
	if !ok || !reflect.DeepEqual(metadata, []byte{0x03, 0x01, 0x02, 0x00}) {
		t.Fatalf("preferred conversational metadata=%x", metadata)
	}
	features, ok := properties["SupportedFeatures"].Value().(map[string]dbus.Variant)
	if !ok {
		t.Fatal("SupportedFeatures is not an a{sv} dictionary")
	}
	if signature := dbus.SignatureOf(features).String(); signature != "a{sv}" {
		t.Fatalf("SupportedFeatures signature=%q", signature)
	}
	roles, ok := features[TMASUUID].Value().([]string)
	if !ok || !reflect.DeepEqual(roles, []string{TMAPRoleCT}) {
		t.Fatalf("TMAP roles=%#v", roles)
	}
	if signature := dbus.SignatureOf(roles).String(); signature != "as" {
		t.Fatalf("TMAP role signature=%q", signature)
	}
}

func TestEndpointPropertiesAdvertiseFrontLeftForBothPACDirections(t *testing.T) {
	for _, uuid := range []string{PACSinkUUID, PACSourceUUID} {
		properties := endpointProperties(uuid)
		if got := properties["Locations"].Value(); got != FrontLeftLocation {
			t.Fatalf("uuid=%s locations=%#v", uuid, got)
		}
	}
}

func TestSelectChannelAllocationPrefersFrontLeft(t *testing.T) {
	allocation, err := selectChannelAllocation(map[string]dbus.Variant{
		"Locations": dbus.MakeVariant(FrontLeftLocation | FrontCenterLocation),
	})
	if err != nil {
		t.Fatal(err)
	}
	if allocation != FrontLeftLocation {
		t.Fatalf("allocation=%#x", allocation)
	}
	allocation, err = selectChannelAllocation(nil)
	if err != nil || allocation != FrontLeftLocation {
		t.Fatalf("fallback allocation=%#x err=%v", allocation, err)
	}
}

func TestEndpointSelectPropertiesCarriesFrontLeftAllocation(t *testing.T) {
	e := &endpoint{direction: DirectionSink}
	selected, dbusErr := e.SelectProperties(map[string]dbus.Variant{
		"Capabilities": dbus.MakeVariant(append([]byte(nil), LC3Capabilities...)),
		"Locations":    dbus.MakeVariant(FrontLeftLocation),
	})
	if dbusErr != nil {
		t.Fatal(dbusErr)
	}
	configuration, ok := variantBytes(selected["Capabilities"])
	if !ok {
		t.Fatal("configuration missing")
	}
	codec, err := ParseConfiguration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if codec.ChannelAllocation != FrontLeftLocation {
		t.Fatalf("selected allocation=%#x", codec.ChannelAllocation)
	}
}

func TestAdvertisementIsVisibleAndConnectable(t *testing.T) {
	properties := advertisementProperties()[advertisementIface]
	if properties["Type"].Value != "peripheral" {
		t.Fatalf("advertisement type=%#v", properties["Type"].Value)
	}
	uuids, ok := properties["ServiceUUIDs"].Value.([]string)
	if !ok || !reflect.DeepEqual(uuids, []string{ASCSUUID, PACSUUID, CASUUID, TMASUUID, VCSUUID}) {
		t.Fatalf("advertisement UUIDs=%#v", uuids)
	}
	if discoverable, ok := properties["Discoverable"].Value.(bool); !ok || !discoverable {
		t.Fatalf("advertisement discoverable=%#v", properties["Discoverable"].Value)
	}
	if timeout, ok := properties["DiscoverableTimeout"].Value.(uint16); !ok || timeout != 0 {
		t.Fatalf("advertisement discoverable timeout=%#v", properties["DiscoverableTimeout"].Value)
	}
	if name, ok := properties["LocalName"].Value.(string); !ok || name != "Callbridge-Asterisk" {
		t.Fatalf("advertisement local name=%#v", properties["LocalName"].Value)
	}
	serviceData, ok := properties["ServiceData"].Value.(map[string]dbus.Variant)
	if !ok || dbus.SignatureOf(serviceData).String() != "a{sv}" {
		t.Fatalf("advertisement service data=%#v", properties["ServiceData"].Value)
	}
	announcement, ok := variantBytes(serviceData[ASCSUUID])
	if !ok || !reflect.DeepEqual(announcement, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00}) {
		t.Fatalf("BAP targeted idle-reconnect announcement=%#v", announcement)
	}
	capAnnouncement, ok := variantBytes(serviceData[CASUUID])
	if !ok || !reflect.DeepEqual(capAnnouncement, []byte{0x01}) {
		t.Fatalf("CAP targeted announcement=%#v", capAnnouncement)
	}
	tmapRole, ok := variantBytes(serviceData[TMASUUID])
	if !ok || !reflect.DeepEqual(tmapRole, []byte{0x02, 0x00}) {
		t.Fatalf("TMAP CT announcement=%#v", tmapRole)
	}
	if appearance, ok := properties["Appearance"].Value.(uint16); !ok || appearance != HeadsetAppearance {
		t.Fatalf("headset appearance=%#v", properties["Appearance"].Value)
	}
	if secondary, ok := properties["SecondaryChannel"].Value.(string); !ok || secondary != "1M" {
		t.Fatalf("advertisement secondary channel=%#v", properties["SecondaryChannel"].Value)
	}
}

func TestLEBearerConnectionChangedIsExact(t *testing.T) {
	expected := "/org/bluez/hci0/dev_00_11_22_33_44_55"
	signal := &dbus.Signal{
		Name: propertiesInterface + ".PropertiesChanged",
		Path: dbus.ObjectPath(expected),
		Body: []interface{}{
			leBearerInterface,
			map[string]dbus.Variant{"Connected": dbus.MakeVariant(true)},
			[]string{},
		},
	}
	if connected, matched := leBearerConnectionChanged(signal, expected); !matched || !connected {
		t.Fatalf("connected=%t matched=%t", connected, matched)
	}
	signal.Path = dbus.ObjectPath(expected + "/sep1")
	if _, matched := leBearerConnectionChanged(signal, expected); matched {
		t.Fatal("accepted a child transport path as the LE bearer")
	}
}

func TestSelectDevicePathUsesStableAddressProperty(t *testing.T) {
	want := dbus.ObjectPath("/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2")
	objects := managedObjects{
		want: {deviceInterface: {
			"Address": dbus.MakeVariant("00:11:22:33:44:55"),
			"Adapter": dbus.MakeVariant(dbus.ObjectPath("/org/bluez/hci0")),
		}},
	}
	got, err := selectDevicePath(objects, "hci0", "6c:ac:c2:0d:40:88")
	if err != nil || got != want {
		t.Fatalf("path=%q err=%v", got, err)
	}
}

func TestEndpointRejectionLogsOnlyStageAndBoundedReason(t *testing.T) {
	var output bytes.Buffer
	endpoint := &endpoint{broker: &Broker{logger: log.New(&output, "", 0)}}
	if _, dbusErr := endpoint.SelectProperties(map[string]dbus.Variant{}); dbusErr == nil {
		t.Fatal("accepted missing capabilities")
	}
	got := output.String()
	if !strings.Contains(got, "method=SelectProperties stage=capabilities reason=LC3 capabilities missing") {
		t.Fatalf("diagnostic=%q", got)
	}
}

func TestSetConfigurationDefersMissingQoSToMediaTransport(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	transport := dbus.ObjectPath(device + "/fd0")
	var output bytes.Buffer
	broker := &Broker{
		expectedDevice: device,
		logger:         log.New(&output, "", 0),
		configured:     make(map[dbus.ObjectPath]transportConfig),
		acquiring:      make(map[dbus.ObjectPath]bool),
		acquired:       make(map[dbus.ObjectPath]bool),
		pairing:        make(map[dbus.ObjectPath]bool),
		pending:        make(map[dbus.ObjectPath]*pendingTransport),
	}
	endpoint := &endpoint{broker: broker, direction: DirectionSink}
	if dbusErr := endpoint.SetConfiguration(transport, map[string]dbus.Variant{
		"Configuration": dbus.MakeVariant(configuration),
	}); dbusErr != nil {
		t.Fatal(dbusErr)
	}
	configured, ok := broker.configured[transport]
	if !ok {
		t.Fatal("transport was not configured")
	}
	wantQoS, err := selectQoS(nil, configured.codec)
	if err != nil {
		t.Fatal(err)
	}
	if configured.qos != wantQoS {
		t.Fatalf("provisional QoS=%#v want=%#v", configured.qos, wantQoS)
	}
	if !strings.Contains(output.String(), "endpoint deferred method=SetConfiguration stage=qos source=MediaTransport1") {
		t.Fatalf("diagnostic=%q", output.String())
	}
}

func TestSetConfigurationStillRejectsMalformedPresentQoS(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	var output bytes.Buffer
	broker := &Broker{
		expectedDevice: device,
		logger:         log.New(&output, "", 0),
		configured:     make(map[dbus.ObjectPath]transportConfig),
		pending:        make(map[dbus.ObjectPath]*pendingTransport),
	}
	endpoint := &endpoint{broker: broker, direction: DirectionSink}
	if dbusErr := endpoint.SetConfiguration(dbus.ObjectPath(device+"/fd0"), map[string]dbus.Variant{
		"Configuration": dbus.MakeVariant(configuration),
		"QoS":           dbus.MakeVariant("invalid"),
	}); dbusErr == nil {
		t.Fatal("accepted malformed present QoS")
	}
	if len(broker.configured) != 0 {
		t.Fatalf("configured transports=%#v", broker.configured)
	}
	if !strings.Contains(output.String(), "stage=qos reason=invalid LC3 QoS dictionary") {
		t.Fatalf("diagnostic=%q", output.String())
	}
}

func TestSetConfigurationDefersBoundedStaleSDUToFinalTransport(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	transport := dbus.ObjectPath(device + "/fd0")
	var output bytes.Buffer
	broker := &Broker{
		expectedDevice: device,
		logger:         log.New(&output, "", 0),
		configured:     make(map[dbus.ObjectPath]transportConfig),
		acquiring:      make(map[dbus.ObjectPath]bool),
		acquired:       make(map[dbus.ObjectPath]bool),
		pairing:        make(map[dbus.ObjectPath]bool),
		pending:        make(map[dbus.ObjectPath]*pendingTransport),
	}
	stale := qosVariants(TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 60,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	// BlueZ 5.87 MediaTransport1.QoS omits TargetLatency even though the
	// broker supplied it during endpoint selection.
	delete(stale, "TargetLatency")
	endpoint := &endpoint{broker: broker, direction: DirectionSink}
	if dbusErr := endpoint.SetConfiguration(transport, map[string]dbus.Variant{
		"Configuration": dbus.MakeVariant(configuration),
		"QoS":           dbus.MakeVariant(stale),
	}); dbusErr != nil {
		t.Fatal(dbusErr)
	}
	configured, ok := broker.configured[transport]
	if !ok || configured.codec.OctetsPerFrame != 80 || configured.qos.SDU != 80 {
		t.Fatalf("configured transport=%#v present=%t", configured, ok)
	}
	wantLog := "reason=stale_sdu codec_octets=80 qos_sdu=60 source=MediaTransport1"
	if !strings.Contains(output.String(), wantLog) {
		t.Fatalf("diagnostic=%q", output.String())
	}
}

func TestSetConfigurationAcceptsMatchingBlueZQoSWithoutTargetLatency(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	transport := dbus.ObjectPath(device + "/fd0")
	broker := &Broker{
		expectedDevice: device,
		logger:         log.New(&bytes.Buffer{}, "", 0),
		configured:     make(map[dbus.ObjectPath]transportConfig),
		pending:        make(map[dbus.ObjectPath]*pendingTransport),
	}
	matching := qosVariants(TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 80,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	delete(matching, "TargetLatency")
	endpoint := &endpoint{broker: broker, direction: DirectionSink}
	if dbusErr := endpoint.SetConfiguration(transport, map[string]dbus.Variant{
		"Configuration": dbus.MakeVariant(configuration),
		"QoS":           dbus.MakeVariant(matching),
	}); dbusErr != nil {
		t.Fatal(dbusErr)
	}
	configured, ok := broker.configured[transport]
	if !ok || configured.qos.SDU != 80 || configured.qos.TargetLatency != 2 {
		t.Fatalf("configured transport=%#v present=%t", configured, ok)
	}
}

func TestSetConfigurationRejectsInvalidPublishedTargetLatencyOnStaleSDU(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	broker := &Broker{
		expectedDevice: device,
		logger:         log.New(&bytes.Buffer{}, "", 0),
		configured:     make(map[dbus.ObjectPath]transportConfig),
		pending:        make(map[dbus.ObjectPath]*pendingTransport),
	}
	stale := qosVariants(TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 60,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 0,
	})
	endpoint := &endpoint{broker: broker, direction: DirectionSink}
	if dbusErr := endpoint.SetConfiguration(dbus.ObjectPath(device+"/fd0"), map[string]dbus.Variant{
		"Configuration": dbus.MakeVariant(configuration),
		"QoS":           dbus.MakeVariant(stale),
	}); dbusErr == nil {
		t.Fatal("accepted invalid published TargetLatency on stale QoS")
	}
	if len(broker.configured) != 0 {
		t.Fatalf("configured transports=%#v", broker.configured)
	}
}

func TestSetConfigurationDefersUnallocatedSDUToFinalTransport(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	transport := dbus.ObjectPath(device + "/fd0")
	var output bytes.Buffer
	broker := &Broker{
		expectedDevice: device,
		logger:         log.New(&output, "", 0),
		configured:     make(map[dbus.ObjectPath]transportConfig),
		acquiring:      make(map[dbus.ObjectPath]bool),
		acquired:       make(map[dbus.ObjectPath]bool),
		pairing:        make(map[dbus.ObjectPath]bool),
		pending:        make(map[dbus.ObjectPath]*pendingTransport),
	}
	unallocated := qosVariants(TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 0,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	delete(unallocated, "TargetLatency")
	endpoint := &endpoint{broker: broker, direction: DirectionSink}
	if dbusErr := endpoint.SetConfiguration(transport, map[string]dbus.Variant{
		"Configuration": dbus.MakeVariant(configuration),
		"QoS":           dbus.MakeVariant(unallocated),
	}); dbusErr != nil {
		t.Fatal(dbusErr)
	}
	configured, ok := broker.configured[transport]
	if !ok || configured.codec.OctetsPerFrame != 80 || configured.qos.SDU != 80 {
		t.Fatalf("configured transport=%#v present=%t", configured, ok)
	}
	wantLog := "reason=unallocated_sdu codec_octets=80 qos_sdu=0 qos_cig=255 qos_cis=255 source=MediaTransport1"
	if !strings.Contains(output.String(), wantLog) {
		t.Fatalf("diagnostic=%q", output.String())
	}
}

func TestSetConfigurationRejectsAllocatedOrUnsafeZeroSDU(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	tests := []struct {
		name   string
		mutate func(map[string]dbus.Variant)
	}{
		{name: "allocated identifiers", mutate: func(qos map[string]dbus.Variant) {
			qos["CIG"] = dbus.MakeVariant(byte(3))
			qos["CIS"] = dbus.MakeVariant(byte(4))
		}},
		{name: "partly allocated identifiers", mutate: func(qos map[string]dbus.Variant) {
			qos["CIS"] = dbus.MakeVariant(byte(4))
		}},
		{name: "missing identifier", mutate: func(qos map[string]dbus.Variant) {
			delete(qos, "CIG")
		}},
		{name: "unsafe PHY", mutate: func(qos map[string]dbus.Variant) {
			qos["PHY"] = dbus.MakeVariant(byte(1))
		}},
		{name: "wrong interval", mutate: func(qos map[string]dbus.Variant) {
			qos["Interval"] = dbus.MakeVariant(uint32(7500))
		}},
		{name: "zero presentation delay", mutate: func(qos map[string]dbus.Variant) {
			qos["PresentationDelay"] = dbus.MakeVariant(uint32(0))
		}},
		{name: "invalid published target latency", mutate: func(qos map[string]dbus.Variant) {
			qos["TargetLatency"] = dbus.MakeVariant(byte(0))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			broker := &Broker{
				expectedDevice: device,
				logger:         log.New(&bytes.Buffer{}, "", 0),
				configured:     make(map[dbus.ObjectPath]transportConfig),
				pending:        make(map[dbus.ObjectPath]*pendingTransport),
			}
			qos := qosVariants(TransportQoS{
				CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 0,
				Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
			})
			delete(qos, "TargetLatency")
			test.mutate(qos)
			endpoint := &endpoint{broker: broker, direction: DirectionSink}
			if dbusErr := endpoint.SetConfiguration(dbus.ObjectPath(device+"/fd0"), map[string]dbus.Variant{
				"Configuration": dbus.MakeVariant(configuration),
				"QoS":           dbus.MakeVariant(qos),
			}); dbusErr == nil {
				t.Fatal("accepted unsafe unallocated QoS")
			}
			if len(broker.configured) != 0 {
				t.Fatalf("configured transports=%#v", broker.configured)
			}
		})
	}
}

func TestSetConfigurationRejectsUnboundedOrUnsafeStaleSDU(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	for _, test := range []struct {
		name string
		sdu  uint16
		phy  byte
	}{
		{name: "unknown frame size", sdu: 79, phy: 2},
		{name: "unsafe PHY", sdu: 60, phy: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			broker := &Broker{
				expectedDevice: device,
				logger:         log.New(&bytes.Buffer{}, "", 0),
				configured:     make(map[dbus.ObjectPath]transportConfig),
				pending:        make(map[dbus.ObjectPath]*pendingTransport),
			}
			qos := qosVariants(TransportQoS{
				CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: test.phy, SDU: test.sdu,
				Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
			})
			endpoint := &endpoint{broker: broker, direction: DirectionSink}
			if dbusErr := endpoint.SetConfiguration(dbus.ObjectPath(device+"/fd0"), map[string]dbus.Variant{
				"Configuration": dbus.MakeVariant(configuration),
				"QoS":           dbus.MakeVariant(qos),
			}); dbusErr == nil {
				t.Fatal("accepted invalid stale QoS")
			}
			if len(broker.configured) != 0 {
				t.Fatalf("configured transports=%#v", broker.configured)
			}
		})
	}
}

func TestSetConfigurationLogsBoundedNumericEvidenceForRejectedSDUMismatch(t *testing.T) {
	configuration, err := SelectConfiguration(LC3Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	const device = "/org/bluez/hci0/dev_5B_3B_C8_E0_A8_C2"
	var output bytes.Buffer
	broker := &Broker{
		expectedDevice: device,
		logger:         log.New(&output, "", 0),
		configured:     make(map[dbus.ObjectPath]transportConfig),
		pending:        make(map[dbus.ObjectPath]*pendingTransport),
	}
	qos := qosVariants(TransportQoS{
		CIG: 3, CIS: 4, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 79,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	delete(qos, "TargetLatency")
	endpoint := &endpoint{broker: broker, direction: DirectionSink}
	if dbusErr := endpoint.SetConfiguration(dbus.ObjectPath(device+"/fd0"), map[string]dbus.Variant{
		"Configuration": dbus.MakeVariant(configuration),
		"QoS":           dbus.MakeVariant(qos),
	}); dbusErr == nil {
		t.Fatal("accepted unknown SDU mismatch")
	}
	want := "qos mismatch codec_octets=80 qos_sdu=79 qos_cig=3 qos_cis=4 source=MediaTransport1"
	if !strings.Contains(output.String(), want) {
		t.Fatalf("diagnostic=%q", output.String())
	}
}

func TestFinalQoSStillRejectsStaleSDU(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	qos := qosVariants(TransportQoS{
		CIG: 3, CIS: 4, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 60,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	if _, err := parseConfiguredQoS(qos, codec, true); err == nil || transportDetailReason(err) != "qos_sdu_mismatch" {
		t.Fatalf("final stale SDU err=%v reason=%s", err, transportDetailReason(err))
	}
}

func TestFinalQoSStillRejectsUnallocatedSDU(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	qos := qosVariants(TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 0,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	if _, err := parseConfiguredQoS(qos, codec, true); err == nil || transportDetailReason(err) != "qos_ids_unallocated" {
		t.Fatalf("final unallocated QoS err=%v reason=%s", err, transportDetailReason(err))
	}
}

func TestBrokerSnapshotSeparatesBothDirections(t *testing.T) {
	b := &Broker{
		registered: true, advertising: true, discoverable: true,
		configured: map[dbus.ObjectPath]transportConfig{
			"/sink":   {direction: DirectionSink},
			"/source": {direction: DirectionSource},
		},
		acquired: map[dbus.ObjectPath]bool{"/sink": true},
	}
	snapshot := b.Snapshot()
	if !snapshot.EndpointsRegistered || !snapshot.Advertising || !snapshot.Discoverable || !snapshot.ExtendedAdvertising ||
		!snapshot.BAPAnnouncement || !snapshot.CAPAnnouncement || !snapshot.TMAPAnnouncement || !snapshot.HeadsetAppearance ||
		!snapshot.SinkConfigured || !snapshot.SourceConfigured || !snapshot.SinkAcquired || snapshot.SourceAcquired || snapshot.BidirectionalCIS {
		t.Fatalf("snapshot=%#v", snapshot)
	}
}

func TestEndpointSelectPropertiesUsesBoundedConversationalQoS(t *testing.T) {
	e := &endpoint{direction: DirectionSink}
	selected, dbusErr := e.SelectProperties(map[string]dbus.Variant{
		"Capabilities":      dbus.MakeVariant(append([]byte(nil), LC3Capabilities...)),
		"ChannelAllocation": dbus.MakeVariant(FrontCenterLocation),
		"QoS": dbus.MakeVariant(map[string]dbus.Variant{
			"Framing": dbus.MakeVariant(byte(0)), "PHY": dbus.MakeVariant(byte(2)),
			"MaximumLatency": dbus.MakeVariant(uint16(10)),
			"MinimumDelay":   dbus.MakeVariant(uint32(20000)), "MaximumDelay": dbus.MakeVariant(uint32(60000)),
			"PreferredMinimumDelay": dbus.MakeVariant(uint32(30000)), "PreferredMaximumDelay": dbus.MakeVariant(uint32(50000)),
		}),
	})
	if dbusErr != nil {
		t.Fatal(dbusErr)
	}
	configuration, ok := variantBytes(selected["Capabilities"])
	if !ok {
		t.Fatal("configuration missing")
	}
	codec, err := ParseConfiguration(configuration)
	if err != nil || codec.SampleRate != 32000 {
		t.Fatalf("codec=%#v,%v", codec, err)
	}
	qos, ok := selected["QoS"].Value().(map[string]dbus.Variant)
	if !ok || validateQoS(qos, codec) != nil {
		t.Fatalf("invalid selected QoS: %#v", qos)
	}
}

func TestQoSRejectsShortDuplicateAndWrongSDU(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	valid := map[string]dbus.Variant{
		"Interval": dbus.MakeVariant(uint32(10000)), "SDU": dbus.MakeVariant(uint16(80)),
		"Framing": dbus.MakeVariant(byte(0)), "PHY": dbus.MakeVariant(byte(2)),
		"Retransmissions": dbus.MakeVariant(byte(2)), "Latency": dbus.MakeVariant(uint16(10)),
		"PresentationDelay": dbus.MakeVariant(uint32(40000)), "TargetLatency": dbus.MakeVariant(byte(2)),
	}
	if err := validateQoS(valid, codec); err != nil {
		t.Fatal(err)
	}
	valid["SDU"] = dbus.MakeVariant(uint16(79))
	if err := validateQoS(valid, codec); err == nil {
		t.Fatal("accepted mismatched SDU")
	}
}

func TestSelectPropertiesHonorsSevenPointFiveMillisecondBounds(t *testing.T) {
	e := &endpoint{direction: DirectionSink}
	capabilities := []byte{
		0x03, ltvFrequency, 0x04, 0x00,
		0x02, ltvDuration, 0x01,
		0x02, ltvChannelAllocation, 0x01,
		0x05, ltvFrameLength, 30, 0, 30, 0,
	}
	selected, dbusErr := e.SelectProperties(map[string]dbus.Variant{
		"Capabilities":      dbus.MakeVariant(capabilities),
		"ChannelAllocation": dbus.MakeVariant(uint32(0x20)),
		"QoS": dbus.MakeVariant(map[string]dbus.Variant{
			"Framing": dbus.MakeVariant(byte(0)), "PHY": dbus.MakeVariant(byte(2)),
			"MaximumLatency": dbus.MakeVariant(uint16(8)),
			"MinimumDelay":   dbus.MakeVariant(uint32(10000)), "MaximumDelay": dbus.MakeVariant(uint32(30000)),
		}),
	})
	if dbusErr != nil {
		t.Fatal(dbusErr)
	}
	configuration, _ := variantBytes(selected["Capabilities"])
	codec, err := ParseConfiguration(configuration)
	if err != nil || codec.FrameDuration != 7500*time.Microsecond || codec.ChannelAllocation != 0x20 {
		t.Fatalf("codec=%#v err=%v", codec, err)
	}
	qos := selected["QoS"].Value().(map[string]dbus.Variant)
	interval, _ := variantUint32(qos["Interval"])
	latency, _ := variantUint16(qos["Latency"])
	delay, _ := variantUint32(qos["PresentationDelay"])
	if interval != 7500 || latency != 8 || delay != 30000 {
		t.Fatalf("selected QoS interval=%d latency=%d delay=%d", interval, latency, delay)
	}
}

func TestFinalQoSRequiresAllocatedBidirectionalCIS(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	qos := qosVariants(TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 80,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	if _, err := parseConfiguredQoS(qos, codec, true); err == nil {
		t.Fatal("accepted unallocated final CIG/CIS")
	}
	qos["CIG"] = dbus.MakeVariant(byte(3))
	qos["CIS"] = dbus.MakeVariant(byte(4))
	if _, err := parseConfiguredQoS(qos, codec, true); err != nil {
		t.Fatal(err)
	}
}

func TestFinalQoSUsesSelectedTargetLatencyWhenBlueZOmittedIt(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	selected := TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 80,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	}
	final := qosVariants(selected)
	final["CIG"] = dbus.MakeVariant(byte(3))
	final["CIS"] = dbus.MakeVariant(byte(4))
	delete(final, "TargetLatency")

	got, err := parseFinalTransportQoS(final, codec, selected)
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetLatency != selected.TargetLatency || got.CIG != 3 || got.CIS != 4 {
		t.Fatalf("final QoS=%#v", got)
	}
	if _, restored := final["TargetLatency"]; restored {
		t.Fatal("final BlueZ QoS map was mutated")
	}
}

func TestFinalQoSRejectsMissingTargetLatencyWithoutValidSelection(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	final := qosVariants(TransportQoS{
		CIG: 3, CIS: 4, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 80,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	delete(final, "TargetLatency")

	for _, invalid := range []byte{0, 4} {
		_, err := parseFinalTransportQoS(final, codec, TransportQoS{TargetLatency: invalid})
		if err == nil || transportDetailReason(err) != "qos_target_latency_fallback_invalid" {
			t.Fatalf("selected target latency=%d err=%v reason=%s", invalid, err, transportDetailReason(err))
		}
	}
}

func TestFinalQoSValidatesPresentTargetLatency(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	final := qosVariants(TransportQoS{
		CIG: 3, CIS: 4, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 80,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 4,
	})
	selected := TransportQoS{TargetLatency: 2}
	if _, err := parseFinalTransportQoS(final, codec, selected); err == nil || transportDetailReason(err) != "qos_target_latency_invalid" {
		t.Fatalf("invalid final target latency err=%v reason=%s", err, transportDetailReason(err))
	}
	final["TargetLatency"] = dbus.MakeVariant(byte(1))
	got, err := parseFinalTransportQoS(final, codec, selected)
	if err != nil || got.TargetLatency != 1 {
		t.Fatalf("present final target latency=%d err=%v", got.TargetLatency, err)
	}
}

func TestFinalQoSFallbackKeepsStrictAllocatedIdentifierChecks(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	final := qosVariants(TransportQoS{
		CIG: 0xff, CIS: 0xff, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 80,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	})
	delete(final, "TargetLatency")
	_, err := parseFinalTransportQoS(final, codec, TransportQoS{TargetLatency: 2})
	if err == nil || transportDetailReason(err) != "qos_ids_unallocated" {
		t.Fatalf("unallocated identifiers err=%v reason=%s", err, transportDetailReason(err))
	}
}

func TestFinalQoSFallbackDoesNotRelaxPublishedFields(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	selected := TransportQoS{TargetLatency: 2}
	valid := TransportQoS{
		CIG: 3, CIS: 4, IntervalUS: 10000, Framing: 0, PHY: 2, SDU: 80,
		Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2,
	}

	tests := []struct {
		name   string
		mutate func(map[string]dbus.Variant)
		reason string
	}{
		{
			name: "missing interval",
			mutate: func(qos map[string]dbus.Variant) {
				delete(qos, "Interval")
			},
			reason: "qos_interval_mismatch",
		},
		{
			name: "non 2M PHY",
			mutate: func(qos map[string]dbus.Variant) {
				qos["PHY"] = dbus.MakeVariant(byte(1))
			},
			reason: "qos_phy_not_le_2m",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			final := qosVariants(valid)
			delete(final, "TargetLatency")
			test.mutate(final)
			_, err := parseFinalTransportQoS(final, codec, selected)
			if err == nil || transportDetailReason(err) != test.reason {
				t.Fatalf("err=%v reason=%s", err, transportDetailReason(err))
			}
		})
	}
}

func TestTransportDetailReasonDoesNotExposeWrappedDBusText(t *testing.T) {
	err := newTransportDetailError(
		"transport_properties_read",
		errors.New("private peer object path and address"),
	)
	reason := transportDetailReason(err)
	if reason != "transport_properties_read" || strings.Contains(reason, "private") {
		t.Fatalf("reason=%q", reason)
	}
}

func TestTransportPairRequiresOneCIGAndCIS(t *testing.T) {
	codec := CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond, OctetsPerFrame: 80}
	qos := TransportQoS{CIG: 1, CIS: 2, IntervalUS: 10000, Framing: 0, PHY: 2}
	sink := &pendingTransport{path: "/sink", config: transportConfig{direction: DirectionSink, codec: codec}, qos: qos, links: []dbus.ObjectPath{"/source"}}
	source := &pendingTransport{path: "/source", config: transportConfig{direction: DirectionSource, codec: codec}, qos: qos, links: []dbus.ObjectPath{"/sink"}}
	if err := validateTransportPair(sink, source); err != nil {
		t.Fatal(err)
	}
	source.qos.CIS = 3
	if err := validateTransportPair(sink, source); err == nil {
		t.Fatal("accepted two unidirectional CIS identifiers")
	}
}

func TestWrapHandoffFailure(t *testing.T) {
	if !errors.Is(wrapHandoffFailure(nil), errHandoffStopped) {
		t.Fatal("nil handoff exit was not classified as terminal")
	}
	cause := errors.New("socket failed")
	wrapped := wrapHandoffFailure(cause)
	if !errors.Is(wrapped, errHandoffStopped) {
		t.Fatal("handoff error was not classified as terminal")
	}
	if errors.Is(wrapped, cause) {
		t.Fatal("internal cause unexpectedly became the control-flow sentinel")
	}
}

func TestWaitHandoffExitPreservesShutdownFailure(t *testing.T) {
	clean := make(chan error, 1)
	clean <- nil
	if err := waitHandoffExit(clean); err != nil {
		t.Fatal(err)
	}
	failed := make(chan error, 1)
	failed <- errors.New("close failed")
	if err := waitHandoffExit(failed); err == nil {
		t.Fatal("handoff shutdown failure was discarded")
	}
}

func TestDescriptorGenerationIsStrictlyMonotonic(t *testing.T) {
	b := &Broker{}
	first, err := b.nextDescriptorGenerationLocked()
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.nextDescriptorGenerationLocked()
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("descriptor generations did not advance: %d then %d", first, second)
	}
}

func TestConfigureCallControlRequiresProviderAndPreRunState(t *testing.T) {
	b := &Broker{}
	if err := b.ConfigureCallControl(nil, true); err == nil {
		t.Fatal("required call correlation accepted a nil provider")
	}
	if err := b.ConfigureCallControl(func() (uint64, bool) { return 7, true }, true); err != nil {
		t.Fatal(err)
	}
	b.connection = &dbus.Conn{}
	if err := b.ConfigureCallControl(func() (uint64, bool) { return 8, true }, true); err == nil {
		t.Fatal("call control changed after broker start")
	}
}

func TestDescriptorForPendingCarriesCallCorrelation(t *testing.T) {
	pending := &pendingTransport{
		path: "/org/bluez/hci0/dev_00_11_22_33_44_55/fd0",
		config: transportConfig{
			direction: DirectionSink,
			codec: CodecConfig{SampleRate: 32000, FrameDuration: 10 * time.Millisecond,
				OctetsPerFrame: 80, ChannelAllocation: FrontCenterLocation},
		},
		qos: TransportQoS{CIG: 1, CIS: 2, IntervalUS: 10000, Framing: 0, PHY: 2,
			Retransmissions: 2, LatencyMS: 10, PresentationDelayUS: 40000, TargetLatency: 2},
		readMTU: 80,
	}
	descriptor := descriptorForPending(pending, 10, 9, 1234, true)
	if !descriptor.CallControlCorrelated || descriptor.CallControlToken != 1234 {
		t.Fatalf("call correlation missing: %#v", descriptor)
	}
	if err := validateDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForCallTokenCoversBoundedStateMediaReordering(t *testing.T) {
	var token atomic.Uint64
	done := make(chan struct{})
	go func() {
		time.Sleep(2 * callCorrelationPoll)
		token.Store(1234)
		close(done)
	}()
	got, correlated := waitForCallToken(func() (uint64, bool) {
		value := token.Load()
		return value, value != 0
	}, 10*callCorrelationPoll)
	<-done
	if !correlated || got != 1234 {
		t.Fatalf("token=%d correlated=%v", got, correlated)
	}
	started := time.Now()
	if got, correlated = waitForCallToken(nil, 2*callCorrelationPoll); correlated || got != 0 {
		t.Fatalf("nil provider token=%d correlated=%v", got, correlated)
	}
	if time.Since(started) < 2*callCorrelationPoll {
		t.Fatal("missing token returned before the bounded wait elapsed")
	}
}

func TestPairReservationCannotCrossReconfiguration(t *testing.T) {
	connection := &dbus.Conn{}
	sink := &pendingTransport{path: "/sink",
		config: transportConfig{direction: DirectionSink, generation: 1}}
	source := &pendingTransport{path: "/source",
		config: transportConfig{direction: DirectionSource, generation: 1}}
	broker := &Broker{connection: connection,
		configured: map[dbus.ObjectPath]transportConfig{
			sink.path: sink.config, source.path: source.config,
		}, pairing: map[dbus.ObjectPath]bool{sink.path: true, source.path: true}}
	if !broker.pairCurrentLocked(connection, sink, source) {
		t.Fatal("current pair reservation was rejected")
	}
	broker.configured[sink.path] = transportConfig{direction: DirectionSink, generation: 2}
	broker.clearPairingLocked(sink, source)
	if !broker.pairing[sink.path] || broker.pairing[source.path] {
		t.Fatalf("generation-scoped cleanup crossed reconfiguration: %#v", broker.pairing)
	}
	if broker.pairCurrentLocked(connection, sink, source) {
		t.Fatal("stale pair survived transport reconfiguration")
	}
}
