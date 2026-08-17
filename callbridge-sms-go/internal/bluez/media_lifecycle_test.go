package bluez

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
	"golang.org/x/sys/unix"
)

func installLifecycleSession(b *Broker, token uint64) *activeTransportSession {
	session := &activeTransportSession{
		connection:       &dbus.Conn{},
		releasePaths:     []dbus.ObjectPath{"/leader"},
		sinkPath:         "/sink",
		sourcePath:       "/source",
		sinkGeneration:   token*2 - 1,
		sourceGeneration: token * 2,
		bundleID:         token,
		callToken:        token,
		lifecycleFD:      -1,
		lifecycleDone:    make(chan struct{}),
		startedAt:        time.Now(),
		idlePaths:        make(map[dbus.ObjectPath]bool),
	}
	b.mu.Lock()
	b.activeSession = session
	b.acquired[session.sinkPath] = true
	b.acquired[session.sourcePath] = true
	b.mu.Unlock()
	return session
}

func waitLifecycleSession(t *testing.T, b *Broker, want *activeTransportSession) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		b.mu.Lock()
		got := b.activeSession
		b.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("active session=%p, want %p", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func newLifecycleBroker(logger *log.Logger) *Broker {
	return &Broker{
		logger:            logger,
		configured:        make(map[dbus.ObjectPath]transportConfig),
		acquiring:         make(map[dbus.ObjectPath]bool),
		acquired:          make(map[dbus.ObjectPath]bool),
		pairing:           make(map[dbus.ObjectPath]bool),
		pending:           make(map[dbus.ObjectPath]*pendingTransport),
		releaseRetryDelay: 0,
		normalReleaseWait: time.Millisecond,
	}
}

func installLifecycleSocketSession(t *testing.T, b *Broker, token uint64) (*activeTransportSession, int) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX,
		unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	session := installLifecycleSession(b, token)
	session.lifecycleFD = pair[0]
	return session, pair[1]
}

func lifecyclePacket(bundle, sinkGeneration, sourceGeneration uint64) []byte {
	return lifecycleTypedPacket(lifecycleTypeProgress, bundle, sinkGeneration,
		sourceGeneration)
}

func lifecycleTypedPacket(typeID byte, bundle, sinkGeneration,
	sourceGeneration uint64) []byte {
	packet := make([]byte, lifecycleMessageSize)
	copy(packet[:4], lifecycleMagic[:])
	packet[4] = lifecycleVersion
	packet[5] = typeID
	binary.LittleEndian.PutUint64(packet[8:16], bundle)
	binary.LittleEndian.PutUint64(packet[16:24], sinkGeneration)
	binary.LittleEndian.PutUint64(packet[24:32], sourceGeneration)
	return packet
}

func transportIdleSignal(path dbus.ObjectPath) *dbus.Signal {
	return &dbus.Signal{
		Path: path,
		Name: propertiesInterface + ".PropertiesChanged",
		Body: []interface{}{
			transportInterface,
			map[string]dbus.Variant{"State": dbus.MakeVariant("idle")},
			[]string{},
		},
	}
}

func TestAbnormalTerminationSignalsReleaseOneLinkedOwnerExactlyOnce(t *testing.T) {
	for _, reason := range []string{
		"call_token_ended",
		"le_bearer_disconnected",
		"asterisk_handoff_disconnected",
	} {
		t.Run(reason, func(t *testing.T) {
			var calls atomic.Uint64
			b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
			b.releaseInvoker = func(_ *dbus.Conn, path dbus.ObjectPath) error {
				if path != "/leader" {
					t.Fatalf("release path=%q", path)
				}
				calls.Add(1)
				return nil
			}
			session := installLifecycleSession(b, 10)
			if !b.requestSpecificSessionRelease(session, reason) {
				t.Fatal("first termination signal was not accepted")
			}
			if b.requestSpecificSessionRelease(session, "duplicate_termination") {
				t.Fatal("duplicate termination started a second release")
			}
			waitLifecycleSession(t, b, nil)
			if calls.Load() != 1 {
				t.Fatalf("release calls=%d, want 1", calls.Load())
			}
			snapshot := b.Snapshot()
			if snapshot.TransportReleaseAttempts != 1 ||
				snapshot.TransportReleaseSuccesses != 1 ||
				snapshot.TransportReleaseErrors != 0 ||
				snapshot.TransportReleaseFallbacks != 0 {
				t.Fatalf("release counters=%+v", snapshot)
			}
		})
	}
}

func TestNormalACKHUPAndLeaseRaceReleaseOncePerGeneration(t *testing.T) {
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	for token := uint64(70); token < 72; token++ {
		session := installLifecycleSession(b, token)
		start := make(chan struct{})
		var accepted atomic.Uint64
		var workers sync.WaitGroup
		for _, trigger := range []func() bool{
			func() bool { return b.ReleaseCallToken(token) },
			func() bool { return b.requestSpecificSessionRelease(session, "asterisk_lifecycle_closed") },
			func() bool { return b.requestSpecificSessionRelease(session, "media_progress_lease_expired") },
		} {
			workers.Add(1)
			go func(trigger func() bool) {
				defer workers.Done()
				<-start
				if trigger() {
					accepted.Add(1)
				}
			}(trigger)
		}
		close(start)
		workers.Wait()
		waitLifecycleSession(t, b, nil)
		if accepted.Load() < 1 || accepted.Load() > 2 {
			t.Fatalf("token %d accepted triggers=%d, want 1 or 2", token, accepted.Load())
		}
	}
	if releases.Load() != 2 {
		t.Fatalf("release calls=%d, want one for each generation", releases.Load())
	}
}

func TestLifecycleHUPReleasesWhenACKIsLost(t *testing.T) {
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.progressStartLease = time.Hour
	b.progressLease = time.Hour
	b.callToken = func() (uint64, bool) { return 80, true }
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	session, asteriskLease := installLifecycleSocketSession(t, b, 80)
	go b.watchActiveSession(session)
	if err := unix.Close(asteriskLease); err != nil {
		t.Fatal(err)
	}
	waitLifecycleSession(t, b, nil)
	if releases.Load() != 1 || b.Snapshot().LifecycleHUPs != 1 {
		t.Fatalf("release=%d snapshot=%+v", releases.Load(), b.Snapshot())
	}
}

func TestNormalEndIsDrainedBeforeImmediateHUP(t *testing.T) {
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.normalReleaseWait = time.Hour
	b.callToken = func() (uint64, bool) { return 83, true }
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	session, asteriskLease := installLifecycleSocketSession(t, b, 83)
	go b.watchActiveSession(session)
	packet := lifecycleTypedPacket(lifecycleTypeNormalEnd, session.bundleID,
		session.sinkGeneration, session.sourceGeneration)
	if _, err := unix.Write(asteriskLease, packet); err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(asteriskLease); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := b.Snapshot()
		if snapshot.LifecycleNormalEndEvents == 1 && snapshot.LifecycleHUPs == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("normal-end plus HUP was not drained: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	b.mu.Lock()
	active := b.activeSession
	b.mu.Unlock()
	if active != session || releases.Load() != 0 {
		t.Fatalf("normal HUP released before remote IDLE: active=%p release=%d",
			active, releases.Load())
	}
	if b.Snapshot().NormalReleaseWaits != 0 {
		t.Fatal("normal-end receipt started the watchdog before media stopped")
	}
	b.handleSignal(transportIdleSignal(session.sinkPath))
	b.mu.Lock()
	active = b.activeSession
	b.mu.Unlock()
	if active != session {
		t.Fatal("one linked ASE becoming idle completed the pair too early")
	}
	b.handleSignal(transportIdleSignal(session.sourcePath))
	waitLifecycleSession(t, b, nil)
	if releases.Load() != 0 || b.Snapshot().NormalReleaseRemoteIdle != 1 {
		t.Fatalf("remote IDLE outcome release=%d snapshot=%+v", releases.Load(), b.Snapshot())
	}
}

func TestNormalReleaseBoundStartsOnlyAfterMediaProgressStops(t *testing.T) {
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.normalReleaseWait = time.Hour
	b.progressStartLease = time.Hour
	b.progressLease = 20 * time.Millisecond
	b.callToken = func() (uint64, bool) { return 835, true }
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	session := installLifecycleSession(b, 835)
	session.progressArmed = true
	session.lastProgress = time.Now()
	if !b.markNormalReleasePending(session, "fixture_normal_end", true) {
		t.Fatal("normal release was not marked pending")
	}
	go b.watchActiveSession(session)
	time.Sleep(5 * time.Millisecond)
	if b.Snapshot().NormalReleaseWaits != 0 {
		t.Fatal("bounded wait started before the injected progress lease elapsed")
	}
	deadline := time.Now().Add(time.Second)
	for b.Snapshot().NormalReleaseWaits != 1 {
		if time.Now().After(deadline) {
			t.Fatal("bounded wait did not start after media progress stopped")
		}
		time.Sleep(time.Millisecond)
	}
	b.handleSignal(transportIdleSignal(session.sinkPath))
	b.handleSignal(transportIdleSignal(session.sourcePath))
	waitLifecycleSession(t, b, nil)
	if releases.Load() != 0 || b.Snapshot().NormalReleaseRemoteIdle != 1 {
		t.Fatalf("remote idle outcome release=%d snapshot=%+v", releases.Load(), b.Snapshot())
	}
}

func TestNormalReleaseTimeoutIsFiniteAndForced(t *testing.T) {
	var releases atomic.Uint64
	var logs bytes.Buffer
	b := newLifecycleBroker(log.New(&logs, "", 0))
	b.normalReleaseWait = 15 * time.Millisecond
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	session := installLifecycleSession(b, 84)
	if !b.beginNormalReleaseWait(session, "fixture_normal_end", true) {
		t.Fatal("normal release wait was not started")
	}
	waitLifecycleSession(t, b, nil)
	snapshot := b.Snapshot()
	if releases.Load() != 1 || snapshot.NormalReleaseTimeouts != 1 ||
		snapshot.TransportReleaseSuccesses != 1 ||
		!bytes.Contains(logs.Bytes(), []byte("severity=high")) {
		t.Fatalf("forced timeout release=%d snapshot=%+v logs=%s",
			releases.Load(), snapshot, logs.String())
	}
}

func TestFiveTerminationTriggersRaceOncePerGeneration(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		var releases atomic.Uint64
		b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
		b.normalReleaseWait = time.Hour
		b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
			releases.Add(1)
			return nil
		}
		session := installLifecycleSession(b, uint64(1000+iteration))
		start := make(chan struct{})
		var workers sync.WaitGroup
		triggers := []func(){
			func() { b.requestSpecificSessionRelease(session, "asterisk_lifecycle_closed") },
			func() { b.requestSpecificSessionRelease(session, "media_progress_lease_expired") },
			func() { b.beginNormalReleaseWait(session, "asterisk_normal_end", true) },
			func() {
				b.handleSignal(transportIdleSignal(session.sinkPath))
				b.handleSignal(transportIdleSignal(session.sourcePath))
			},
			func() { b.forceNormalReleaseTimeout(session, time.Millisecond) },
		}
		for _, trigger := range triggers {
			workers.Add(1)
			go func(trigger func()) {
				defer workers.Done()
				<-start
				trigger()
			}(trigger)
		}
		close(start)
		workers.Wait()
		waitLifecycleSession(t, b, nil)
		snapshot := b.Snapshot()
		if releases.Load() > 1 ||
			releases.Load()+snapshot.TransportIdleCompletions != 1 {
			t.Fatalf("iteration %d terminal outcomes release=%d idle=%d snapshot=%+v",
				iteration, releases.Load(), snapshot.TransportIdleCompletions, snapshot)
		}
	}
}

func TestNormalReleaseWaitHasNoUnmeasuredProductionDefault(t *testing.T) {
	b, err := NewBroker("hci0", "00:11:22:33:44:55",
		t.TempDir()+"/handoff.sock", 0, log.New(&bytes.Buffer{}, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if b.normalReleaseWait != 0 {
		t.Fatalf("unmeasured production default=%s", b.normalReleaseWait)
	}
	if err := b.ConfigureNormalReleaseWait(remoteASEReleasingWatchdog); err == nil {
		t.Fatal("remote watchdog boundary was accepted as a wait")
	}
	if err := b.ConfigureNormalReleaseWait(25 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestStartupLeaseBoundsCallWithNoMediaProgress(t *testing.T) {
	var releases atomic.Uint64
	var logs bytes.Buffer
	b := newLifecycleBroker(log.New(&logs, "", 0))
	b.progressStartLease = 15 * time.Millisecond
	b.progressLease = time.Hour
	b.callToken = func() (uint64, bool) { return 81, true }
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	session := installLifecycleSession(b, 81)
	go b.watchActiveSession(session)
	waitLifecycleSession(t, b, nil)
	snapshot := b.Snapshot()
	if releases.Load() != 1 || snapshot.LifecycleStartExpirations != 1 ||
		!bytes.Contains(logs.Bytes(), []byte("phase=startup")) {
		t.Fatalf("release=%d snapshot=%+v logs=%s", releases.Load(), snapshot, logs.String())
	}
}

func TestOnlyMatchingMediaProgressRenewsArmedLease(t *testing.T) {
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.progressStartLease = time.Second
	b.progressLease = 80 * time.Millisecond
	b.callToken = func() (uint64, bool) { return 82, true }
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	session, asteriskLease := installLifecycleSocketSession(t, b, 82)
	defer unix.Close(asteriskLease)
	go b.watchActiveSession(session)
	matching := lifecyclePacket(session.bundleID, session.sinkGeneration, session.sourceGeneration)
	if _, err := unix.Write(asteriskLease, matching); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for b.Snapshot().LifecycleProgressEvents != 1 {
		if time.Now().After(deadline) {
			t.Fatal("matching media progress was not observed")
		}
		time.Sleep(time.Millisecond)
	}
	stale := lifecyclePacket(session.bundleID-1, session.sinkGeneration-1,
		session.sourceGeneration-1)
	if _, err := unix.Write(asteriskLease, stale); err != nil {
		t.Fatal(err)
	}
	waitLifecycleSession(t, b, nil)
	snapshot := b.Snapshot()
	if releases.Load() != 1 || snapshot.LifecycleProgressEvents != 1 ||
		snapshot.LifecycleStaleEvents != 1 || snapshot.LifecycleLeaseExpirations != 1 {
		t.Fatalf("release=%d snapshot=%+v", releases.Load(), snapshot)
	}
}

func TestActiveSessionUsesPublishedDescriptorGenerations(t *testing.T) {
	sink := &pendingTransport{
		path:   "/sink",
		config: transportConfig{direction: DirectionSink, generation: 11},
	}
	source := &pendingTransport{
		path:   "/source",
		config: transportConfig{direction: DirectionSource, generation: 12},
	}
	sinkDescriptor := Descriptor{
		Direction: DirectionSink, Generation: 101, BundleID: 101,
		CallControlToken: 91,
	}
	sourceDescriptor := Descriptor{
		Direction: DirectionSource, Generation: 102, BundleID: 101,
		CallControlToken: 91,
	}
	session := activeSessionForHandoff(&dbus.Conn{}, sink, source,
		sinkDescriptor, sourceDescriptor, -1, time.Unix(1, 0))
	progress := lifecycleProgress{
		BundleID:         sinkDescriptor.BundleID,
		SinkGeneration:   sinkDescriptor.Generation,
		SourceGeneration: sourceDescriptor.Generation,
	}
	if !lifecycleProgressMatches(session, progress) {
		t.Fatalf("published lifecycle identity does not match session: %+v", session)
	}
	if lifecycleProgressMatches(session, lifecycleProgress{
		BundleID:         sinkDescriptor.BundleID,
		SinkGeneration:   sink.config.generation,
		SourceGeneration: source.config.generation,
	}) {
		t.Fatal("internal configuration generations were exposed as lifecycle identity")
	}
}

func TestLateOldLifecycleCannotReleaseNewGeneration(t *testing.T) {
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.progressStartLease = time.Hour
	b.progressLease = time.Hour
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	first, firstPeer := installLifecycleSocketSession(t, b, 90)
	first.callToken = 0
	go b.watchActiveSession(first)
	second, secondPeer := installLifecycleSocketSession(t, b, 91)
	second.callToken = 0
	defer unix.Close(secondPeer)
	go b.watchActiveSession(second)
	oldProgress := lifecyclePacket(first.bundleID, first.sinkGeneration,
		first.sourceGeneration)
	if _, err := unix.Write(firstPeer, oldProgress); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for b.Snapshot().LifecycleStaleEvents != 1 {
		if time.Now().After(deadline) {
			t.Fatal("late first-session progress was not classified stale")
		}
		time.Sleep(time.Millisecond)
	}
	if err := unix.Close(firstPeer); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	b.mu.Lock()
	active := b.activeSession
	b.mu.Unlock()
	if active != second || releases.Load() != 0 {
		t.Fatalf("late first HUP changed second session: active=%p release=%d", active, releases.Load())
	}
	if !b.requestSpecificSessionRelease(second, "second_call_complete") {
		t.Fatal("second generation did not retain its own release state")
	}
	waitLifecycleSession(t, b, nil)
	if releases.Load() != 1 {
		t.Fatalf("release calls=%d", releases.Load())
	}
}

func TestLEBearerAndAsteriskPeerTerminationUseLifecycleRelease(t *testing.T) {
	for _, test := range []struct {
		name    string
		trigger func(*Broker) bool
	}{
		{name: "le_bearer_drop", trigger: func(b *Broker) bool {
			return b.handleLEBearerDisconnected()
		}},
		{name: "asterisk_handoff_disconnect", trigger: func(b *Broker) bool {
			if b.handoff == nil || b.handoff.disconnectHandler == nil {
				return false
			}
			b.handoff.disconnectHandler()
			return true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Uint64
			b, err := NewBroker("hci0", "00:11:22:33:44:55",
				t.TempDir()+"/handoff.sock", 0, log.New(&bytes.Buffer{}, "", 0))
			if err != nil {
				t.Fatal(err)
			}
			b.releaseRetryDelay = 0
			b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
				calls.Add(1)
				return nil
			}
			installLifecycleSession(b, 40)
			if !test.trigger(b) {
				t.Fatal("termination trigger was not wired")
			}
			waitLifecycleSession(t, b, nil)
			if calls.Load() != 1 {
				t.Fatalf("release calls=%d", calls.Load())
			}
		})
	}
}

func TestTwoSequentialCallTokensReleaseAndReacquire(t *testing.T) {
	var current atomic.Uint64
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.progressStartLease = time.Millisecond
	b.progressLease = time.Hour
	b.callToken = func() (uint64, bool) {
		value := current.Load()
		return value, value != 0
	}
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}

	current.Store(11)
	first := installLifecycleSession(b, 11)
	go b.watchActiveSession(first)
	current.Store(0)
	waitLifecycleSession(t, b, nil)

	current.Store(12)
	second := installLifecycleSession(b, 12)
	go b.watchActiveSession(second)
	current.Store(0)
	waitLifecycleSession(t, b, nil)
	if releases.Load() != 2 {
		t.Fatalf("release calls=%d, want two independent calls", releases.Load())
	}
	if snapshot := b.Snapshot(); snapshot.BidirectionalCIS ||
		snapshot.TransportReleaseSuccesses != 2 {
		t.Fatalf("final snapshot=%+v", snapshot)
	}
}

func TestRTPTimeoutOrAsteriskChannelEndCannotLeaveOwner(t *testing.T) {
	for _, scenario := range []string{"rtp_timeout", "asterisk_channel_ended"} {
		t.Run(scenario, func(t *testing.T) {
			var releases atomic.Uint64
			b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
			b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
				releases.Add(1)
				return nil
			}
			if scenario == "rtp_timeout" {
				session := installLifecycleSession(b, 50)
				if !b.requestSpecificSessionRelease(session, "media_progress_lease_expired") {
					t.Fatal("RTP timeout did not start abnormal release")
				}
			} else {
				session, lifecyclePeer := installLifecycleSocketSession(t, b, 50)
				b.progressStartLease = time.Hour
				b.progressLease = time.Hour
				go b.watchActiveSession(session)
				if err := unix.Close(lifecyclePeer); err != nil {
					t.Fatal(err)
				}
			}
			waitLifecycleSession(t, b, nil)
			if releases.Load() != 1 {
				t.Fatalf("release calls=%d", releases.Load())
			}
		})
	}
}

func TestAsteriskTerminateTokenWaitsForRemoteIdleOnlyOnMatchingCall(t *testing.T) {
	var releases atomic.Uint64
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releases.Add(1)
		return nil
	}
	session := installLifecycleSession(b, 60)
	if b.ReleaseCallToken(0) || b.ReleaseCallToken(59) {
		t.Fatal("unrelated Asterisk channel released active media")
	}
	if !b.ReleaseCallToken(60) {
		t.Fatal("matching Asterisk terminate did not release media")
	}
	if b.ReleaseCallToken(60) {
		t.Fatal("duplicate terminate started a second normal teardown")
	}
	b.mu.Lock()
	active := b.activeSession
	b.mu.Unlock()
	if active != session || releases.Load() != 0 {
		t.Fatal("matching Asterisk terminate released before remote idle")
	}
	b.handleSignal(transportIdleSignal(session.sinkPath))
	b.handleSignal(transportIdleSignal(session.sourcePath))
	waitLifecycleSession(t, b, nil)
	if releases.Load() != 0 || session.callToken != 60 ||
		b.Snapshot().NormalReleaseRemoteIdle != 1 {
		t.Fatalf("release calls=%d session=%+v", releases.Load(), session)
	}
}

func TestReleaseErrorsRetryThenDropDBusOwner(t *testing.T) {
	var releaseCalls atomic.Uint64
	var fallbackCalls atomic.Uint64
	var logs bytes.Buffer
	b := newLifecycleBroker(log.New(&logs, "", 0))
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		releaseCalls.Add(1)
		return dbus.Error{Name: "org.bluez.Error.Failed", Body: []interface{}{"bounded failure"}}
	}
	b.releaseFallback = func(_ *dbus.Conn) error {
		fallbackCalls.Add(1)
		return nil
	}
	session := installLifecycleSession(b, 20)
	if !b.requestSpecificSessionRelease(session, "rtp_timeout") {
		t.Fatal("release was not started")
	}
	waitLifecycleSession(t, b, nil)
	if releaseCalls.Load() != transportReleaseRetries || fallbackCalls.Load() != 1 {
		t.Fatalf("release calls=%d fallback=%d", releaseCalls.Load(), fallbackCalls.Load())
	}
	snapshot := b.Snapshot()
	if snapshot.TransportReleaseAttempts != transportReleaseRetries ||
		snapshot.TransportReleaseErrors != transportReleaseRetries ||
		snapshot.TransportReleaseSuccesses != 0 ||
		snapshot.TransportReleaseFallbacks != 1 {
		t.Fatalf("release counters=%+v", snapshot)
	}
	if !bytes.Contains(logs.Bytes(), []byte("error_name=org.bluez.Error.Failed")) ||
		!bytes.Contains(logs.Bytes(), []byte("outcome=connection_closed")) {
		t.Fatalf("release failure was not observable: %s", logs.String())
	}
}

func TestReleaseAndOwnerFallbackFailureRemainFailClosed(t *testing.T) {
	b := newLifecycleBroker(log.New(&bytes.Buffer{}, "", 0))
	b.releaseInvoker = func(_ *dbus.Conn, _ dbus.ObjectPath) error {
		return errors.New("release unavailable")
	}
	b.releaseFallback = func(_ *dbus.Conn) error {
		return errors.New("owner disconnect unavailable")
	}
	session := installLifecycleSession(b, 30)
	if !b.requestSpecificSessionRelease(session, "asterisk_channel_ended") {
		t.Fatal("release was not started")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		b.mu.Lock()
		exhausted := session.releaseExhausted
		active := b.activeSession
		b.mu.Unlock()
		if exhausted {
			if active != session {
				t.Fatal("failed owner cleanup silently cleared the session")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("release failure did not reach an explicit fail-closed state")
		}
		time.Sleep(time.Millisecond)
	}
	if b.requestSpecificSessionRelease(session, "duplicate_after_exhaustion") {
		t.Fatal("exhausted session started an unbounded retry loop")
	}
}
