package bluez

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func startTestHandoffServer(t *testing.T, peerUID int) (*HandoffServer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "leaudio.sock")
	server, err := NewHandoffServer(path, peerUID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("handoff server exit: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("handoff server did not stop")
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("handoff socket remained after shutdown: %v", err)
		}
	})
	return server, path
}

func connectTestHandoffPeer(t *testing.T, path string) int {
	t.Helper()
	client, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		err = unix.Connect(client, &unix.SockaddrUnix{Name: path})
		if err == nil {
			return client
		}
		if time.Now().After(deadline) {
			unix.Close(client)
			t.Fatalf("connect handoff server: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitTestHandoffClient(t *testing.T, server *HandoffServer, connected bool) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		server.mu.Lock()
		client := server.clientFD
		server.mu.Unlock()
		if (client != -1) == connected {
			return client
		}
		if time.Now().After(deadline) {
			t.Fatalf("handoff client connected=%t, want %t", client != -1, connected)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandoffServerRefusesExistingSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leaudio.sock")
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		t.Fatal(err)
	}
	server, err := NewHandoffServer(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("replaced an existing handoff socket")
	}
}

func TestHandoffServerRejectsUnexpectedUID(t *testing.T) {
	wrongUID := os.Getuid() + 1
	_, path := startTestHandoffServer(t, wrongUID)
	client := connectTestHandoffPeer(t, path)
	defer unix.Close(client)
	poll := []unix.PollFd{{Fd: int32(client), Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR}}
	count, err := unix.Poll(poll, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 || poll[0].Revents&unix.POLLHUP == 0 {
		t.Fatalf("unexpected UID connection was not closed: revents=%#x", poll[0].Revents)
	}
}

func TestHandoffServerRejectsSecondLivePeer(t *testing.T) {
	server, path := startTestHandoffServer(t, os.Getuid())
	first := connectTestHandoffPeer(t, path)
	defer unix.Close(first)
	accepted := waitTestHandoffClient(t, server, true)

	second := connectTestHandoffPeer(t, path)
	defer unix.Close(second)
	poll := []unix.PollFd{{Fd: int32(second), Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR}}
	count, err := unix.Poll(poll, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 || poll[0].Revents&unix.POLLHUP == 0 {
		t.Fatalf("second live peer was not rejected: revents=%#x", poll[0].Revents)
	}
	if got := waitTestHandoffClient(t, server, true); got != accepted {
		t.Fatalf("live handoff peer changed: got fd %d, want %d", got, accepted)
	}
}

func TestHandoffServerReleasesDisconnectedPeer(t *testing.T) {
	server, path := startTestHandoffServer(t, os.Getuid())
	first := connectTestHandoffPeer(t, path)
	waitTestHandoffClient(t, server, true)
	if err := unix.Shutdown(first, unix.SHUT_RDWR); err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(first); err != nil {
		t.Fatal(err)
	}
	waitTestHandoffClient(t, server, false)

	second := connectTestHandoffPeer(t, path)
	defer unix.Close(second)
	waitTestHandoffClient(t, server, true)

	sinkPipe := []int{0, 0}
	if err := unix.Pipe2(sinkPipe, unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sinkPipe[0])
	defer unix.Close(sinkPipe[1])
	sourcePipe := []int{0, 0}
	if err := unix.Pipe2(sourcePipe, unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sourcePipe[0])
	defer unix.Close(sourcePipe[1])

	sink := testDescriptor(DirectionSink, 1, 1)
	source := testDescriptor(DirectionSource, 2, 1)
	leaseFD, err := server.PublishPair(sinkPipe[0], sink, sourcePipe[1], source)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(leaseFD)
	for index := 0; index < 2; index++ {
		packet := make([]byte, 1024)
		oob := make([]byte, unix.CmsgSpace(8))
		_, oobn, _, _, err := unix.Recvmsg(second, packet, oob, 0)
		if err != nil {
			t.Fatal(err)
		}
		messages, err := unix.ParseSocketControlMessage(oob[:oobn])
		if err != nil || len(messages) != 1 {
			t.Fatalf("replacement control messages %d=%d,%v", index, len(messages), err)
		}
		rights, err := unix.ParseUnixRights(&messages[0])
		wantRights := 1
		if index == 0 {
			wantRights = 2
		}
		if err != nil || len(rights) != wantRights {
			t.Fatalf("replacement rights %d=%v,%v", index, rights, err)
		}
		for _, fd := range rights {
			unix.Close(fd)
		}
	}
}

func testDescriptor(direction Direction, generation, bundle uint64) Descriptor {
	return Descriptor{
		Direction: direction, Generation: generation, BundleID: bundle,
		OwnershipTransferred: true, Linked: true,
		LifecycleOwned: direction == DirectionSink,
		SampleRate:     32000, FrameDurationUS: 10000, OctetsPerFrame: 80,
		ChannelAllocation: FrontCenterLocation, IntervalUS: 10000, PresentationDelayUS: 40000,
		ReadMTU: 80, WriteMTU: 80, LatencyMS: 10, Framing: 0, PHY: 2,
		Retransmissions: 2, TargetLatency: 2, CIG: 1, CIS: 2,
		Transport: "/org/bluez/hci0/dev_AA/fd0",
	}
}

func TestDescriptorRoundTripAndValidation(t *testing.T) {
	descriptor := testDescriptor(DirectionSink, 1, 1)
	packet, err := encodeDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeDescriptor(packet)
	if err != nil || decoded != descriptor {
		t.Fatalf("decode=%#v,%v", decoded, err)
	}
	for _, invalid := range [][]byte{packet[:8], append(append([]byte{}, packet...), 0), bytes.Repeat([]byte{0xff}, handoffHeaderSize)} {
		if _, err := decodeDescriptor(invalid); err == nil {
			t.Fatalf("accepted invalid packet %x", invalid)
		}
	}
	reserved := append([]byte(nil), packet...)
	reserved[68] = 1
	if _, err := decodeDescriptor(reserved); err == nil {
		t.Fatal("accepted nonzero reserved header")
	}
	unknownFlag := append([]byte(nil), packet...)
	unknownFlag[7] |= 0x80
	if _, err := decodeDescriptor(unknownFlag); err == nil {
		t.Fatal("accepted unknown handoff flag")
	}
	descriptor.CallControlToken = 9
	if _, err := encodeDescriptor(descriptor); err == nil {
		t.Fatal("accepted uncorrelated call-control token")
	}
	descriptor.Transport = "/org/bluez/hci0/dev_AA/\x00fd0"
	if _, err := encodeDescriptor(descriptor); err == nil {
		t.Fatal("accepted NUL in transport path")
	}
}

func TestSeqpacketSCMRightsAndStalePair(t *testing.T) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(pair[0])
	defer unix.Close(pair[1])
	sinkPipe := []int{0, 0}
	if err := unix.Pipe2(sinkPipe, unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sinkPipe[0])
	defer unix.Close(sinkPipe[1])
	sourcePipe := []int{0, 0}
	if err := unix.Pipe2(sourcePipe, unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sourcePipe[0])
	defer unix.Close(sourcePipe[1])

	server := &HandoffServer{clientFD: pair[0], listenerFD: -1}
	sink := testDescriptor(DirectionSink, 1, 1)
	source := testDescriptor(DirectionSource, 2, 1)
	leaseFD, err := server.PublishPair(sinkPipe[0], sink, sourcePipe[1], source)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(leaseFD)
	for index, want := range []Descriptor{sink, source} {
		packet := make([]byte, 1024)
		oob := make([]byte, unix.CmsgSpace(8))
		n, oobn, _, _, recvErr := unix.Recvmsg(pair[1], packet, oob, 0)
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		got, decodeErr := decodeDescriptor(packet[:n])
		if decodeErr != nil || got != want {
			t.Fatalf("descriptor %d=%#v,%v", index, got, decodeErr)
		}
		messages, parseErr := unix.ParseSocketControlMessage(oob[:oobn])
		if parseErr != nil || len(messages) != 1 {
			t.Fatalf("control messages %d=%d,%v", index, len(messages), parseErr)
		}
		rights, rightsErr := unix.ParseUnixRights(&messages[0])
		wantRights := 1
		if index == 0 {
			wantRights = 2
		}
		if rightsErr != nil || len(rights) != wantRights {
			t.Fatalf("rights %d=%v,%v", index, rights, rightsErr)
		}
		for _, fd := range rights {
			unix.Close(fd)
		}
	}
	if leaseFD, err := server.PublishPair(sinkPipe[0], sink, sourcePipe[1], source); err == nil {
		unix.Close(leaseFD)
		t.Fatal("accepted duplicate bundle")
	}
}

func TestHandoffPairCanShareOneLinkedCISFileDescription(t *testing.T) {
	handoff, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(handoff[0])
	defer unix.Close(handoff[1])
	iso, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(iso[1])
	linked, err := duplicateLinkedFD(iso[0])
	if err != nil {
		t.Fatal(err)
	}

	server := &HandoffServer{clientFD: handoff[0], listenerFD: -1}
	sink := testDescriptor(DirectionSink, 1, 1)
	source := testDescriptor(DirectionSource, 2, 1)
	leaseFD, err := server.PublishPair(iso[0], sink, linked, source)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(leaseFD)

	received := make([]int, 0, 2)
	for index := 0; index < 2; index++ {
		packet := make([]byte, 1024)
		oob := make([]byte, unix.CmsgSpace(8))
		_, oobn, _, _, recvErr := unix.Recvmsg(handoff[1], packet, oob, 0)
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		messages, parseErr := unix.ParseSocketControlMessage(oob[:oobn])
		if parseErr != nil || len(messages) != 1 {
			t.Fatalf("control messages %d=%d,%v", index, len(messages), parseErr)
		}
		rights, rightsErr := unix.ParseUnixRights(&messages[0])
		wantRights := 1
		if index == 0 {
			wantRights = 2
		}
		if rightsErr != nil || len(rights) != wantRights {
			t.Fatalf("rights %d=%v,%v", index, rights, rightsErr)
		}
		received = append(received, rights[0])
		for _, fd := range rights[1:] {
			unix.Close(fd)
		}
	}
	defer unix.Close(received[0])
	defer unix.Close(received[1])
	unix.Close(iso[0])
	unix.Close(linked)

	if _, err := unix.Write(iso[1], []byte("lc3-in")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 16)
	count, err := unix.Read(received[0], buffer)
	if err != nil || string(buffer[:count]) != "lc3-in" {
		t.Fatalf("sink read count=%d value=%q err=%v", count, buffer[:count], err)
	}
	if _, err := unix.Write(received[1], []byte("lc3-out")); err != nil {
		t.Fatal(err)
	}
	count, err = unix.Read(iso[1], buffer)
	if err != nil || string(buffer[:count]) != "lc3-out" {
		t.Fatalf("source write count=%d value=%q err=%v", count, buffer[:count], err)
	}
}

func TestHandoffLifecycleCarriesGenerationProgressAndHUP(t *testing.T) {
	handoff, err := unix.Socketpair(unix.AF_UNIX,
		unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(handoff[0])
	defer unix.Close(handoff[1])
	media, err := unix.Socketpair(unix.AF_UNIX,
		unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(media[0])
	defer unix.Close(media[1])
	linked, err := duplicateLinkedFD(media[0])
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(linked)

	server := &HandoffServer{clientFD: handoff[0], listenerFD: -1}
	sink := testDescriptor(DirectionSink, 21, 21)
	source := testDescriptor(DirectionSource, 22, 21)
	leaseFD, err := server.PublishPair(media[0], sink, linked, source)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(leaseFD)

	var asteriskLifecycle int = -1
	for index := 0; index < 2; index++ {
		packet := make([]byte, 1024)
		oob := make([]byte, unix.CmsgSpace(8))
		_, oobn, _, _, recvErr := unix.Recvmsg(handoff[1], packet, oob, 0)
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		messages, parseErr := unix.ParseSocketControlMessage(oob[:oobn])
		if parseErr != nil || len(messages) != 1 {
			t.Fatalf("control messages %d=%d,%v", index, len(messages), parseErr)
		}
		rights, rightsErr := unix.ParseUnixRights(&messages[0])
		if rightsErr != nil {
			t.Fatal(rightsErr)
		}
		if index == 0 {
			if len(rights) != 2 {
				t.Fatalf("sink rights=%v", rights)
			}
			unix.Close(rights[0])
			asteriskLifecycle = rights[1]
		} else {
			if len(rights) != 1 {
				t.Fatalf("source rights=%v", rights)
			}
			unix.Close(rights[0])
		}
	}
	if asteriskLifecycle < 0 {
		t.Fatal("missing Asterisk lifecycle descriptor")
	}
	progressPacket := lifecyclePacket(sink.BundleID, sink.Generation, source.Generation)
	if _, err := unix.Write(asteriskLifecycle, progressPacket); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, lifecycleMessageSize)
	count, err := unix.Read(leaseFD, buffer)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := decodeLifecycleProgress(buffer[:count])
	if err != nil || progress.BundleID != sink.BundleID ||
		progress.SinkGeneration != sink.Generation ||
		progress.SourceGeneration != source.Generation {
		t.Fatalf("progress=%+v err=%v", progress, err)
	}
	if err := unix.Close(asteriskLifecycle); err != nil {
		t.Fatal(err)
	}
	poll := []unix.PollFd{{Fd: int32(leaseFD), Events: unix.POLLIN | unix.POLLRDHUP}}
	ready, err := unix.Poll(poll, 1000)
	if err != nil || ready != 1 ||
		poll[0].Revents&(unix.POLLHUP|unix.POLLRDHUP|unix.POLLIN) == 0 {
		t.Fatalf("lifecycle HUP ready=%d revents=%#x err=%v", ready, poll[0].Revents, err)
	}
}

func TestDescriptorAcceptsMandatorySevenPointFiveMillisecondPreset(t *testing.T) {
	descriptor := testDescriptor(DirectionSource, 1, 1)
	descriptor.SampleRate = 16000
	descriptor.FrameDurationUS = 7500
	descriptor.IntervalUS = 7500
	descriptor.OctetsPerFrame = 30
	packet, err := encodeDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeDescriptor(packet); err != nil {
		t.Fatal(err)
	}
}

func TestDescriptorPairRejectsDifferentCISBundle(t *testing.T) {
	sink := testDescriptor(DirectionSink, 1, 1)
	source := testDescriptor(DirectionSource, 2, 1)
	source.CIS = 3
	if err := validateDescriptorPair(sink, source); err == nil {
		t.Fatal("accepted mismatched bidirectional CIS")
	}
}

func FuzzDecodeDescriptor(f *testing.F) {
	seed, _ := encodeDescriptor(testDescriptor(DirectionSink, 1, 1))
	f.Add(seed)
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = decodeDescriptor(raw)
	})
}
