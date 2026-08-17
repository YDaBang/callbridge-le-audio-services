package bluez

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	handoffVersion    = byte(3)
	handoffHeaderSize = 72
	descriptorCount   = byte(1)
	maximumSocketPath = 100
	maximumTransport  = 512

	handoffFlagOwnership = byte(1 << 0)
	handoffFlagLinked    = byte(1 << 1)
	handoffFlagCallToken = byte(1 << 2)
	handoffFlagLifecycle = byte(1 << 3)
	handoffKnownFlags    = handoffFlagOwnership | handoffFlagLinked | handoffFlagCallToken | handoffFlagLifecycle

	lifecycleVersion       = byte(1)
	lifecycleTypeProgress  = byte(1)
	lifecycleTypeNormalEnd = byte(2)
	lifecycleMessageSize   = 32
)

var handoffMagic = [4]byte{'G', 'G', 'L', 'E'}
var lifecycleMagic = [4]byte{'G', 'G', 'L', 'C'}

type Direction uint8

const (
	DirectionSink   Direction = 1
	DirectionSource Direction = 2
)

type Descriptor struct {
	Direction             Direction
	Generation            uint64
	BundleID              uint64
	CallControlToken      uint64
	CallControlCorrelated bool
	OwnershipTransferred  bool
	Linked                bool
	LifecycleOwned        bool
	SampleRate            uint32
	FrameDurationUS       uint32
	OctetsPerFrame        uint16
	ChannelAllocation     uint32
	IntervalUS            uint32
	PresentationDelayUS   uint32
	ReadMTU               uint16
	WriteMTU              uint16
	LatencyMS             uint16
	Framing               byte
	PHY                   byte
	Retransmissions       byte
	TargetLatency         byte
	CIG                   byte
	CIS                   byte
	Transport             string
}

type HandoffServer struct {
	path              string
	peerUID           uint32
	mu                sync.Mutex
	listenerFD        int
	clientFD          int
	lastGen           [3]uint64
	lastBundle        uint64
	disconnectHandler func()
}

func (s *HandoffServer) SetDisconnectHandler(handler func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listenerFD != -1 {
		return errors.New("LE Audio handoff disconnect handler changed after start")
	}
	s.disconnectHandler = handler
	return nil
}

func NewHandoffServer(path string, peerUID int) (*HandoffServer, error) {
	if !filepath.IsAbs(path) || len(path) > maximumSocketPath || peerUID < 0 {
		return nil, errors.New("invalid LE Audio handoff configuration")
	}
	return &HandoffServer{path: path, peerUID: uint32(peerUID), listenerFD: -1, clientFD: -1}, nil
}

func (s *HandoffServer) Run(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create LE Audio runtime directory: %w", err)
	}
	if info, err := os.Lstat(s.path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("LE Audio handoff path exists and is not a socket")
		}
		return errors.New("LE Audio handoff socket already exists; refusing replacement")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect LE Audio handoff path: %w", err)
	}

	listener, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open LE Audio handoff socket: %w", err)
	}
	if err := unix.Bind(listener, &unix.SockaddrUnix{Name: s.path}); err != nil {
		_ = unix.Close(listener)
		return fmt.Errorf("bind LE Audio handoff socket: %w", err)
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		_ = unix.Close(listener)
		_ = os.Remove(s.path)
		return fmt.Errorf("protect LE Audio handoff socket: %w", err)
	}
	if err := unix.Listen(listener, 1); err != nil {
		_ = unix.Close(listener)
		_ = os.Remove(s.path)
		return fmt.Errorf("listen on LE Audio handoff socket: %w", err)
	}
	if err := unix.SetNonblock(listener, true); err != nil {
		_ = unix.Close(listener)
		_ = os.Remove(s.path)
		return fmt.Errorf("configure LE Audio handoff socket: %w", err)
	}

	s.mu.Lock()
	if s.listenerFD != -1 {
		s.mu.Unlock()
		_ = unix.Close(listener)
		_ = os.Remove(s.path)
		return errors.New("LE Audio handoff server already running")
	}
	s.listenerFD = listener
	s.mu.Unlock()
	defer s.close()

	for ctx.Err() == nil {
		s.mu.Lock()
		client := s.clientFD
		s.mu.Unlock()
		poll := []unix.PollFd{{Fd: int32(listener), Events: unix.POLLIN}}
		if client != -1 {
			// The handoff protocol is server-to-client only. Readability therefore
			// means either EOF or an invalid peer packet; both release the slot.
			poll = append(poll, unix.PollFd{Fd: int32(client), Events: unix.POLLIN | unix.POLLRDHUP})
		}
		count, pollErr := unix.Poll(poll, 250)
		if pollErr != nil {
			if errors.Is(pollErr, unix.EINTR) {
				continue
			}
			return fmt.Errorf("poll LE Audio handoff socket: %w", pollErr)
		}
		if count == 0 {
			continue
		}
		if client != -1 && poll[1].Revents&(unix.POLLIN|unix.POLLRDHUP|unix.POLLHUP|unix.POLLERR|unix.POLLNVAL) != 0 {
			var disconnected func()
			s.mu.Lock()
			if s.clientFD == client {
				_ = unix.Close(s.clientFD)
				s.clientFD = -1
				disconnected = s.disconnectHandler
			}
			s.mu.Unlock()
			if disconnected != nil {
				disconnected()
			}
		}
		if poll[0].Revents&unix.POLLIN == 0 {
			if poll[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
				return errors.New("LE Audio handoff listener failed")
			}
			continue
		}
		client, _, acceptErr := unix.Accept4(listener, unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK)
		if acceptErr != nil {
			if errors.Is(acceptErr, unix.EAGAIN) || errors.Is(acceptErr, unix.EWOULDBLOCK) {
				continue
			}
			return fmt.Errorf("accept LE Audio handoff peer: %w", acceptErr)
		}
		credential, credentialErr := unix.GetsockoptUcred(client, unix.SOL_SOCKET, unix.SO_PEERCRED)
		if credentialErr != nil || credential.Uid != s.peerUID {
			_ = unix.Close(client)
			continue
		}
		s.mu.Lock()
		if s.clientFD != -1 {
			s.mu.Unlock()
			_ = unix.Close(client)
			continue
		}
		s.clientFD = client
		s.mu.Unlock()
	}
	return nil
}

func (s *HandoffServer) PublishPair(sinkFD int, sink Descriptor, sourceFD int, source Descriptor) (int, error) {
	sink.LifecycleOwned = true
	source.LifecycleOwned = false
	if err := validateDescriptorPair(sink, source); err != nil {
		return -1, err
	}
	sinkPacket, err := encodeDescriptor(sink)
	if err != nil {
		return -1, err
	}
	sourcePacket, err := encodeDescriptor(source)
	if err != nil {
		return -1, err
	}
	if sinkFD < 0 || sourceFD < 0 || sinkFD == sourceFD {
		return -1, errors.New("invalid LE Audio transport descriptor pair")
	}
	lifecycle, err := unix.Socketpair(unix.AF_UNIX,
		unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return -1, fmt.Errorf("open LE Audio lifecycle socket: %w", err)
	}
	keepLifecycle := false
	defer func() {
		if !keepLifecycle {
			_ = unix.Close(lifecycle[0])
		}
		_ = unix.Close(lifecycle[1])
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	if sink.Generation <= s.lastGen[DirectionSink] || source.Generation <= s.lastGen[DirectionSource] ||
		sink.BundleID <= s.lastBundle {
		return -1, errors.New("stale LE Audio generation")
	}
	if s.clientFD == -1 {
		return -1, errors.New("LE Audio media peer is not connected")
	}
	if err := s.sendDescriptorLocked(sinkPacket, sinkFD, lifecycle[1]); err != nil {
		return -1, err
	}
	if err := s.sendDescriptorLocked(sourcePacket, sourceFD); err != nil {
		return -1, err
	}
	s.lastGen[DirectionSink] = sink.Generation
	s.lastGen[DirectionSource] = source.Generation
	s.lastBundle = sink.BundleID
	keepLifecycle = true
	return lifecycle[0], nil
}

func (s *HandoffServer) sendDescriptorLocked(packet []byte, fds ...int) error {
	if len(fds) == 0 {
		return errors.New("missing LE Audio handoff descriptors")
	}
	written, sendErr := unix.SendmsgN(s.clientFD, packet, unix.UnixRights(fds...), nil, unix.MSG_NOSIGNAL)
	if sendErr == nil && written == len(packet) {
		return nil
	}
	_ = unix.Close(s.clientFD)
	s.clientFD = -1
	if sendErr != nil {
		return fmt.Errorf("send LE Audio transport: %w", sendErr)
	}
	return errors.New("short LE Audio descriptor handoff")
}

func (s *HandoffServer) close() {
	s.mu.Lock()
	if s.clientFD != -1 {
		_ = unix.Close(s.clientFD)
		s.clientFD = -1
	}
	if s.listenerFD != -1 {
		_ = unix.Close(s.listenerFD)
		s.listenerFD = -1
	}
	s.mu.Unlock()
	if info, err := os.Lstat(s.path); err == nil && info.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(s.path)
	}
}

func encodeDescriptor(descriptor Descriptor) ([]byte, error) {
	if err := validateDescriptor(descriptor); err != nil {
		return nil, err
	}
	packet := make([]byte, handoffHeaderSize+len(descriptor.Transport))
	copy(packet[:4], handoffMagic[:])
	packet[4] = handoffVersion
	packet[5] = byte(descriptor.Direction)
	packet[6] = descriptorCount
	packet[7] = handoffFlagOwnership | handoffFlagLinked
	if descriptor.CallControlCorrelated {
		packet[7] |= handoffFlagCallToken
	}
	if descriptor.LifecycleOwned {
		packet[7] |= handoffFlagLifecycle
	}
	binary.LittleEndian.PutUint64(packet[8:16], descriptor.Generation)
	binary.LittleEndian.PutUint64(packet[16:24], descriptor.BundleID)
	binary.LittleEndian.PutUint64(packet[24:32], descriptor.CallControlToken)
	binary.LittleEndian.PutUint32(packet[32:36], descriptor.SampleRate)
	binary.LittleEndian.PutUint32(packet[36:40], descriptor.FrameDurationUS)
	binary.LittleEndian.PutUint32(packet[40:44], descriptor.ChannelAllocation)
	binary.LittleEndian.PutUint32(packet[44:48], descriptor.IntervalUS)
	binary.LittleEndian.PutUint32(packet[48:52], descriptor.PresentationDelayUS)
	binary.LittleEndian.PutUint16(packet[52:54], descriptor.OctetsPerFrame)
	binary.LittleEndian.PutUint16(packet[54:56], descriptor.ReadMTU)
	binary.LittleEndian.PutUint16(packet[56:58], descriptor.WriteMTU)
	binary.LittleEndian.PutUint16(packet[58:60], descriptor.LatencyMS)
	packet[60] = descriptor.Framing
	packet[61] = descriptor.PHY
	packet[62] = descriptor.Retransmissions
	packet[63] = descriptor.TargetLatency
	packet[64] = descriptor.CIG
	packet[65] = descriptor.CIS
	binary.LittleEndian.PutUint16(packet[66:68], uint16(len(descriptor.Transport)))
	copy(packet[handoffHeaderSize:], descriptor.Transport)
	return packet, nil
}

func decodeDescriptor(packet []byte) (Descriptor, error) {
	if len(packet) < handoffHeaderSize || string(packet[:4]) != string(handoffMagic[:]) ||
		packet[4] != handoffVersion || packet[6] != descriptorCount ||
		packet[7]&^handoffKnownFlags != 0 || packet[7]&handoffFlagOwnership == 0 ||
		packet[7]&handoffFlagLinked == 0 || binary.LittleEndian.Uint32(packet[68:72]) != 0 {
		return Descriptor{}, errors.New("invalid LE Audio handoff header")
	}
	pathLength := int(binary.LittleEndian.Uint16(packet[66:68]))
	if pathLength < 1 || pathLength > maximumTransport || len(packet) != handoffHeaderSize+pathLength {
		return Descriptor{}, errors.New("invalid LE Audio handoff length")
	}
	descriptor := Descriptor{
		Direction: Direction(packet[5]), Generation: binary.LittleEndian.Uint64(packet[8:16]),
		BundleID: binary.LittleEndian.Uint64(packet[16:24]), CallControlToken: binary.LittleEndian.Uint64(packet[24:32]),
		CallControlCorrelated: packet[7]&handoffFlagCallToken != 0,
		OwnershipTransferred:  true, Linked: true,
		LifecycleOwned: packet[7]&handoffFlagLifecycle != 0,
		SampleRate:     binary.LittleEndian.Uint32(packet[32:36]), FrameDurationUS: binary.LittleEndian.Uint32(packet[36:40]),
		ChannelAllocation: binary.LittleEndian.Uint32(packet[40:44]), IntervalUS: binary.LittleEndian.Uint32(packet[44:48]),
		PresentationDelayUS: binary.LittleEndian.Uint32(packet[48:52]), OctetsPerFrame: binary.LittleEndian.Uint16(packet[52:54]),
		ReadMTU: binary.LittleEndian.Uint16(packet[54:56]), WriteMTU: binary.LittleEndian.Uint16(packet[56:58]),
		LatencyMS: binary.LittleEndian.Uint16(packet[58:60]), Framing: packet[60], PHY: packet[61],
		Retransmissions: packet[62], TargetLatency: packet[63], CIG: packet[64], CIS: packet[65],
		Transport: string(packet[handoffHeaderSize:]),
	}
	if _, err := encodeDescriptor(descriptor); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

func validateDescriptor(descriptor Descriptor) error {
	if descriptor.Direction != DirectionSink && descriptor.Direction != DirectionSource {
		return errors.New("invalid LE Audio direction")
	}
	if descriptor.Generation == 0 || descriptor.BundleID == 0 {
		return errors.New("invalid LE Audio generation or bundle")
	}
	if !descriptor.OwnershipTransferred || !descriptor.Linked {
		return errors.New("LE Audio descriptor is not an owned linked transport")
	}
	if descriptor.LifecycleOwned != (descriptor.Direction == DirectionSink) {
		return errors.New("LE Audio lifecycle ownership must belong to the sink descriptor")
	}
	if descriptor.CallControlCorrelated != (descriptor.CallControlToken != 0) {
		return errors.New("invalid LE Audio call-control token")
	}
	presetOK := false
	for _, candidate := range conversationalPresets {
		if descriptor.SampleRate == uint32(candidate.sampleRate) &&
			descriptor.FrameDurationUS == uint32(candidate.frameDuration/time.Microsecond) &&
			descriptor.OctetsPerFrame == uint16(candidate.octets) {
			presetOK = true
			break
		}
	}
	if !presetOK || descriptor.IntervalUS != descriptor.FrameDurationUS {
		return errors.New("invalid LE Audio codec or interval preset")
	}
	if descriptor.ChannelAllocation == 0 || bits.OnesCount32(descriptor.ChannelAllocation) != 1 {
		return errors.New("invalid LE Audio channel allocation")
	}
	if descriptor.Framing > 1 || descriptor.PHY&0x02 == 0 || descriptor.Retransmissions == 0 ||
		descriptor.Retransmissions > 13 || descriptor.TargetLatency < 1 || descriptor.TargetLatency > 3 ||
		descriptor.CIG > 0xef || descriptor.CIS > 0xef {
		return errors.New("invalid LE Audio transport parameters")
	}
	if descriptor.LatencyMS == 0 || descriptor.LatencyMS > 100 ||
		descriptor.PresentationDelayUS == 0 || descriptor.PresentationDelayUS > 0x00ffffff {
		return errors.New("invalid LE Audio latency")
	}
	if descriptor.ReadMTU > 4096 || descriptor.WriteMTU > 4096 ||
		(descriptor.Direction == DirectionSink && descriptor.ReadMTU < descriptor.OctetsPerFrame) ||
		(descriptor.Direction == DirectionSource && descriptor.WriteMTU < descriptor.OctetsPerFrame) {
		return errors.New("invalid LE Audio MTU")
	}
	if len(descriptor.Transport) == 0 || len(descriptor.Transport) > maximumTransport ||
		descriptor.Transport[0] != '/' || strings.ContainsRune(descriptor.Transport, '\x00') {
		return errors.New("invalid BlueZ transport path")
	}
	return nil
}

func validateDescriptorPair(sink, source Descriptor) error {
	if sink.Direction != DirectionSink || source.Direction != DirectionSource {
		return errors.New("LE Audio pair directions are incomplete")
	}
	if sink.BundleID != source.BundleID || sink.SampleRate != source.SampleRate ||
		sink.FrameDurationUS != source.FrameDurationUS || sink.OctetsPerFrame != source.OctetsPerFrame ||
		sink.IntervalUS != source.IntervalUS || sink.Framing != source.Framing || sink.PHY != source.PHY ||
		sink.CIG != source.CIG || sink.CIS != source.CIS ||
		sink.CallControlCorrelated != source.CallControlCorrelated ||
		sink.CallControlToken != source.CallControlToken {
		return errors.New("LE Audio transports are not one bidirectional CIS bundle")
	}
	return nil
}

type lifecycleProgress struct {
	BundleID         uint64
	SinkGeneration   uint64
	SourceGeneration uint64
}

func decodeLifecycleMessage(packet []byte) (byte, lifecycleProgress, error) {
	if len(packet) != lifecycleMessageSize || string(packet[:4]) != string(lifecycleMagic[:]) ||
		packet[4] != lifecycleVersion ||
		(packet[5] != lifecycleTypeProgress && packet[5] != lifecycleTypeNormalEnd) ||
		packet[6] != 0 || packet[7] != 0 {
		return 0, lifecycleProgress{}, errors.New("invalid LE Audio lifecycle message")
	}
	progress := lifecycleProgress{
		BundleID:         binary.LittleEndian.Uint64(packet[8:16]),
		SinkGeneration:   binary.LittleEndian.Uint64(packet[16:24]),
		SourceGeneration: binary.LittleEndian.Uint64(packet[24:32]),
	}
	if progress.BundleID == 0 || progress.SinkGeneration == 0 || progress.SourceGeneration == 0 {
		return 0, lifecycleProgress{}, errors.New("invalid LE Audio lifecycle identity")
	}
	return packet[5], progress, nil
}

func decodeLifecycleProgress(packet []byte) (lifecycleProgress, error) {
	typeID, progress, err := decodeLifecycleMessage(packet)
	if err != nil {
		return lifecycleProgress{}, err
	}
	if typeID != lifecycleTypeProgress {
		return lifecycleProgress{}, errors.New("LE Audio lifecycle message is not progress")
	}
	return progress, nil
}
