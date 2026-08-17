package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"callbridge.local/callbridge-sms-go/lecall/internal/gtbs"
	"callbridge.local/callbridge-sms-go/lecall/internal/protocol"
	"golang.org/x/sys/unix"
)

const maximumSocketPath = 100

type CommandHandler func(context.Context, protocol.Message) error
type SnapshotProvider func() gtbs.Snapshot

type Server struct {
	path     string
	peerUID  uint32
	device   string
	handler  CommandHandler
	snapshot SnapshotProvider

	mu         sync.Mutex
	listenerFD int
	clientFD   int
	ready      bool
	sequence   atomic.Uint64
}

func NewServer(path string, peerUID int, device string, handler CommandHandler, snapshot SnapshotProvider) (*Server, error) {
	device = protocol.NormalizeDevice(device)
	if !filepath.IsAbs(path) || len(path) > maximumSocketPath || peerUID < 0 ||
		device == "" || handler == nil || snapshot == nil {
		return nil, errors.New("invalid LE call-control server configuration")
	}
	server := &Server{path: path, peerUID: uint32(peerUID), device: device,
		handler: handler, snapshot: snapshot, listenerFD: -1, clientFD: -1}
	server.sequence.Store(uint64(time.Now().UnixNano()))
	return server, nil
}

func (s *Server) Run(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create LE call-control runtime directory: %w", err)
	}
	if info, err := os.Lstat(s.path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("LE call-control path exists and is not a socket")
		}
		return errors.New("LE call-control socket already exists; refusing replacement")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect LE call-control socket: %w", err)
	}
	listener, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("open LE call-control socket: %w", err)
	}
	if err := unix.Bind(listener, &unix.SockaddrUnix{Name: s.path}); err != nil {
		_ = unix.Close(listener)
		return fmt.Errorf("bind LE call-control socket: %w", err)
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		_ = unix.Close(listener)
		_ = os.Remove(s.path)
		return fmt.Errorf("protect LE call-control socket: %w", err)
	}
	if err := unix.Listen(listener, 1); err != nil {
		_ = unix.Close(listener)
		_ = os.Remove(s.path)
		return fmt.Errorf("listen on LE call-control socket: %w", err)
	}
	s.mu.Lock()
	if s.listenerFD != -1 {
		s.mu.Unlock()
		_ = unix.Close(listener)
		_ = os.Remove(s.path)
		return errors.New("LE call-control server already running")
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
			poll = append(poll, unix.PollFd{Fd: int32(client), Events: unix.POLLIN})
		}
		count, pollErr := unix.Poll(poll, 250)
		if pollErr != nil {
			if errors.Is(pollErr, unix.EINTR) {
				continue
			}
			return fmt.Errorf("poll LE call-control socket: %w", pollErr)
		}
		if count == 0 {
			continue
		}
		if poll[0].Revents&unix.POLLIN != 0 {
			s.accept(listener)
		}
		if len(poll) == 2 && poll[1].Revents&unix.POLLIN != 0 {
			s.receive(ctx, client)
		}
		if len(poll) == 2 && poll[1].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			s.dropClient(client)
		}
	}
	return nil
}

func (s *Server) accept(listener int) {
	client, _, err := unix.Accept4(listener, unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK)
	if err != nil {
		return
	}
	credential, credentialErr := unix.GetsockoptUcred(client, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if credentialErr != nil || credential.Uid != s.peerUID {
		_ = unix.Close(client)
		return
	}
	s.mu.Lock()
	if s.clientFD != -1 {
		s.mu.Unlock()
		_ = unix.Close(client)
		return
	}
	s.clientFD = client
	snapshot := s.snapshot()
	messages := snapshotMessages(s.device, snapshot)
	readyCode := byte(0)
	if s.ready {
		readyCode = 1
	}
	messages = append(messages, protocol.Message{Type: protocol.TypeReady,
		Sequence: s.nextSequence(), Device: s.device, Code: readyCode})
	s.sendMessagesLocked(messages)
	s.mu.Unlock()
}

func (s *Server) receive(parent context.Context, client int) {
	packet := make([]byte, protocol.PacketSize+1)
	n, _, flags, _, err := unix.Recvmsg(client, packet, nil, unix.MSG_DONTWAIT)
	if err != nil {
		if !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EWOULDBLOCK) {
			s.dropClient(client)
		}
		return
	}
	if flags&unix.MSG_TRUNC != 0 || n != protocol.PacketSize {
		s.dropClient(client)
		return
	}
	message, err := protocol.Decode(packet[:n])
	if err != nil || message.Type != protocol.TypeCommand || message.Device != s.device {
		s.dropClient(client)
		return
	}
	commandContext, cancel := context.WithTimeout(parent, 5*time.Second)
	err = s.handler(commandContext, message)
	cancel()
	ack := protocol.Message{Type: protocol.TypeAck, Sequence: message.Sequence,
		Token: message.Token, Device: s.device, Index: message.Index,
		Code: message.Code, Value: gtbs.ClassifyCommandError(err)}
	s.send(ack)
}

func (s *Server) SetReady(ready bool) {
	s.mu.Lock()
	changed := s.ready != ready
	s.ready = ready
	if changed {
		code := byte(0)
		if ready {
			code = 1
		}
		s.sendMessageLocked(protocol.Message{Type: protocol.TypeReady,
			Sequence: s.nextSequence(), Device: s.device, Code: code})
	}
	s.mu.Unlock()
}

func (s *Server) BroadcastReady(ready bool) {
	code := byte(0)
	if ready {
		code = 1
	}
	s.send(protocol.Message{Type: protocol.TypeReady, Sequence: s.nextSequence(),
		Device: s.device, Code: code})
}

func (s *Server) BroadcastSnapshot(snapshot gtbs.Snapshot) {
	s.sendMessages(snapshotMessages(s.device, snapshot))
}

func snapshotMessages(device string, snapshot gtbs.Snapshot) []protocol.Message {
	if len(snapshot.Calls) == 0 {
		return []protocol.Message{{Type: protocol.TypeState,
			Flags: protocol.FlagSnapshot | protocol.FlagLast, Sequence: snapshot.Sequence,
			Device: device, Code: protocol.NoCallState}}
	}
	messages := make([]protocol.Message, 0, len(snapshot.Calls))
	for index, call := range snapshot.Calls {
		flags := protocol.FlagSnapshot
		if index == len(snapshot.Calls)-1 {
			flags |= protocol.FlagLast
		}
		messages = append(messages, protocol.Message{Type: protocol.TypeState, Flags: flags,
			Sequence: snapshot.Sequence, Token: call.Token, Device: device,
			Index: call.Index, Code: call.State, Value: call.Flags})
	}
	return messages
}

func (s *Server) BroadcastResult(opcode, index, result byte, token uint64) {
	s.send(protocol.Message{Type: protocol.TypeResult, Sequence: s.nextSequence(),
		Token: token, Device: s.device, Index: index, Code: opcode, Value: result})
}

func (s *Server) send(message protocol.Message) {
	s.sendMessages([]protocol.Message{message})
}

func (s *Server) sendMessages(messages []protocol.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendMessagesLocked(messages)
}

func (s *Server) sendMessagesLocked(messages []protocol.Message) {
	for _, message := range messages {
		s.sendMessageLocked(message)
		if s.clientFD == -1 {
			return
		}
	}
}

func (s *Server) sendMessageLocked(message protocol.Message) {
	if s.clientFD == -1 {
		return
	}
	packet, err := message.Encode()
	if err != nil {
		return
	}
	written, sendErr := unix.SendmsgN(s.clientFD, packet, nil, nil, unix.MSG_NOSIGNAL)
	if sendErr != nil || written != len(packet) {
		s.closeClientLocked()
	}
}

func (s *Server) dropClient(client int) {
	s.mu.Lock()
	if s.clientFD == client {
		s.closeClientLocked()
	}
	s.mu.Unlock()
}

func (s *Server) closeClientLocked() {
	if s.clientFD != -1 {
		_ = unix.Close(s.clientFD)
		s.clientFD = -1
	}
}

func (s *Server) close() {
	s.mu.Lock()
	s.closeClientLocked()
	if s.listenerFD != -1 {
		_ = unix.Close(s.listenerFD)
		s.listenerFD = -1
	}
	s.mu.Unlock()
	if info, err := os.Lstat(s.path); err == nil && info.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(s.path)
	}
}

func (s *Server) nextSequence() uint64 {
	for {
		sequence := s.sequence.Add(1)
		if sequence != 0 {
			return sequence
		}
	}
}
