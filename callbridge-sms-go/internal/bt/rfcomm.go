package bt

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type Conn struct {
	fd   int
	once sync.Once
}

func ParseAddress(value string) ([6]byte, error) {
	var address [6]byte
	parts := strings.Split(value, ":")
	if len(parts) != 6 {
		return address, errors.New("invalid Bluetooth address")
	}
	for index, part := range parts {
		if len(part) != 2 {
			return address, errors.New("invalid Bluetooth address")
		}
		parsed, err := strconv.ParseUint(part, 16, 8)
		if err != nil {
			return address, errors.New("invalid Bluetooth address")
		}
		address[5-index] = byte(parsed)
	}
	return address, nil
}

func Connect(addressText string, channel uint8, timeout time.Duration) (*Conn, error) {
	address, err := ParseAddress(addressText)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_BLUETOOTH, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, unix.BTPROTO_RFCOMM)
	if err != nil {
		return nil, fmt.Errorf("create RFCOMM socket: %w", err)
	}
	connected := false
	defer func() {
		if !connected {
			_ = unix.Close(fd)
		}
	}()
	err = unix.Connect(fd, &unix.SockaddrRFCOMM{Addr: address, Channel: channel})
	if err != nil && !errors.Is(err, unix.EINPROGRESS) {
		return nil, fmt.Errorf("connect RFCOMM: %w", err)
	}
	if errors.Is(err, unix.EINPROGRESS) {
		pollFDs := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
		ready, pollErr := unix.Poll(pollFDs, int(timeout.Milliseconds()))
		if pollErr != nil {
			return nil, fmt.Errorf("wait RFCOMM: %w", pollErr)
		}
		if ready != 1 {
			return nil, errors.New("RFCOMM connection timed out")
		}
		result, socketErr := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ERROR)
		if socketErr != nil {
			return nil, fmt.Errorf("inspect RFCOMM connection: %w", socketErr)
		}
		if result != 0 {
			return nil, fmt.Errorf("connect RFCOMM: %w", syscall.Errno(result))
		}
	}
	if err := unix.SetNonblock(fd, false); err != nil {
		return nil, fmt.Errorf("set RFCOMM blocking mode: %w", err)
	}
	conn := &Conn{fd: fd}
	if err := conn.SetTimeout(timeout); err != nil {
		return nil, err
	}
	connected = true
	return conn, nil
}

func FromFD(fd int, timeout time.Duration) (*Conn, error) {
	if fd < 0 {
		return nil, errors.New("invalid RFCOMM descriptor")
	}
	unix.CloseOnExec(fd)
	if err := unix.SetNonblock(fd, false); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("set passed RFCOMM descriptor blocking: %w", err)
	}
	conn := &Conn{fd: fd}
	if err := conn.SetTimeout(timeout); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *Conn) SetTimeout(timeout time.Duration) error {
	value := unix.NsecToTimeval(timeout.Nanoseconds())
	if err := unix.SetsockoptTimeval(c.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &value); err != nil {
		return fmt.Errorf("set RFCOMM receive timeout: %w", err)
	}
	if err := unix.SetsockoptTimeval(c.fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, &value); err != nil {
		return fmt.Errorf("set RFCOMM send timeout: %w", err)
	}
	return nil
}

func (c *Conn) ReadFull(buffer []byte) error {
	offset := 0
	for offset < len(buffer) {
		count, err := unix.Read(c.fd, buffer[offset:])
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if count == 0 {
			return io.ErrUnexpectedEOF
		}
		offset += count
	}
	return nil
}

func (c *Conn) WriteAll(buffer []byte) error {
	offset := 0
	for offset < len(buffer) {
		count, err := unix.Write(c.fd, buffer[offset:])
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		offset += count
	}
	return nil
}

func (c *Conn) Close() error {
	var err error
	c.once.Do(func() { err = unix.Close(c.fd) })
	return err
}
