package mapclient

import (
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"callbridge.local/callbridge-sms-go/internal/bt"
	"callbridge.local/callbridge-sms-go/internal/model"
	"callbridge.local/callbridge-sms-go/internal/obex"
)

const (
	opConnect    = 0x80
	opDisconnect = 0x81
	opPut        = 0x02
	opPutFinal   = 0x82
	opGetFinal   = 0x83
	opSetPath    = 0x85

	headerName         = 0x01
	headerType         = 0x42
	headerBody         = 0x48
	headerEndOfBody    = 0x49
	headerTarget       = 0x46
	headerAppParams    = 0x4c
	headerConnectionID = 0xcb

	tagNotificationStatus = 0x0e
	tagAttachment         = 0x0a
	tagTransparent        = 0x0b
	tagRetry              = 0x0c
	tagCharset            = 0x14
	tagFractionRequest    = 0x15

	maxCollectedBody = 2 << 20
	unknownPath      = "\x00"
)

var masTargetUUID = []byte{0xbb, 0x58, 0x2b, 0x40, 0x42, 0x0c, 0x11, 0xdb, 0xb0, 0xde, 0x08, 0x00, 0x20, 0x0c, 0x9a, 0x66}

type Session struct {
	conn        transport
	connection  uint32
	packetMax   int
	currentPath string
}

type transport interface {
	obex.Transport
	Close() error
}

func Connect(device string, channel uint8, timeout time.Duration) (*Session, error) {
	conn, err := bt.Connect(device, channel, timeout)
	if err != nil {
		return nil, err
	}
	session := &Session{conn: conn, packetMax: 0xffff}
	connected := false
	defer func() {
		if !connected {
			_ = conn.Close()
		}
	}()
	payload := []byte{0x10, 0x00, 0x40, 0x00}
	payload = append(payload, obex.MustHeader(headerTarget, masTargetUUID)...)
	response, err := session.request(opConnect, payload)
	if err != nil {
		return nil, fmt.Errorf("MAP connect: %w", err)
	}
	if response.Code != obex.ResponseOK || len(response.Payload) < 4 {
		return nil, fmt.Errorf("MAP connect rejected: 0x%02x", response.Code)
	}
	peerMax := int(binary.BigEndian.Uint16(response.Payload[2:4]))
	if peerMax >= 255 && peerMax < session.packetMax {
		session.packetMax = peerMax
	}
	headers, err := obex.ParseHeaders(response.Payload, 4)
	if err != nil {
		return nil, fmt.Errorf("parse MAP connect response: %w", err)
	}
	rawID, ok := obex.Find(headers, headerConnectionID)
	if !ok {
		return nil, errors.New("MAP connect response lacks connection ID")
	}
	session.connection, err = obex.Uint32(rawID)
	if err != nil {
		return nil, err
	}
	connected = true
	return session, nil
}

func (s *Session) Close() error {
	if s.conn == nil {
		return nil
	}
	_ = s.disconnect()
	err := s.conn.Close()
	s.conn = nil
	return err
}

func (s *Session) RegisterNotifications(enabled bool) error {
	status := byte(0)
	if enabled {
		status = 1
	}
	params := []byte{tagNotificationStatus, 0x01, status}
	payload := s.connectionHeader()
	payload = append(payload, obex.MustHeader(headerType, []byte("x-bt/MAP-NotificationRegistration"))...)
	payload = append(payload, obex.MustHeader(headerAppParams, params)...)
	payload = append(payload, obex.MustHeader(headerEndOfBody, []byte("0"))...)
	response, err := s.request(opPutFinal, payload)
	if err != nil {
		return err
	}
	if response.Code != obex.ResponseOK {
		return fmt.Errorf("notification registration rejected: 0x%02x", response.Code)
	}
	return nil
}

func (s *Session) Keepalive() error {
	return s.resetPath(true)
}

func (s *Session) ListMessages(folder string, maximum, offset int) ([]model.ListingMessage, error) {
	if maximum < 1 || maximum > 500 || offset < 0 || offset > 65535 {
		return nil, errors.New("message listing bounds invalid")
	}
	if err := s.gotoFolder(folder); err != nil {
		return nil, err
	}
	params := []byte{
		0x01, 0x02, byte(maximum >> 8), byte(maximum),
		0x02, 0x02, byte(offset >> 8), byte(offset),
		0x13, 0x01, 0x00,
	}
	payload := s.connectionHeader()
	payload = append(payload, obex.MustHeader(headerType, []byte("x-bt/MAP-msg-listing\x00"))...)
	payload = append(payload, obex.MustHeader(headerAppParams, params)...)
	body, err := s.collectGET(payload)
	if err != nil {
		return nil, err
	}
	return parseListing(body)
}

func (s *Session) GetMessage(folder, handle string) ([]byte, error) {
	if !safeHandle(handle) {
		return nil, errors.New("invalid message handle")
	}
	if err := s.gotoFolder(folder); err != nil {
		return nil, err
	}
	params := []byte{
		tagAttachment, 0x01, 0x01,
		tagCharset, 0x01, 0x01,
		tagFractionRequest, 0x01, 0x00,
	}
	payload := s.connectionHeader()
	payload = append(payload, obex.NameHeader(headerName, handle)...)
	payload = append(payload, obex.MustHeader(headerType, []byte("x-bt/message"))...)
	payload = append(payload, obex.MustHeader(headerAppParams, params)...)
	return s.collectGET(payload)
}

func (s *Session) SendSMS(to, text string) error {
	return s.PushBMessage("telecom/msg/outbox", MakeSMSBMessage(to, text), false, true)
}

func (s *Session) PushBMessage(folder, body string, transparent, retry bool) error {
	if body == "" || len(body) > 60<<10 {
		return errors.New("bMessage size invalid")
	}
	if err := s.gotoFolder(folder); err != nil {
		return err
	}
	transparentByte := byte(0)
	if transparent {
		transparentByte = 1
	}
	retryByte := byte(0)
	if retry {
		retryByte = 1
	}
	params := []byte{
		tagTransparent, 0x01, transparentByte,
		tagRetry, 0x01, retryByte,
		tagCharset, 0x01, 0x01,
	}
	first := s.connectionHeader()
	first = append(first, obex.MustHeader(headerType, []byte("x-bt/message\x00"))...)
	first = append(first, obex.MustHeader(headerAppParams, params)...)
	return s.putBody(first, []byte(body))
}

func (s *Session) putBody(firstHeaders, body []byte) error {
	headers := firstHeaders
	for {
		available := s.packetMax - 3 - len(headers) - 3
		if available < 1 {
			return errors.New("OBEX packet has no body capacity")
		}
		final := len(body) <= available
		chunkSize := available
		if final {
			chunkSize = len(body)
		}
		headerID := byte(headerBody)
		opcode := byte(opPut)
		if final {
			headerID = headerEndOfBody
			opcode = opPutFinal
		}
		payload := append([]byte(nil), headers...)
		payload = append(payload, obex.MustHeader(headerID, body[:chunkSize])...)
		response, err := s.request(opcode, payload)
		if err != nil {
			return err
		}
		if final {
			if response.Code != obex.ResponseOK {
				return fmt.Errorf("PushMessage rejected: 0x%02x", response.Code)
			}
			return nil
		}
		if response.Code != obex.ResponseContinue {
			return fmt.Errorf("PushMessage continuation rejected: 0x%02x", response.Code)
		}
		body = body[chunkSize:]
		headers = s.connectionHeader()
	}
}

func MakeSMSBMessage(destination, text string) string {
	messagePart := "BEGIN:MSG\r\n" + text + "\r\nEND:MSG\r\n"
	return "BEGIN:BMSG\r\n" +
		"VERSION:1.0\r\n" +
		"STATUS:UNREAD\r\n" +
		"TYPE:SMS_GSM\r\n" +
		"FOLDER:telecom/msg/outbox\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nN:\r\nTEL:\r\nEND:VCARD\r\n" +
		"BEGIN:BENV\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nN:\r\nTEL:" + destination + "\r\nEND:VCARD\r\n" +
		"BEGIN:BBODY\r\nCHARSET:UTF-8\r\n" +
		fmt.Sprintf("LENGTH:%d\r\n", len([]byte(messagePart))) +
		messagePart +
		"END:BBODY\r\nEND:BENV\r\nEND:BMSG\r\n"
}

func (s *Session) collectGET(initial []byte) ([]byte, error) {
	response, err := s.request(opGetFinal, initial)
	if err != nil {
		return nil, err
	}
	var body []byte
	for {
		headers, parseErr := obex.ParseHeaders(response.Payload, 0)
		if parseErr != nil {
			return nil, parseErr
		}
		body = append(body, obex.CollectBody(headers, headerBody, headerEndOfBody)...)
		if len(body) > maxCollectedBody {
			return nil, errors.New("MAP response body exceeds bound")
		}
		if response.Code == obex.ResponseOK {
			return body, nil
		}
		if response.Code != obex.ResponseContinue {
			return nil, fmt.Errorf("MAP GET rejected: 0x%02x", response.Code)
		}
		response, err = s.request(opGetFinal, s.connectionHeader())
		if err != nil {
			return nil, err
		}
	}
}

func (s *Session) gotoFolder(folder string) error {
	folder = strings.Trim(folder, "/")
	parts := strings.Split(folder, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "\x00\r\n") {
			return errors.New("invalid MAP folder")
		}
	}
	if folder == s.currentPath {
		return nil
	}
	if err := s.resetPath(false); err != nil {
		return err
	}
	for _, part := range parts {
		payload := []byte{0x02, 0x00}
		payload = append(payload, s.connectionHeader()...)
		payload = append(payload, obex.NameHeader(headerName, part)...)
		response, err := s.request(opSetPath, payload)
		if err != nil {
			s.currentPath = unknownPath
			return err
		}
		if response.Code != obex.ResponseOK {
			s.currentPath = unknownPath
			return fmt.Errorf("MAP SetPath rejected: 0x%02x", response.Code)
		}
	}
	s.currentPath = folder
	return nil
}

func (s *Session) resetPath(force bool) error {
	if !force && s.currentPath == "" {
		return nil
	}
	payload := []byte{0x02, 0x00}
	payload = append(payload, s.connectionHeader()...)
	response, err := s.request(opSetPath, payload)
	if err != nil {
		s.currentPath = unknownPath
		return err
	}
	if response.Code != obex.ResponseOK {
		s.currentPath = unknownPath
		return fmt.Errorf("MAP root SetPath rejected: 0x%02x", response.Code)
	}
	s.currentPath = ""
	return nil
}

func (s *Session) disconnect() error {
	response, err := s.request(opDisconnect, s.connectionHeader())
	if err != nil {
		return err
	}
	if response.Code != obex.ResponseOK {
		return fmt.Errorf("MAP disconnect rejected: 0x%02x", response.Code)
	}
	return nil
}

func (s *Session) connectionHeader() []byte {
	return obex.Uint32Header(headerConnectionID, s.connection)
}

func (s *Session) request(opcode byte, payload []byte) (obex.Packet, error) {
	if s.conn == nil {
		return obex.Packet{}, errors.New("MAP session is closed")
	}
	if len(payload)+3 > s.packetMax {
		return obex.Packet{}, errors.New("OBEX request exceeds negotiated packet size")
	}
	if err := obex.WritePacket(s.conn, opcode, payload); err != nil {
		return obex.Packet{}, err
	}
	return obex.ReadPacket(s.conn, 0xffff)
}

func parseListing(body []byte) ([]model.ListingMessage, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	var messages []model.ListingMessage
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("parse MAP listing: %w", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "msg" {
			continue
		}
		attrs := make(map[string]string, len(start.Attr))
		for _, attr := range start.Attr {
			attrs[attr.Name.Local] = attr.Value
		}
		handle := attrs["handle"]
		if !safeHandle(handle) {
			continue
		}
		messages = append(messages, model.ListingMessage{
			Handle: handle, Type: attrs["type"], DateTime: attrs["datetime"], Attrs: attrs,
		})
	}
	return messages, nil
}

func safeHandle(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

// IsTransportError distinguishes a dead or timed-out RFCOMM stream from an
// explicit MAP/OBEX rejection. Only transport failures require reconnecting
// the single MAS session.
func IsTransportError(err error) bool {
	if err == nil {
		return false
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	for _, target := range []error{
		io.EOF, io.ErrUnexpectedEOF, syscall.EPIPE, syscall.ECONNRESET,
		syscall.ECONNABORTED, syscall.ENOTCONN, syscall.ETIMEDOUT,
		syscall.EHOSTDOWN, syscall.EHOSTUNREACH, syscall.EAGAIN, syscall.EBADF,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
