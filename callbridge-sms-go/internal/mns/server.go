package mns

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"

	"callbridge.local/callbridge-sms-go/internal/bt"
	"callbridge.local/callbridge-sms-go/internal/model"
	"callbridge.local/callbridge-sms-go/internal/obex"
)

const (
	bluezBusName     = "org.bluez"
	managerPath      = dbus.ObjectPath("/org/bluez")
	profilePath      = dbus.ObjectPath("/callbridge/map/mns/profile")
	profileInterface = "org.bluez.Profile1"
	managerInterface = "org.bluez.ProfileManager1"
	serviceUUID      = "00001133-0000-1000-8000-00805f9b34fb"

	opConnect    = 0x80
	opDisconnect = 0x81
	opPut        = 0x02
	opPutFinal   = 0x82

	headerType         = 0x42
	headerTarget       = 0x46
	headerBody         = 0x48
	headerEndOfBody    = 0x49
	headerWho          = 0x4a
	headerConnectionID = 0xcb

	maximumEventBody = 1 << 20
)

var targetUUID = []byte{0xbb, 0x58, 0x2b, 0x41, 0x42, 0x0c, 0x11, 0xdb, 0xb0, 0xde, 0x08, 0x00, 0x20, 0x0c, 0x9a, 0x66}

type Callbacks struct {
	Registered func(bool)
	Connected  func(bool)
	Dropped    func()
}

type Server struct {
	device   string
	adapter  string
	channel  uint16
	events   chan<- model.Event
	logger   *log.Logger
	callback Callbacks
}

func New(device, adapter string, channel uint16, events chan<- model.Event, logger *log.Logger, callbacks Callbacks) (*Server, error) {
	if device == "" || adapter == "" || channel < 1 || channel > 30 || events == nil || logger == nil {
		return nil, errors.New("invalid MNS server configuration")
	}
	return &Server{device: device, adapter: adapter, channel: channel, events: events, logger: logger, callback: callbacks}, nil
}

func (s *Server) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := s.runOnce(ctx); err != nil && ctx.Err() == nil {
			s.logger.Printf("mns registration unavailable reason=%s", safeReason(err))
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Server) runOnce(ctx context.Context) error {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return fmt.Errorf("connect system bus: %w", err)
	}
	defer conn.Close()
	if !conn.SupportsUnixFDs() {
		return errors.New("system bus lacks Unix FD passing")
	}
	release := make(chan struct{}, 1)
	profile := &profile{
		server: s, context: ctx, release: release, connections: make(map[*bt.Conn]struct{}),
	}
	if err := conn.ExportAll(profile, profilePath, profileInterface); err != nil {
		return fmt.Errorf("export MNS profile: %w", err)
	}
	manager := conn.Object(bluezBusName, managerPath)
	options := map[string]dbus.Variant{
		"Name":                  dbus.MakeVariant("Message Notification Service"),
		"Role":                  dbus.MakeVariant("server"),
		"Channel":               dbus.MakeVariant(s.channel),
		"ServiceRecord":         dbus.MakeVariant(serviceRecord(s.channel)),
		"RequireAuthentication": dbus.MakeVariant(false),
		"RequireAuthorization":  dbus.MakeVariant(false),
		"AutoConnect":           dbus.MakeVariant(true),
	}
	if call := manager.Call(managerInterface+".RegisterProfile", 0, profilePath, serviceUUID, options); call.Err != nil {
		return fmt.Errorf("register MNS profile: %w", call.Err)
	}
	if s.callback.Registered != nil {
		s.callback.Registered(true)
	}
	s.logger.Printf("mns profile registered channel=%d", s.channel)
	defer func() {
		if s.callback.Registered != nil {
			s.callback.Registered(false)
		}
		profile.closeAll()
		_ = manager.Call(managerInterface+".UnregisterProfile", 0, profilePath).Err
	}()
	select {
	case <-ctx.Done():
		return nil
	case <-conn.Context().Done():
		return errors.New("system bus disconnected")
	case <-release:
		return errors.New("MNS profile released")
	}
}

type profile struct {
	server      *Server
	context     context.Context
	release     chan<- struct{}
	mu          sync.Mutex
	connections map[*bt.Conn]struct{}
}

func (p *profile) Release() {
	select {
	case p.release <- struct{}{}:
	default:
	}
}

func (p *profile) NewConnection(device dbus.ObjectPath, fd dbus.UnixFD, _ map[string]dbus.Variant) {
	rawFD := int(fd)
	if !p.expectedDevice(device) {
		conn, err := bt.FromFD(rawFD, 2*time.Second)
		if err == nil {
			_ = conn.Close()
		}
		p.server.logger.Printf("mns rejected unexpected paired device")
		return
	}
	conn, err := bt.FromFD(rawFD, 30*time.Second)
	if err != nil {
		p.server.logger.Printf("mns connection setup failed reason=%s", safeReason(err))
		return
	}
	p.mu.Lock()
	p.connections[conn] = struct{}{}
	p.mu.Unlock()
	if p.server.callback.Connected != nil {
		p.server.callback.Connected(true)
	}
	go func() {
		defer func() {
			_ = conn.Close()
			p.mu.Lock()
			delete(p.connections, conn)
			remaining := len(p.connections)
			p.mu.Unlock()
			if remaining == 0 && p.server.callback.Connected != nil {
				p.server.callback.Connected(false)
			}
		}()
		p.handle(conn)
	}()
}

func (p *profile) RequestDisconnection(_ dbus.ObjectPath) { p.closeAll() }
func (p *profile) Cancel(_ dbus.ObjectPath, _ string)     {}

func (p *profile) expectedDevice(path dbus.ObjectPath) bool {
	want := "/org/bluez/" + p.server.adapter + "/dev_" + strings.ReplaceAll(strings.ToUpper(p.server.device), ":", "_")
	return string(path) == want
}

func (p *profile) closeAll() {
	p.mu.Lock()
	connections := make([]*bt.Conn, 0, len(p.connections))
	for conn := range p.connections {
		connections = append(connections, conn)
	}
	p.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (p *profile) handle(conn *bt.Conn) {
	var accumulated []byte
	contentType := ""
	connected := false
	for p.context.Err() == nil {
		packet, err := obex.ReadPacket(conn, 0xffff)
		if err != nil {
			return
		}
		switch packet.Code {
		case opConnect:
			if len(packet.Payload) < 4 {
				_ = obex.WritePacket(conn, obex.ResponseBadRequest, nil)
				return
			}
			headers, parseErr := obex.ParseHeaders(packet.Payload, 4)
			target, ok := obex.Find(headers, headerTarget)
			if parseErr != nil || !ok || !bytes.Equal(target, targetUUID) {
				_ = obex.WritePacket(conn, obex.ResponseBadRequest, nil)
				return
			}
			payload := []byte{0x10, 0x00, 0xff, 0xff}
			payload = append(payload, obex.Uint32Header(headerConnectionID, 1)...)
			payload = append(payload, obex.MustHeader(headerWho, targetUUID)...)
			if obex.WritePacket(conn, obex.ResponseOK, payload) != nil {
				return
			}
			connected = true
			accumulated = accumulated[:0]
			contentType = ""
		case opPut, opPutFinal:
			if !connected {
				_ = obex.WritePacket(conn, obex.ResponseBadRequest, nil)
				return
			}
			headers, parseErr := obex.ParseHeaders(packet.Payload, 0)
			if parseErr != nil {
				_ = obex.WritePacket(conn, obex.ResponseBadRequest, nil)
				return
			}
			if rawType, ok := obex.Find(headers, headerType); ok {
				contentType = strings.TrimRight(string(rawType), "\x00")
			}
			accumulated = append(accumulated, obex.CollectBody(headers, headerBody, headerEndOfBody)...)
			if len(accumulated) > maximumEventBody {
				_ = obex.WritePacket(conn, obex.ResponseBadRequest, nil)
				return
			}
			responseCode := byte(obex.ResponseContinue)
			if packet.Code == opPutFinal {
				responseCode = obex.ResponseOK
			}
			if obex.WritePacket(conn, responseCode, nil) != nil {
				return
			}
			if packet.Code == opPutFinal && len(accumulated) > 0 {
				if contentType == "x-bt/MAP-event-report" || contentType == "" {
					if event, parseErr := parseEvent(accumulated); parseErr == nil {
						if event.EventType == "NewMessage" && event.Handle != "" {
							select {
							case p.server.events <- event:
							default:
								if p.server.callback.Dropped != nil {
									p.server.callback.Dropped()
								}
								p.server.logger.Printf("mns event queue full ref=%s", reference(event.Handle))
							}
						}
					} else {
						p.server.logger.Printf("mns event parse rejected reason=%s", safeReason(parseErr))
					}
				}
				accumulated = accumulated[:0]
				contentType = ""
			}
		case opDisconnect:
			_ = obex.WritePacket(conn, obex.ResponseOK, nil)
			return
		default:
			if obex.WritePacket(conn, obex.ResponseNotImplemented, nil) != nil {
				return
			}
		}
	}
}

func parseEvent(raw []byte) (model.Event, error) {
	start := bytes.IndexByte(raw, '<')
	if start < 0 {
		return model.Event{}, errors.New("event XML missing")
	}
	decoder := xml.NewDecoder(bytes.NewReader(raw[start:]))
	for {
		token, err := decoder.Token()
		if err != nil {
			return model.Event{}, err
		}
		element, ok := token.(xml.StartElement)
		if !ok || element.Name.Local != "event" {
			continue
		}
		event := model.Event{Source: "mns", DateTime: time.Now().UTC().Format(time.RFC3339)}
		for _, attr := range element.Attr {
			switch attr.Name.Local {
			case "type":
				event.EventType = attr.Value
			case "handle":
				event.Handle = attr.Value
			case "folder":
				event.Folder = attr.Value
			case "msg_type":
				event.MessageType = attr.Value
			case "datetime":
				if strings.TrimSpace(attr.Value) != "" {
					event.DateTime = attr.Value
				}
			}
		}
		if !safeHandle(event.Handle) {
			return model.Event{}, errors.New("invalid event handle")
		}
		return event, nil
	}
}

func serviceRecord(channel uint16) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" ?>
<record>
  <attribute id="0x0001"><sequence><uuid value="0x1133"/></sequence></attribute>
  <attribute id="0x0004"><sequence>
    <sequence><uuid value="0x0100"/></sequence>
    <sequence><uuid value="0x0003"/><uint8 value="0x%02x"/></sequence>
    <sequence><uuid value="0x0008"/></sequence>
  </sequence></attribute>
  <attribute id="0x0005"><sequence><uuid value="0x1002"/></sequence></attribute>
  <attribute id="0x0009"><sequence><sequence><uuid value="0x1134"/><uint16 value="0x0104"/></sequence></sequence></attribute>
  <attribute id="0x0100"><text value="Message Notification Service"/></attribute>
  <attribute id="0x0101"><text value="MAP Message Notification Server"/></attribute>
  <attribute id="0x0102"><text value="Callbridge"/></attribute>
</record>`, channel)
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

func reference(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:6])
}

func safeReason(err error) string {
	if err == nil {
		return "none"
	}
	return fmt.Sprintf("%T", err)
}
