package ami

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

const (
	maximumPacket = 128 << 10
	MaxBodyBytes  = 1200
)

type Client struct {
	address string
	user    string
	secret  string
}

func New(address, user, secretFile string) (*Client, error) {
	info, err := os.Stat(secretFile)
	if err != nil {
		return nil, fmt.Errorf("read AMI secret metadata: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() < 1 || info.Size() > 4096 {
		return nil, errors.New("AMI secret file permissions or size invalid")
	}
	raw, err := os.ReadFile(secretFile)
	if err != nil {
		return nil, fmt.Errorf("read AMI secret: %w", err)
	}
	secret := strings.TrimSpace(string(raw))
	if address == "" || user == "" || secret == "" || strings.ContainsAny(user+secret, "\x00\r\n") {
		return nil, errors.New("invalid AMI client configuration")
	}
	return &Client{address: address, user: user, secret: secret}, nil
}

func (c *Client) SendMessage(ctx context.Context, recipient, sender, body, contentType, actionID string) error {
	if !digits(recipient) || len(recipient) > 32 || body == "" || len(body) > MaxBodyBytes {
		return errors.New("invalid AMI MessageSend input")
	}
	if contentType != "text/plain" && contentType != "application/x-acro-filetransfer+json" {
		return errors.New("invalid AMI message content type")
	}
	for _, value := range []string{sender, actionID} {
		if value == "" || strings.ContainsAny(value, "\x00\r\n") {
			return errors.New("invalid AMI header value")
		}
	}
	dialer := net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}
	conn, err := dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return errors.New("AMI connection failed")
	}
	defer conn.Close()
	deadline := time.Now().Add(8 * time.Second)
	_ = conn.SetDeadline(deadline)
	reader := bufio.NewReaderSize(conn, 4096)
	login := "Action: Login\r\n" +
		"Username: " + c.user + "\r\n" +
		"Secret: " + c.secret + "\r\n" +
		"Events: off\r\n\r\n"
	if _, err := io.WriteString(conn, login); err != nil {
		return errors.New("AMI login write failed")
	}
	fields, err := readResponse(reader)
	if err != nil || !strings.EqualFold(fields["response"], "success") {
		return errors.New("AMI login rejected")
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(body))
	action := "Action: MessageSend\r\n" +
		"To: pjsip:" + recipient + "\r\n" +
		"From: sip:" + sender + "@cellular.invalid\r\n" +
		"Base64Body: " + encoded + "\r\n" +
		"Variable: Content-Type=" + contentType + "\r\n" +
		"ActionID: " + actionID + "\r\n\r\n"
	if _, err := io.WriteString(conn, action); err != nil {
		return errors.New("AMI action write failed")
	}
	fields, err = readResponse(reader)
	if err != nil {
		return errors.New("AMI action response failed")
	}
	if !strings.EqualFold(fields["response"], "success") {
		return errors.New("AMI MessageSend rejected")
	}
	_, _ = io.WriteString(conn, "Action: Logoff\r\n\r\n")
	return nil
}

func readResponse(reader *bufio.Reader) (map[string]string, error) {
	fields := make(map[string]string)
	total := 0
	for {
		line, err := reader.ReadString('\n')
		total += len(line)
		if total > maximumPacket {
			return nil, errors.New("AMI response exceeds bound")
		}
		line = strings.TrimRight(line, "\r\n")
		if name, value, ok := strings.Cut(line, ":"); ok {
			fields[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
		}
		if line == "" && fields["response"] != "" {
			return fields, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func digits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
