package ami

import (
	"bufio"
	"context"
	"encoding/base64"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSendMessage(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	result := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("Asterisk Call Manager/9.0\r\n"))
		reader := bufio.NewReader(conn)
		login, err := readBlock(reader)
		if err != nil || !strings.Contains(login, "Username: callbridge") || !strings.Contains(login, "Secret: test-secret") {
			result <- errorsText("bad login")
			return
		}
		_, _ = conn.Write([]byte("Response: Success\r\n\r\n"))
		action, err := readBlock(reader)
		encoded := base64.StdEncoding.EncodeToString([]byte("한글🙂"))
		if err != nil || strings.Contains(action, "한글") || !strings.Contains(action, "Base64Body: "+encoded) || !strings.Contains(action, "Variable: Content-Type=text/plain") || !strings.Contains(action, "ActionID: action-1") {
			result <- errorsText("bad action")
			return
		}
		_, _ = conn.Write([]byte("Response: Success\r\nMessage: queued\r\n\r\n"))
		result <- nil
	}()
	secret := filepath.Join(t.TempDir(), "ami-secret")
	if err := os.WriteFile(secret, []byte("test-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := New(listener.Addr().String(), "callbridge", secret)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SendMessage(context.Background(), "1002", "+821000000000", "한글🙂", "text/plain", "action-1"); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestSecretRequiresPrivateMode(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "ami-secret")
	if err := os.WriteFile(secret, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(secret, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New("127.0.0.1:5038", "user", secret); err == nil {
		t.Fatal("accepted insecure secret mode")
	}
}

func readBlock(reader *bufio.Reader) (string, error) {
	var result strings.Builder
	for {
		line, err := reader.ReadString('\n')
		result.WriteString(line)
		if line == "\r\n" || line == "\n" {
			return result.String(), err
		}
		if err != nil {
			return result.String(), err
		}
	}
}

type errorsText string

func (e errorsText) Error() string { return string(e) }
