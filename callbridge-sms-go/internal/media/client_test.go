package media

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"callbridge.local/callbridge-sms-go/internal/model"
)

func TestUploadEncryptsAndBuildsGroundwireSignal(t *testing.T) {
	original := []byte("private-image-bytes")
	var uploaded []byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			var allocation struct {
				ContentType string `json:"contentType"`
				ContentSize int    `json:"contentSize"`
			}
			if err := json.NewDecoder(request.Body).Decode(&allocation); err != nil || allocation.ContentType != "image/jpeg" || allocation.ContentSize != len(original) {
				t.Fatalf("bad allocation: %#v err=%v", allocation, err)
			}
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(writer, `{"url":"`+serverURL(request)+`/ABCDEFGHIJKLMNOPQRSTUVWX12345678"}`)
		case http.MethodPut:
			if request.URL.Path != "/ABCDEFGHIJKLMNOPQRSTUVWX12345678" || request.Header.Get("Content-Type") != "image/jpeg" {
				t.Fatalf("bad upload request path=%q type=%q", request.URL.Path, request.Header.Get("Content-Type"))
			}
			uploaded, _ = io.ReadAll(request.Body)
			writer.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()
	base, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{base: base, client: server.Client(), maximum: 1024}
	descriptor, err := client.Upload(context.Background(), model.Attachment{
		ContentType: "image/jpeg", Filename: "photo.jpg", Data: original,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := hex.DecodeString(descriptor.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	decrypted := make([]byte, len(uploaded))
	cipher.NewCTR(block, make([]byte, aes.BlockSize)).XORKeyStream(decrypted, uploaded)
	if string(decrypted) != string(original) {
		t.Fatal("uploaded media did not decrypt to original")
	}
	if descriptor.Hash != strconv.FormatUint(uint64(crc32.ChecksumIEEE(original)), 10) {
		t.Fatalf("hash=%q", descriptor.Hash)
	}
	signal, err := MarshalSignal("caption", descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(signal), `"content-type":"image/jpeg"`) || !strings.Contains(string(signal), `"body":"caption"`) || strings.Contains(string(signal), "private-image-bytes") {
		t.Fatalf("bad signal: %s", signal)
	}
}

func TestContentURLIsPinnedToAllocationHost(t *testing.T) {
	base, _ := url.Parse("https://mmmsg.acrobits.net/")
	client := &Client{base: base}
	for _, value := range []string{
		"http://mmmsg.acrobits.net/ABCDEFGHIJKLMNOP",
		"https://evil.example/ABCDEFGHIJKLMNOP",
		"https://mmmsg.acrobits.net/a/b",
		"https://mmmsg.acrobits.net/short",
	} {
		if client.validContentURL(value) {
			t.Fatalf("accepted unsafe URL %q", value)
		}
	}
	if !client.validContentURL("https://mmmsg.acrobits.net/ABCDEFGHIJKLMNOP") {
		t.Fatal("rejected valid pinned URL")
	}
}

func serverURL(request *http.Request) string {
	return "https://" + request.Host
}
