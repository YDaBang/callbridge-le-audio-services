package media

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"callbridge.local/callbridge-sms-go/internal/model"
)

const SignalContentType = "application/x-acro-filetransfer+json"

type Descriptor struct {
	ContentType   string
	ContentURL    string
	ContentSize   int
	Filename      string
	EncryptionKey string
	Hash          string
}

type Client struct {
	base    *url.URL
	client  *http.Client
	maximum int
}

func New(rawURL string, maximum int, timeout time.Duration) (*Client, error) {
	base, err := url.Parse(rawURL)
	if err != nil || base.Scheme != "https" || base.Host != "mmmsg.acrobits.net" || base.Path != "/" || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("invalid Groundwire media service URL")
	}
	if maximum < 1 || maximum > 2<<20 || timeout < 10*time.Second || timeout > 120*time.Second {
		return nil, errors.New("invalid Groundwire media client bounds")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Client{base: base, client: client, maximum: maximum}, nil
}

func (c *Client) Upload(ctx context.Context, attachment model.Attachment) (Descriptor, error) {
	if len(attachment.Data) < 1 || len(attachment.Data) > c.maximum || !allowedContentType(attachment.ContentType) {
		return Descriptor{}, errors.New("invalid Groundwire media attachment")
	}
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return Descriptor{}, errors.New("create Groundwire media key")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return Descriptor{}, errors.New("initialize Groundwire media cipher")
	}
	encrypted := make([]byte, len(attachment.Data))
	cipher.NewCTR(block, make([]byte, aes.BlockSize)).XORKeyStream(encrypted, attachment.Data)

	contentURL, err := c.allocate(ctx, attachment.ContentType, len(encrypted))
	if err != nil {
		return Descriptor{}, err
	}
	if err := c.put(ctx, contentURL, attachment.ContentType, encrypted); err != nil {
		return Descriptor{}, err
	}
	return Descriptor{
		ContentType:   attachment.ContentType,
		ContentURL:    contentURL,
		ContentSize:   len(attachment.Data),
		Filename:      attachment.Filename,
		EncryptionKey: strings.ToUpper(hex.EncodeToString(key)),
		Hash:          strconv.FormatUint(uint64(crc32.ChecksumIEEE(attachment.Data)), 10),
	}, nil
}

func (c *Client) allocate(ctx context.Context, contentType string, size int) (string, error) {
	payload, err := json.Marshal(struct {
		ContentType string `json:"contentType"`
		ContentSize int    `json:"contentSize"`
	}{ContentType: contentType, ContentSize: size})
	if err != nil {
		return "", errors.New("encode Groundwire media allocation")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base.String(), bytes.NewReader(payload))
	if err != nil {
		return "", errors.New("create Groundwire media allocation request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return "", errors.New("Groundwire media allocation failed")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(data) > 4096 || response.StatusCode != http.StatusCreated {
		return "", errors.New("Groundwire media allocation rejected")
	}
	var result struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(data, &result); err != nil || !c.validContentURL(result.URL) {
		return "", errors.New("Groundwire media allocation response invalid")
	}
	return result.URL, nil
}

func (c *Client) put(ctx context.Context, contentURL, contentType string, encrypted []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, contentURL, bytes.NewReader(encrypted))
	if err != nil {
		return errors.New("create Groundwire media upload request")
	}
	request.Header.Set("Content-Type", contentType)
	response, err := c.client.Do(request)
	if err != nil {
		return errors.New("Groundwire media upload failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return errors.New("Groundwire media upload rejected")
	}
	return nil
}

func (c *Client) validContentURL(raw string) bool {
	value, err := url.Parse(raw)
	if err != nil || value.Scheme != "https" || value.Host != c.base.Host || value.User != nil || value.RawQuery != "" || value.Fragment != "" {
		return false
	}
	token := strings.TrimPrefix(value.EscapedPath(), "/")
	if token == value.EscapedPath() || len(token) < 16 || len(token) > 128 || strings.Contains(token, "/") {
		return false
	}
	for _, r := range token {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func MarshalSignal(body string, descriptor Descriptor) ([]byte, error) {
	type attachment struct {
		ContentType   string `json:"content-type"`
		ContentURL    string `json:"content-url"`
		ContentSize   int    `json:"content-size"`
		Filename      string `json:"filename,omitempty"`
		EncryptionKey string `json:"encryption-key"`
		Hash          string `json:"hash"`
	}
	payload := struct {
		Body        string       `json:"body,omitempty"`
		Attachments []attachment `json:"attachments"`
	}{
		Body: body,
		Attachments: []attachment{{
			ContentType: descriptor.ContentType, ContentURL: descriptor.ContentURL,
			ContentSize: descriptor.ContentSize, Filename: descriptor.Filename,
			EncryptionKey: descriptor.EncryptionKey, Hash: descriptor.Hash,
		}},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.New("encode Groundwire media signal")
	}
	return data, nil
}

func allowedContentType(value string) bool {
	switch value {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "image/heic", "image/heif":
		return true
	default:
		return false
	}
}
