package message

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/http"
	"net/mail"
	"net/textproto"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"callbridge.local/callbridge-sms-go/internal/model"
)

const (
	MaxSIPPartBytes   = 600
	MaxSIPParts       = 16
	MaxMMSAttachments = 4
	maxMIMEParts      = 32
	maxMMSTextBytes   = 64 << 10
	maxMMSTotalBytes  = 2 << 20
)

var mmsEnvelopeHeaders = map[string]struct{}{
	"date": {}, "subject": {}, "from": {}, "to": {}, "cc": {},
	"reply-to": {}, "message-id": {}, "mime-version": {},
	"content-type": {}, "content-transfer-encoding": {},
}

func ParseBMessage(raw []byte) model.Message {
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	body := extractBlock(text, "BEGIN:MSG\n", "\nEND:MSG")
	if body == "" {
		body = strings.TrimSpace(text)
	} else {
		body = strings.TrimSpace(body)
	}
	var senderName, senderNumber string
	if vcard := extractBlock(text, "BEGIN:VCARD\n", "\nEND:VCARD"); vcard != "" {
		for _, line := range strings.Split(vcard, "\n") {
			if strings.HasPrefix(line, "FN:") && senderName == "" {
				senderName = strings.TrimSpace(strings.TrimPrefix(line, "FN:"))
			}
			upper := strings.ToUpper(line)
			if strings.HasPrefix(upper, "TEL:") || strings.HasPrefix(upper, "TEL;") {
				if index := strings.IndexByte(line, ':'); index >= 0 && senderNumber == "" {
					senderNumber = strings.TrimSpace(line[index+1:])
				}
			}
		}
	}
	return model.Message{Body: body, SenderName: senderName, SenderNumber: senderNumber}
}

func StripMMSEnvelope(body, messageType string) string {
	if !strings.EqualFold(strings.TrimSpace(messageType), "MMS") {
		return body
	}
	canonical := strings.ReplaceAll(body, "\r\n", "\n")
	canonical = strings.ReplaceAll(canonical, "\r", "\n")
	lines := strings.Split(canonical, "\n")
	blank := -1
	for index, line := range lines {
		if line == "" {
			blank = index
			break
		}
	}
	if blank < 2 || blank > 20 {
		return body
	}
	names := make(map[string]struct{})
	haveHeader := false
	for _, line := range lines[:blank] {
		if len(line) > 0 && unicode.IsSpace(rune(line[0])) && haveHeader {
			continue
		}
		name, _, ok := strings.Cut(line, ":")
		if !ok {
			return body
		}
		name = strings.ToLower(strings.TrimSpace(name))
		if _, known := mmsEnvelopeHeaders[name]; !known && !strings.HasPrefix(name, "x-") {
			return body
		}
		names[name] = struct{}{}
		haveHeader = true
	}
	core := 0
	for _, name := range []string{"date", "subject", "from", "to"} {
		if _, ok := names[name]; ok {
			core++
		}
	}
	if core < 2 {
		return body
	}
	payload := strings.TrimLeft(strings.Join(lines[blank+1:], "\n"), "\n")
	if strings.TrimSpace(payload) == "" {
		return body
	}
	return payload
}

// ParseMMS replaces a MAP MMS MIME envelope with its visible text and bounded
// image attachments. Attachment bytes remain in memory and are never written
// to the state file or JSON diagnostic endpoints.
func ParseMMS(parsed model.Message, maximumAttachmentBytes int) (model.Message, error) {
	if maximumAttachmentBytes < 1 || maximumAttachmentBytes > maxMMSTotalBytes {
		return model.Message{}, errors.New("MMS attachment bound invalid")
	}
	body := strings.TrimSpace(parsed.Body)
	if body == "" {
		return parsed, nil
	}
	mailMessage, err := mail.ReadMessage(strings.NewReader(body))
	if err != nil || strings.TrimSpace(mailMessage.Header.Get("Content-Type")) == "" {
		parsed.Body = StripMMSEnvelope(parsed.Body, "MMS")
		return parsed, nil
	}
	collector := mimeCollector{maximumAttachmentBytes: maximumAttachmentBytes}
	if err := collector.walk(textproto.MIMEHeader(mailMessage.Header), mailMessage.Body, 0); err != nil {
		return model.Message{}, err
	}
	parsed.Body = strings.TrimSpace(strings.Join(collector.text, "\n"))
	parsed.Attachments = collector.attachments
	return parsed, nil
}

type mimeCollector struct {
	maximumAttachmentBytes int
	parts                  int
	totalAttachmentBytes   int
	text                   []string
	attachments            []model.Attachment
}

func (c *mimeCollector) walk(header textproto.MIMEHeader, body io.Reader, depth int) error {
	if depth > 5 {
		return errors.New("MMS MIME nesting exceeds bound")
	}
	c.parts++
	if c.parts > maxMIMEParts {
		return errors.New("MMS MIME part count exceeds bound")
	}
	mediaType, params, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil {
		return errors.New("MMS MIME content type invalid")
	}
	mediaType = strings.ToLower(mediaType)
	if strings.HasPrefix(mediaType, "multipart/") {
		boundary := params["boundary"]
		if boundary == "" || len(boundary) > 200 {
			return errors.New("MMS MIME boundary invalid")
		}
		reader := multipart.NewReader(body, boundary)
		for {
			part, partErr := reader.NextPart()
			if errors.Is(partErr, io.EOF) {
				return nil
			}
			if partErr != nil {
				return errors.New("MMS MIME multipart invalid")
			}
			walkErr := c.walk(part.Header, part, depth+1)
			_ = part.Close()
			if walkErr != nil {
				return walkErr
			}
		}
	}
	decoded, err := transferDecoded(header.Get("Content-Transfer-Encoding"), body)
	if err != nil {
		return err
	}
	if mediaType == "text/plain" {
		data, err := readBounded(decoded, maxMMSTextBytes)
		if err != nil {
			return errors.New("MMS text exceeds bound")
		}
		if !utf8.Valid(data) {
			return errors.New("MMS text is not UTF-8")
		}
		if value := strings.TrimSpace(string(data)); value != "" {
			c.text = append(c.text, value)
		}
		return nil
	}
	if !strings.HasPrefix(mediaType, "image/") && mediaType != "application/octet-stream" {
		return nil
	}
	data, err := readBounded(decoded, c.maximumAttachmentBytes)
	if err != nil {
		return errors.New("MMS image exceeds bound")
	}
	if len(data) == 0 {
		return nil
	}
	detected, ok := supportedImageType(mediaType, data)
	if !ok {
		return nil
	}
	if len(c.attachments) >= MaxMMSAttachments {
		return errors.New("MMS image count exceeds bound")
	}
	c.totalAttachmentBytes += len(data)
	if c.totalAttachmentBytes > maxMMSTotalBytes {
		return errors.New("MMS image total exceeds bound")
	}
	filename := safeFilename(params["name"])
	if _, dispositionParams, parseErr := mime.ParseMediaType(header.Get("Content-Disposition")); parseErr == nil {
		if value := safeFilename(dispositionParams["filename"]); value != "" {
			filename = value
		}
	}
	c.attachments = append(c.attachments, model.Attachment{
		ContentType: detected,
		Filename:    filename,
		Data:        data,
	})
	return nil
}

func transferDecoded(encoding string, body io.Reader) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "7bit", "8bit", "binary":
		return body, nil
	case "base64":
		return base64.NewDecoder(base64.StdEncoding, body), nil
	case "quoted-printable":
		return quotedprintable.NewReader(body), nil
	default:
		return nil, fmt.Errorf("unsupported MMS transfer encoding")
	}
}

func readBounded(reader io.Reader, maximum int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maximum {
		return nil, errors.New("decoded MIME part exceeds bound")
	}
	return data, nil
}

func supportedImageType(declared string, data []byte) (string, bool) {
	detected := strings.ToLower(strings.TrimSpace(strings.SplitN(http.DetectContentType(data), ";", 2)[0]))
	if detected == "application/octet-stream" && isHEIF(data) {
		detected = "image/heic"
	}
	allowed := map[string]bool{
		"image/jpeg": true, "image/png": true, "image/gif": true,
		"image/webp": true, "image/heic": true, "image/heif": true,
	}
	if !allowed[detected] {
		return "", false
	}
	declared = strings.ToLower(strings.TrimSpace(declared))
	if declared == "image/jpg" {
		declared = "image/jpeg"
	}
	if declared != "" && declared != "application/octet-stream" && declared != detected {
		if !(declared == "image/heif" && detected == "image/heic") {
			return "", false
		}
	}
	return detected, true
}

func isHEIF(data []byte) bool {
	if len(data) < 12 || string(data[4:8]) != "ftyp" {
		return false
	}
	brand := string(data[8:12])
	switch brand {
	case "heic", "heix", "hevc", "hevx", "heim", "heis", "mif1", "msf1":
		return true
	default:
		return false
	}
}

func safeFilename(value string) string {
	value = strings.TrimSpace(filepath.Base(value))
	if value == "." || value == "" || len(value) > 120 || strings.ContainsAny(value, "\x00\r\n") {
		return ""
	}
	return value
}

func SplitUTF8(text string, maximum int) []string {
	if maximum < utf8.UTFMax {
		maximum = utf8.UTFMax
	}
	if len([]byte(text)) <= maximum || text == "" {
		return []string{text}
	}
	runes := []rune(text)
	parts := make([]string, 0, len(text)/maximum+1)
	for start := 0; start < len(runes); {
		size := 0
		end := start
		boundary := -1
		for end < len(runes) {
			width := utf8.RuneLen(runes[end])
			if size+width > maximum {
				break
			}
			size += width
			end++
			if unicode.IsSpace(runes[end-1]) {
				boundary = end
			}
		}
		if end < len(runes) && boundary > start {
			end = boundary
		}
		if end == start {
			end++
		}
		parts = append(parts, string(runes[start:end]))
		start = end
	}
	return parts
}

func VisibleParts(body, mapDateTime string) []string {
	received, hasTime := ReceivedTime(mapDateTime)
	singlePrefix := ""
	if hasTime {
		singlePrefix = "[" + received.Format("01/02 15:04") + " 수신]\n"
	}
	if len([]byte(singlePrefix+body)) <= MaxSIPPartBytes {
		return []string{singlePrefix + body}
	}
	total := 2
	for attempts := 0; attempts < 8; attempts++ {
		chunks := splitWithPrefixes(body, received, hasTime, total)
		if len(chunks) == total {
			parts := make([]string, 0, total)
			for index, chunk := range chunks {
				parts = append(parts, multipartPrefix(received, hasTime, index, total)+"\n"+chunk)
			}
			return parts
		}
		total = len(chunks)
	}
	// The only changing value is the decimal width of total, so convergence is
	// bounded tightly. This fallback retains the hard 600-byte wire limit.
	chunks := SplitUTF8(body, MaxSIPPartBytes-64)
	parts := make([]string, 0, len(chunks))
	for index, chunk := range chunks {
		parts = append(parts, multipartPrefix(received, hasTime, index, len(chunks))+"\n"+chunk)
	}
	return parts
}

func splitWithPrefixes(body string, received time.Time, hasTime bool, total int) []string {
	remaining := body
	var chunks []string
	for index := 0; remaining != ""; index++ {
		prefixBytes := len([]byte(multipartPrefix(received, hasTime, index, total) + "\n"))
		chunk := SplitUTF8(remaining, MaxSIPPartBytes-prefixBytes)[0]
		chunks = append(chunks, chunk)
		remaining = remaining[len(chunk):]
	}
	return chunks
}

func multipartPrefix(received time.Time, hasTime bool, index, total int) string {
	if index > 0 {
		return "[" + itoa(index+1) + "/" + itoa(total) + "]"
	}
	if hasTime {
		return "[" + received.Format("01/02 15:04") + " 수신 1/" + itoa(total) + "]"
	}
	return "[MMS 1/" + itoa(total) + "]"
}

func ReceivedTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	kst := time.FixedZone("KST", 9*60*60)
	if len(raw) >= 15 {
		if parsed, err := time.ParseInLocation("20060102T150405", raw[:15], kst); err == nil {
			return parsed, true
		}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.In(kst), true
	}
	return time.Time{}, false
}

func NormalizeSender(value string) string {
	raw := strings.TrimSpace(value)
	var digits strings.Builder
	for _, r := range raw {
		if unicode.IsDigit(r) && r <= unicode.MaxASCII {
			digits.WriteRune(r)
		}
	}
	value = digits.String()
	if len(value) < 3 {
		return "000"
	}
	if strings.HasPrefix(raw, "+") {
		return "+" + value
	}
	if strings.HasPrefix(value, "010") {
		return "+82" + value[1:]
	}
	if strings.HasPrefix(value, "82") {
		return "+" + value
	}
	return value
}

func NormalizeDestination(value string) (string, error) {
	value = strings.TrimSpace(value)
	var result strings.Builder
	for index, r := range value {
		switch {
		case r >= '0' && r <= '9':
			result.WriteRune(r)
		case r == '+' && index == 0:
			result.WriteRune(r)
		case r == '-' || r == '(' || r == ')' || r == '.' || r == ' ':
		default:
			return "", errors.New("destination contains unsupported characters")
		}
	}
	normalized := result.String()
	digits := strings.TrimPrefix(normalized, "+")
	if len(digits) < 3 || len(digits) > 20 {
		return "", errors.New("destination length invalid")
	}
	if strings.HasPrefix(digits, "010") && !strings.HasPrefix(normalized, "+") {
		return "+82" + digits[1:], nil
	}
	return normalized, nil
}

func extractBlock(text, startMarker, endMarker string) string {
	start := strings.Index(text, startMarker)
	if start < 0 {
		return ""
	}
	start += len(startMarker)
	end := strings.Index(text[start:], endMarker)
	if end < 0 {
		return ""
	}
	return text[start : start+end]
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	index := len(buffer)
	for value > 0 {
		index--
		buffer[index] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[index:])
}
