package message

import (
	"strings"
	"testing"

	"callbridge.local/callbridge-sms-go/internal/model"
)

func TestParseBMessage(t *testing.T) {
	raw := []byte("BEGIN:BMSG\r\nBEGIN:VCARD\r\nVERSION:3.0\r\nFN:테스트발신자\r\nTEL:+821000000000\r\nEND:VCARD\r\nBEGIN:MSG\r\n한글 테스트\r\nEND:MSG\r\nEND:BMSG\r\n")
	got := ParseBMessage(raw)
	if got.Body != "한글 테스트" || got.SenderName != "테스트발신자" || got.SenderNumber != "+821000000000" {
		t.Fatalf("unexpected parsed message: %#v", got)
	}
}

func TestStripMMSEnvelopeConservatively(t *testing.T) {
	body := "Date: Thu, 23 Jul 2026 10:27:19 +0900\nSubject: =?UTF-8?B?...?=\nFrom: sender\nTo: undisclosed\n\n딕동♫\n배송이 시작됩니다."
	if got := StripMMSEnvelope(body, "MMS"); got != "딕동♫\n배송이 시작됩니다." {
		t.Fatalf("envelope not stripped: %q", got)
	}
	ordinary := "Date: 오늘\n이것은 본문입니다."
	if got := StripMMSEnvelope(ordinary, "MMS"); got != ordinary {
		t.Fatalf("ordinary body was stripped: %q", got)
	}
	if got := StripMMSEnvelope(body, "SMS_GSM"); got != body {
		t.Fatal("SMS body must not be treated as an MMS envelope")
	}
}

func TestParseMMSExtractsTextAndImage(t *testing.T) {
	parsed := model.Message{Body: "MIME-Version: 1.0\r\nContent-Type: multipart/related; boundary=b\r\n\r\n" +
		"--b\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n사진 본문\r\n" +
		"--b\r\nContent-Type: image/jpeg; name=photo.jpg\r\nContent-Transfer-Encoding: base64\r\n\r\n/9j/2Q==\r\n" +
		"--b--\r\n"}
	got, err := ParseMMS(parsed, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if got.Body != "사진 본문" || len(got.Attachments) != 1 || got.Attachments[0].ContentType != "image/jpeg" || got.Attachments[0].Filename != "photo.jpg" {
		t.Fatalf("unexpected MMS parse: %#v", got)
	}
	if string(got.Attachments[0].Data) != string([]byte{0xff, 0xd8, 0xff, 0xd9}) {
		t.Fatalf("unexpected image bytes: %x", got.Attachments[0].Data)
	}
}

func TestVisiblePartsWireLimitAndTimestampOnlyOnFirst(t *testing.T) {
	body := strings.Repeat("한글 테스트 ", 160)
	parts := VisibleParts(body, "20260723T102719")
	if len(parts) < 2 || !strings.HasPrefix(parts[0], "[07/23 10:27 수신 1/") {
		t.Fatalf("unexpected first part: %q", parts[0])
	}
	var rebuilt strings.Builder
	for index, part := range parts {
		if len([]byte(part)) > MaxSIPPartBytes {
			t.Fatalf("part %d has %d bytes", index, len([]byte(part)))
		}
		lineEnd := strings.IndexByte(part, '\n')
		if lineEnd < 0 {
			t.Fatalf("part %d lacks prefix", index)
		}
		if index > 0 && strings.Contains(part[:lineEnd], "수신") {
			t.Fatalf("later part repeats time: %q", part[:lineEnd])
		}
		rebuilt.WriteString(part[lineEnd+1:])
	}
	if rebuilt.String() != body {
		t.Fatal("multipart split changed message text")
	}
}

func TestNormalizeNumbers(t *testing.T) {
	if got := NormalizeSender("010-0000-0000"); got != "+821000000000" {
		t.Fatalf("NormalizeSender()=%q", got)
	}
	if got, err := NormalizeDestination("010-0000-0000"); err != nil || got != "+821000000000" {
		t.Fatalf("NormalizeDestination()=%q,%v", got, err)
	}
	for _, unsafe := range []string{"12", "0101234@example", "010\n1234"} {
		if _, err := NormalizeDestination(unsafe); err == nil {
			t.Fatalf("accepted unsafe destination %q", unsafe)
		}
	}
}
