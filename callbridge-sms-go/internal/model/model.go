package model

import "time"

type Event struct {
	EventType   string `json:"event_type"`
	Handle      string `json:"handle"`
	Folder      string `json:"folder"`
	MessageType string `json:"msg_type"`
	DateTime    string `json:"datetime"`
	Source      string `json:"source,omitempty"`
}

type Message struct {
	Body         string       `json:"body"`
	SenderName   string       `json:"sender_name,omitempty"`
	SenderNumber string       `json:"sender_number,omitempty"`
	Attachments  []Attachment `json:"-"`
}

type Attachment struct {
	ContentType string
	Filename    string
	Data        []byte
}

type ListingMessage struct {
	Handle   string            `json:"handle"`
	Type     string            `json:"type,omitempty"`
	DateTime string            `json:"datetime,omitempty"`
	Attrs    map[string]string `json:"attrs,omitempty"`
}

type Outbound struct {
	ID       string    `json:"id"`
	To       string    `json:"to"`
	Text     string    `json:"text"`
	QueuedAt time.Time `json:"queued_at"`
}
