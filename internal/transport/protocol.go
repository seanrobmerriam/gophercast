// Package transport implements a length-prefixed JSON wire protocol for
// gophercast over TCP. All frames are encoded as:
//
//	[4-byte big-endian uint32 payload length][JSON payload]
package transport

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// FrameType discriminates request and response frames on the wire.
type FrameType string

const (
	// Client → server
	FramePublish          FrameType = "PUBLISH"
	FrameSubscribe        FrameType = "SUBSCRIBE"
	FrameSubscribePattern FrameType = "SUBSCRIBE_PATTERN"
	FrameUnsubscribe      FrameType = "UNSUBSCRIBE"

	// Server → client
	FrameAck     FrameType = "ACK"
	FrameError   FrameType = "ERROR"
	FrameMessage FrameType = "MESSAGE"
)

// Frame is the single wire type for every message exchanged between client
// and server. Fields are omitted from JSON when zero/nil to keep frames small.
type Frame struct {
	Type FrameType `json:"type"`

	// ReqID correlates a client request with its server ACK/ERROR response.
	ReqID string `json:"req_id,omitempty"`

	// PUBLISH fields (client → server)
	Topic   string            `json:"topic,omitempty"`
	Data    json.RawMessage   `json:"data,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`

	// SUBSCRIBE / SUBSCRIBE_PATTERN / UNSUBSCRIBE fields
	Pattern        string `json:"pattern,omitempty"`
	SubscriptionID string `json:"subscription_id,omitempty"`

	// ACK fields (server → client, reply to PUBLISH)
	Delivered int `json:"delivered,omitempty"`
	Dropped   int `json:"dropped,omitempty"`

	// ACK fields (server → client, reply to SUBSCRIBE*)
	// Reuses SubscriptionID to carry the assigned subscription ID back.

	// ERROR field
	Error string `json:"error,omitempty"`

	// MESSAGE fields (server → client, fan-out delivery)
	MsgID       string `json:"msg_id,omitempty"`
	PublishedAt int64  `json:"published_at,omitempty"` // Unix nanoseconds
}

// writeFrame encodes f as JSON and writes it with a 4-byte length prefix to w.
func writeFrame(w io.Writer, f Frame) error {
	payload, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("transport: marshal frame: %w", err)
	}
	if len(payload) > maxFrameBytes {
		return fmt.Errorf("transport: frame too large (%d bytes)", len(payload))
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("transport: write header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("transport: write payload: %w", err)
	}
	return nil
}

// readFrame reads a length-prefixed JSON frame from r.
func readFrame(r io.Reader) (Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, fmt.Errorf("transport: read header: %w", err)
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size > maxFrameBytes {
		return Frame{}, fmt.Errorf("transport: frame too large (%d bytes)", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, fmt.Errorf("transport: read payload: %w", err)
	}
	var f Frame
	if err := json.Unmarshal(buf, &f); err != nil {
		return Frame{}, fmt.Errorf("transport: unmarshal frame: %w", err)
	}
	return f, nil
}

// maxFrameBytes caps individual frame size at 4 MiB.
const maxFrameBytes = 4 << 20
