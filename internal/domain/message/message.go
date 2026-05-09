package message

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/gophercast/gophercast/internal/domain/topic"
)

// Message represents data being published to a topic.
// Messages are immutable once created.
type Message struct {
	id          string
	topic       topic.Topic
	data        interface{}
	headers     map[string]string
	publishedAt time.Time
}

// NewMessage creates a new message for the given topic with the provided data.
// A unique ID and timestamp are automatically assigned.
func NewMessage(t topic.Topic, data interface{}) Message {
	return Message{
		id:          generateMessageID(),
		topic:       t,
		data:        data,
		headers:     make(map[string]string),
		publishedAt: time.Now(),
	}
}

// NewMessageWithHeaders creates a new message with the given headers in addition
// to a generated ID and timestamp. headers may be nil.
func NewMessageWithHeaders(t topic.Topic, data interface{}, headers map[string]string) Message {
	h := make(map[string]string, len(headers))
	for k, v := range headers {
		h[k] = v
	}
	return Message{
		id:          generateMessageID(),
		topic:       t,
		data:        data,
		headers:     h,
		publishedAt: time.Now(),
	}
}

// Reconstruct rebuilds a Message from its constituent parts.
// Intended for transport adapters that deserialise messages from the wire;
// application code should use NewMessage or NewMessageWithHeaders.
func Reconstruct(id string, t topic.Topic, data interface{}, headers map[string]string, publishedAt time.Time) Message {
	h := make(map[string]string, len(headers))
	for k, v := range headers {
		h[k] = v
	}
	return Message{
		id:          id,
		topic:       t,
		data:        data,
		headers:     h,
		publishedAt: publishedAt,
	}
}

// ID returns the unique message identifier.
func (m Message) ID() string {
	return m.id
}

// Topic returns the topic this message belongs to.
func (m Message) Topic() topic.Topic {
	return m.topic
}

// Data returns the message payload.
func (m Message) Data() interface{} {
	return m.data
}

// Headers returns a copy of the message headers.
func (m Message) Headers() map[string]string {
	out := make(map[string]string, len(m.headers))
	for k, v := range m.headers {
		out[k] = v
	}
	return out
}

// Header returns the value of a single header and whether it was present.
func (m Message) Header(key string) (string, bool) {
	v, ok := m.headers[key]
	return v, ok
}

// CorrelationID is a convenience accessor for the "correlation-id" header.
func (m Message) CorrelationID() string {
	return m.headers["correlation-id"]
}

// PublishedAt returns when the message was created.
func (m Message) PublishedAt() time.Time {
	return m.publishedAt
}

// String returns a human-readable representation of the message.
func (m Message) String() string {
	return fmt.Sprintf("Message[%s] on topic[%s] at %s",
		m.id, m.topic.String(), m.publishedAt.Format(time.RFC3339))
}

// generateMessageID creates a unique identifier for a message.
func generateMessageID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}
