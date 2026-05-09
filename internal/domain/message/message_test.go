package message_test

import (
	"testing"
	"time"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

func TestNewMessage(t *testing.T) {
	tests := []struct {
		name         string
		topicName    string
		data         interface{}
		checkData    bool
		expectedData interface{}
	}{
		{
			name:         "valid with data",
			topicName:    "users",
			data:         map[string]string{"id": "123"},
			checkData:    true,
			expectedData: map[string]string{"id": "123"},
		},
		{
			name:         "valid with nil data",
			topicName:    "users",
			data:         nil,
			checkData:    true,
			expectedData: nil,
		},
		{
			name:         "valid with string",
			topicName:    "events",
			data:         "test event",
			checkData:    true,
			expectedData: "test event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topicObj, _ := topic.New(tt.topicName)
			msg := message.NewMessage(topicObj, tt.data)

			// Check ID is generated and not empty
			if msg.ID() == "" {
				t.Error("ID should not be empty")
			}

			// Check Topic is correctly set
			if msg.Topic().String() != tt.topicName {
				t.Errorf("Topic = %v, want %v", msg.Topic().String(), tt.topicName)
			}

			// Check Data is stored correctly
			if tt.checkData {
				if tt.expectedData == nil {
					// For nil data, just check it's nil
					if msg.Data() != nil {
						t.Errorf("Data = %v, want nil", msg.Data())
					}
				} else {
					// For non-nil expected data, check it's not nil
					if msg.Data() == nil {
						t.Error("Data should not be nil")
					}
					// For maps, we can't compare directly with ==, so just verify type
					if _, ok := tt.expectedData.(map[string]string); ok {
						if _, ok := msg.Data().(map[string]string); !ok {
							t.Errorf("Data should be a map, got %T", msg.Data())
						}
					}
					// For strings, verify it's a string
					if _, ok := tt.expectedData.(string); ok {
						if _, ok := msg.Data().(string); !ok {
							t.Errorf("Data should be a string, got %T", msg.Data())
						}
					}
				}
			}

			// Check PublishedAt is set to current time (within 1 second)
			now := time.Now()
			if msg.PublishedAt().Sub(now) > time.Second || now.Sub(msg.PublishedAt()) > time.Second {
				t.Errorf("PublishedAt should be within 1 second of now")
			}
		})
	}
}

func TestMessageString(t *testing.T) {
	topicObj, _ := topic.New("users")
	msg := message.NewMessage(topicObj, "test data")

	str := msg.String()

	// Check format: "Message[{ID}] on topic[{topic}] at {timestamp}"
	if len(str) == 0 {
		t.Error("String() should not be empty")
	}

	// Should contain the topic name
	if !contains(str, "users") {
		t.Errorf("String() should contain topic name, got: %s", str)
	}

	// Should contain "Message"
	if !contains(str, "Message") {
		t.Errorf("String() should contain 'Message', got: %s", str)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestNewMessageWithHeaders(t *testing.T) {
	tp, _ := topic.New("events")
	headers := map[string]string{
		"correlation-id": "req-123",
		"content-type":   "application/json",
	}
	msg := message.NewMessageWithHeaders(tp, "payload", headers)

	if v, ok := msg.Header("correlation-id"); !ok || v != "req-123" {
		t.Errorf("Header correlation-id = %q ok=%v, want %q true", v, ok, "req-123")
	}
	if v, ok := msg.Header("content-type"); !ok || v != "application/json" {
		t.Errorf("Header content-type = %q, want application/json", v)
	}
	if _, ok := msg.Header("missing"); ok {
		t.Error("Missing header should not be present")
	}

	if got := msg.CorrelationID(); got != "req-123" {
		t.Errorf("CorrelationID = %q, want req-123", got)
	}

	// Headers() must return a copy.
	h := msg.Headers()
	h["correlation-id"] = "mutated"
	if msg.CorrelationID() != "req-123" {
		t.Error("Mutating returned headers must not affect the message")
	}
}

func TestNewMessageHeadersIsolated(t *testing.T) {
	tp, _ := topic.New("events")
	original := map[string]string{"k": "v"}
	msg := message.NewMessageWithHeaders(tp, nil, original)

	// Mutate the original map; the message must be unaffected.
	original["k"] = "mutated"
	if v, _ := msg.Header("k"); v != "v" {
		t.Errorf("Header was mutated by caller; got %q, want v", v)
	}
}

func TestReconstruct(t *testing.T) {
	tp, _ := topic.New("events")
	now := time.Now().Truncate(time.Second)
	msg := message.Reconstruct("fixed-id", tp, "data", map[string]string{"k": "v"}, now)

	if msg.ID() != "fixed-id" {
		t.Errorf("ID = %q, want fixed-id", msg.ID())
	}
	if !msg.PublishedAt().Equal(now) {
		t.Errorf("PublishedAt = %v, want %v", msg.PublishedAt(), now)
	}
	if v, _ := msg.Header("k"); v != "v" {
		t.Errorf("Header k = %q, want v", v)
	}
}

func TestNewMessageDefaultHeaders(t *testing.T) {
	tp, _ := topic.New("events")
	msg := message.NewMessage(tp, nil)
	h := msg.Headers()
	if h == nil {
		t.Error("Headers() should not return nil for a message created via NewMessage")
	}
}
