package subscription_test

import (
	"testing"
	"time"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/subscription"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

func TestNewSubscription(t *testing.T) {
	topicObj, _ := topic.New("users")
	sub := subscription.NewSubscription(topicObj)

	// Check ID is generated and unique
	if sub.ID() == "" {
		t.Error("ID should not be empty")
	}

	// Check Topic is correctly set
	if sub.Topic().String() != "users" {
		t.Errorf("Topic = %v, want %v", sub.Topic().String(), "users")
	}

	// Check MessageChannel is initialized and not nil
	if sub.MessageChannel() == nil {
		t.Error("MessageChannel should not be nil")
	}

	// Check CreatedAt is set
	if sub.CreatedAt().IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	// Check CreatedAt is recent
	now := time.Now()
	if sub.CreatedAt().Sub(now) > time.Second || now.Sub(sub.CreatedAt()) > time.Second {
		t.Error("CreatedAt should be within 1 second of now")
	}
}

func TestSubscriptionSendMessage(t *testing.T) {
	topicObj, _ := topic.New("users")
	sub := subscription.NewSubscription(topicObj)

	msg := message.NewMessage(topicObj, "test data")

	// Send message successfully
	sub.SendMessage(msg)

	// Check if message was received on channel
	select {
	case received := <-sub.MessageChannel():
		if received.Data() != "test data" {
			t.Errorf("Received data = %v, want %v", received.Data(), "test data")
		}
	case <-time.After(time.Second):
		t.Error("Did not receive message within 1 second")
	}
}

func TestSubscriptionClose(t *testing.T) {
	topicObj, _ := topic.New("users")
	sub := subscription.NewSubscription(topicObj)

	// Close the subscription
	sub.Close()

	// Check that channel is closed
	select {
	case _, ok := <-sub.MessageChannel():
		if ok {
			t.Error("Channel should be closed")
		}
	default:
		// Channel was buffered and not read, but Close() should have closed it
	}
}

func TestSubscriptionDroppedCount(t *testing.T) {
	topicObj, _ := topic.New("users")
	sub := subscription.NewSubscription(topicObj)
	defer sub.Close()

	msg := message.NewMessage(topicObj, "x")

	// Fill the buffer (capacity 200) plus extras that should be dropped.
	const total = 250
	for i := 0; i < total; i++ {
		sub.SendMessage(msg)
	}

	got := sub.DroppedCount()
	if got != total-200 {
		t.Errorf("DroppedCount=%d, want %d", got, total-200)
	}
}

func TestSubscriptionSendAfterClose(t *testing.T) {
	topicObj, _ := topic.New("users")
	sub := subscription.NewSubscription(topicObj)
	sub.Close()

	if sub.SendMessage(message.NewMessage(topicObj, "x")) {
		t.Error("SendMessage after Close should return false")
	}
	if sub.DroppedCount() != 1 {
		t.Errorf("DroppedCount=%d, want 1", sub.DroppedCount())
	}
}

// --- Delivery policy tests ---

func TestDeliveryPolicyDropOldest(t *testing.T) {
	topicObj, _ := topic.New("events")
	sub := subscription.NewSubscription(topicObj,
		subscription.WithPolicy(subscription.DropOldest),
		subscription.WithBufferSize(3),
	)
	defer sub.Close()

	msg := func(d string) message.Message { return message.NewMessage(topicObj, d) }

	// Fill to capacity.
	sub.SendMessage(msg("a"))
	sub.SendMessage(msg("b"))
	sub.SendMessage(msg("c"))

	// This should evict "a" and deliver "d".
	sub.SendMessage(msg("d"))

	got := collectN(sub.MessageChannel(), 3, 200*time.Millisecond)
	if len(got) != 3 {
		t.Fatalf("got %d messages, want 3: %v", len(got), got)
	}
	if got[0] == "a" {
		t.Errorf("oldest message 'a' should have been evicted, got %v", got)
	}
	// Last message must be "d".
	if got[len(got)-1] != "d" {
		t.Errorf("newest message must be 'd', got %v", got[len(got)-1])
	}
}

func TestDeliveryPolicyBlocking(t *testing.T) {
	topicObj, _ := topic.New("events")
	sub := subscription.NewSubscription(topicObj,
		subscription.WithPolicy(subscription.Blocking),
		subscription.WithBufferSize(1),
	)

	msg := message.NewMessage(topicObj, "hello")

	// Fill the 1-slot buffer.
	if !sub.SendMessage(msg) {
		t.Fatal("first send to empty buffer should succeed")
	}

	// Start a reader that drains after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		<-sub.MessageChannel()
		sub.Close()
	}()

	// Second send should block until the reader drains or Close is called.
	sub.SendMessage(msg) // may succeed (if reader drains) or return false (Close wins)
}

func TestDeliveryPolicyBlockingCloseCancels(t *testing.T) {
	topicObj, _ := topic.New("events")
	sub := subscription.NewSubscription(topicObj,
		subscription.WithPolicy(subscription.Blocking),
		subscription.WithBufferSize(0), // unbuffered
	)

	done := make(chan bool, 1)
	go func() {
		// This will block until Close() is called.
		result := sub.SendMessage(message.NewMessage(topicObj, "x"))
		done <- result
	}()

	time.Sleep(30 * time.Millisecond)
	sub.Close()

	select {
	case ok := <-done:
		if ok {
			t.Error("blocking send should return false after Close")
		}
	case <-time.After(time.Second):
		t.Error("sendBlocking did not unblock after Close")
	}
}

// --- AckedSubscription tests ---

func TestAckedSubscriptionDelivers(t *testing.T) {
	topicObj, _ := topic.New("events")
	inner := subscription.NewSubscription(topicObj)
	as := subscription.NewAckedSubscription(inner,
		subscription.WithAckTimeout(500*time.Millisecond),
		subscription.WithMaxRetries(2),
	)

	msg := message.NewMessage(topicObj, "hello")
	inner.SendMessage(msg)

	select {
	case env := <-as.EnvelopeChannel():
		if env.Msg.Data() != "hello" {
			t.Errorf("data=%v, want hello", env.Msg.Data())
		}
		env.Ack()
	case <-time.After(time.Second):
		t.Fatal("no envelope received")
	}

	// After ack, no redelivery should happen.
	select {
	case env := <-as.EnvelopeChannel():
		t.Errorf("unexpected redelivery: %v", env.Msg.Data())
	case <-time.After(700 * time.Millisecond):
	}
}

func TestAckedSubscriptionRedelivers(t *testing.T) {
	topicObj, _ := topic.New("events")
	inner := subscription.NewSubscription(topicObj)
	as := subscription.NewAckedSubscription(inner,
		subscription.WithAckTimeout(100*time.Millisecond),
		subscription.WithMaxRetries(3),
	)

	msg := message.NewMessage(topicObj, "retry-me")
	inner.SendMessage(msg)

	// Receive but do NOT ack.
	select {
	case <-as.EnvelopeChannel():
	case <-time.After(time.Second):
		t.Fatal("first delivery not received")
	}

	// Should be redelivered after ackTimeout.
	select {
	case env := <-as.EnvelopeChannel():
		if env.Msg.Data() != "retry-me" {
			t.Errorf("redelivered data=%v, want retry-me", env.Msg.Data())
		}
		env.Ack()
	case <-time.After(time.Second):
		t.Fatal("redelivery not received")
	}
}

func TestAckedSubscriptionMaxRetries(t *testing.T) {
	topicObj, _ := topic.New("events")
	inner := subscription.NewSubscription(topicObj)
	as := subscription.NewAckedSubscription(inner,
		subscription.WithAckTimeout(50*time.Millisecond),
		subscription.WithMaxRetries(2),
	)

	inner.SendMessage(message.NewMessage(topicObj, "expire"))

	received := 0
	timeout := time.After(2 * time.Second)
	for {
		select {
		case env, ok := <-as.EnvelopeChannel():
			if !ok {
				goto done
			}
			received++
			_ = env
		case <-timeout:
			goto done
		}
	}
done:
	// initial delivery + up to maxRetries redeliveries = 3 total
	if received > 3 {
		t.Errorf("received %d deliveries, want ≤3 (1 initial + 2 retries)", received)
	}
}

func TestAckedSubscriptionCloseEndsChannel(t *testing.T) {
	topicObj, _ := topic.New("events")
	inner := subscription.NewSubscription(topicObj)
	as := subscription.NewAckedSubscription(inner,
		subscription.WithAckTimeout(time.Second),
		subscription.WithMaxRetries(1),
	)

	inner.Close()

	select {
	case _, ok := <-as.EnvelopeChannel():
		if ok {
			t.Error("envelope channel should be closed when inner is closed")
		}
	case <-time.After(2 * time.Second):
		t.Error("envelope channel did not close after inner.Close()")
	}
}

// collectN drains up to n messages from ch within d, returning their Data() strings.
func collectN(ch <-chan message.Message, n int, d time.Duration) []string {
	var out []string
	timer := time.NewTimer(d)
	defer timer.Stop()
	for len(out) < n {
		select {
		case m := <-ch:
			if s, ok := m.Data().(string); ok {
				out = append(out, s)
			}
		case <-timer.C:
			return out
		}
	}
	return out
}
