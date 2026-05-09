package transport_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gophercast/gophercast/internal/domain/broker"
	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/topic"
	"github.com/gophercast/gophercast/internal/transport"
)

// startServer starts a server on a random port and returns the address string.
// The server is closed when t.Cleanup runs.
func startServer(t *testing.T) string {
	t.Helper()
	b := broker.NewBroker()
	srv := transport.NewServer(b)
	addr, err := srv.Listen(":0")
	if err != nil {
		t.Fatalf("server listen: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return addr.String()
}

func TestTransportPublishSubscribe(t *testing.T) {
	addr := startServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := transport.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	topicObj, _ := topic.New("events.created")

	sub, err := client.Subscribe(ctx, topicObj)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Give the server a moment to register the subscription.
	time.Sleep(20 * time.Millisecond)

	payload, _ := json.Marshal("hello-transport")
	msg := message.NewMessageWithHeaders(topicObj, json.RawMessage(payload), map[string]string{
		"correlation-id": "abc123",
	})

	delivered, _, err := client.Publish(ctx, msg)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if delivered != 1 {
		t.Errorf("delivered = %d, want 1", delivered)
	}

	select {
	case received, ok := <-sub.MessageChannel():
		if !ok {
			t.Fatal("message channel closed unexpectedly")
		}
		if received.Topic().String() != "events.created" {
			t.Errorf("topic = %q, want %q", received.Topic().String(), "events.created")
		}
		v, ok := received.Header("correlation-id")
		if !ok || v != "abc123" {
			t.Errorf("correlation-id = %q (present=%v), want %q", v, ok, "abc123")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestTransportWildcardSubscription(t *testing.T) {
	addr := startServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := transport.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	pattern, err := topic.NewPattern("events.*")
	if err != nil {
		t.Fatalf("NewPattern: %v", err)
	}
	sub, err := client.SubscribePattern(ctx, pattern)
	if err != nil {
		t.Fatalf("SubscribePattern: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	topicObj, _ := topic.New("events.created")
	payload, _ := json.Marshal(42)
	msg := message.NewMessage(topicObj, json.RawMessage(payload))

	_, _, err = client.Publish(ctx, msg)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case received, ok := <-sub.MessageChannel():
		if !ok {
			t.Fatal("message channel closed unexpectedly")
		}
		if received.Topic().String() != "events.created" {
			t.Errorf("topic = %q, want %q", received.Topic().String(), "events.created")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestTransportUnsubscribe(t *testing.T) {
	addr := startServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := transport.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	topicObj, _ := topic.New("test.unsub")

	subCtx, subCancel := context.WithCancel(ctx)
	sub, err := client.Subscribe(subCtx, topicObj)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	// Cancel the subscription context — this triggers UNSUBSCRIBE.
	subCancel()

	// The message channel should be closed shortly after.
	select {
	case _, ok := <-sub.MessageChannel():
		if ok {
			t.Error("expected channel to be closed after unsubscribe")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel was not closed after context cancel")
	}
}

func TestTransportClientClose(t *testing.T) {
	addr := startServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := transport.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	topicObj, _ := topic.New("test.close")
	sub, err := client.Subscribe(ctx, topicObj)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	client.Close()

	// After closing, the message channel should be drained/closed.
	select {
	case <-sub.MessageChannel():
		// closed (ok == false) or drained — both acceptable
	case <-time.After(2 * time.Second):
		t.Fatal("message channel not closed after client.Close()")
	}
}
