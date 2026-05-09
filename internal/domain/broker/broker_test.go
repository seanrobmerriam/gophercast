package broker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gophercast/gophercast/internal/domain/broker"
	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/subscription"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

func TestNewBroker(t *testing.T) {
	if broker.NewBroker() == nil {
		t.Error("NewBroker() should not return nil")
	}
}

func TestBrokerSubscribe(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("users")

	sub1 := b.Subscribe(context.Background(), topicObj)
	if sub1 == nil {
		t.Fatal("Subscribe() should not return nil")
	}
	if sub1.Topic().String() != "users" {
		t.Errorf("Subscription topic = %v, want %v", sub1.Topic().String(), "users")
	}

	sub2 := b.Subscribe(context.Background(), topicObj)
	if sub1.ID() == sub2.ID() {
		t.Error("Subscription IDs should be unique")
	}
}

func TestBrokerPublishNoSubscribers(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("users")
	delivered, dropped := b.Publish(context.Background(), message.NewMessage(topicObj, "test"))
	if delivered != 0 || dropped != 0 {
		t.Errorf("publish to empty topic: delivered=%d dropped=%d, want 0/0", delivered, dropped)
	}
}

func TestBrokerPublishDeliversToSubscriber(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("users")
	sub := b.Subscribe(context.Background(), topicObj)

	delivered, dropped := b.Publish(context.Background(), message.NewMessage(topicObj, "test"))
	if delivered != 1 || dropped != 0 {
		t.Errorf("delivered=%d dropped=%d, want 1/0", delivered, dropped)
	}

	select {
	case msg := <-sub.MessageChannel():
		if msg.Data() != "test" {
			t.Errorf("Received data = %v, want %v", msg.Data(), "test")
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive message within 1s")
	}
}

func TestBrokerPublishMultipleSubscribers(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("users")
	sub1 := b.Subscribe(context.Background(), topicObj)
	sub2 := b.Subscribe(context.Background(), topicObj)
	sub3 := b.Subscribe(context.Background(), topicObj)

	delivered, _ := b.Publish(context.Background(), message.NewMessage(topicObj, "x"))
	if delivered != 3 {
		t.Errorf("delivered=%d, want 3", delivered)
	}

	for i, ch := range []<-chan message.Message{sub1.MessageChannel(), sub2.MessageChannel(), sub3.MessageChannel()} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Errorf("sub%d did not receive", i+1)
		}
	}
}

func TestBrokerUnsubscribeStopsDelivery(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("users")
	sub := b.Subscribe(context.Background(), topicObj)
	b.Unsubscribe(sub.ID())

	delivered, _ := b.Publish(context.Background(), message.NewMessage(topicObj, "x"))
	if delivered != 0 {
		t.Errorf("delivered=%d after unsubscribe, want 0", delivered)
	}

	if _, ok := <-sub.MessageChannel(); ok {
		t.Error("channel should be closed after unsubscribe")
	}
}

func TestBrokerCloseClosesSubscriptions(t *testing.T) {
	b := broker.NewBroker()
	topicObj, _ := topic.New("users")
	sub := b.Subscribe(context.Background(), topicObj)

	b.Close()

	if _, ok := <-sub.MessageChannel(); ok {
		t.Error("Channel should be closed after broker.Close()")
	}
}

func TestBrokerPublishWrongTopic(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	usersTopic, _ := topic.New("users")
	ordersTopic, _ := topic.New("orders")

	sub := b.Subscribe(context.Background(), usersTopic)
	delivered, _ := b.Publish(context.Background(), message.NewMessage(ordersTopic, "order"))
	if delivered != 0 {
		t.Errorf("cross-topic delivered=%d, want 0", delivered)
	}

	select {
	case msg := <-sub.MessageChannel():
		t.Errorf("Should not receive message for different topic, got: %v", msg.Data())
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBrokerConcurrentPublish(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("users")
	sub := b.Subscribe(context.Background(), topicObj)

	var received atomic.Int64
	go func() {
		for range sub.MessageChannel() {
			received.Add(1)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				b.Publish(context.Background(), message.NewMessage(topicObj, "x"))
			}
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for received.Load() < 100 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := received.Load(); got != 100 {
		t.Errorf("received=%d, want 100", got)
	}
}

func TestBrokerWildcardSingleSegment(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	pat, err := topic.NewPattern("users.*")
	if err != nil {
		t.Fatalf("NewPattern: %v", err)
	}
	sub := b.SubscribePattern(context.Background(), pat)

	for _, name := range []string{"users.created", "users.deleted", "users.created.v2", "orders.created"} {
		tp, _ := topic.New(name)
		b.Publish(context.Background(), message.NewMessage(tp, name))
	}

	got := drainFor(sub.MessageChannel(), 200*time.Millisecond)
	want := map[string]bool{"users.created": true, "users.deleted": true}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("unexpected: %v", g)
		}
	}
}

func TestBrokerWildcardMultiSegment(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	pat, _ := topic.NewPattern("users.#")
	sub := b.SubscribePattern(context.Background(), pat)

	for _, name := range []string{"users.created", "users.created.v2", "users", "orders.x"} {
		tp, _ := topic.New(name)
		b.Publish(context.Background(), message.NewMessage(tp, name))
	}

	got := drainFor(sub.MessageChannel(), 200*time.Millisecond)
	if len(got) != 2 {
		t.Fatalf("got %v, want 2 messages", got)
	}
}

func TestBrokerContextCancelUnsubscribes(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("users")
	ctx, cancel := context.WithCancel(context.Background())
	sub := b.Subscribe(ctx, topicObj)

	cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		delivered, _ := b.Publish(context.Background(), message.NewMessage(topicObj, "x"))
		if delivered == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	delivered, _ := b.Publish(context.Background(), message.NewMessage(topicObj, "x"))
	if delivered != 0 {
		t.Errorf("delivered=%d after ctx cancel, want 0", delivered)
	}
	// Drain any buffered messages then verify channel is closed.
	for range sub.MessageChannel() {
	}
}

func drainFor(ch <-chan message.Message, d time.Duration) []string {
	var out []string
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return out
			}
			if s, ok := m.Data().(string); ok {
				out = append(out, s)
			}
		case <-timer.C:
			return out
		}
	}
}

func TestBrokerSubscribeWithPolicy(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("events")
	// Tiny buffer + DropOldest policy.
	sub := b.Subscribe(context.Background(), topicObj,
		subscription.WithPolicy(subscription.DropOldest),
		subscription.WithBufferSize(2),
	)

	// Publish 4 messages; only 2 fit so oldest should be evicted.
	for _, d := range []string{"a", "b", "c", "d"} {
		b.Publish(context.Background(), message.NewMessage(topicObj, d))
	}

	got := drainFor(sub.MessageChannel(), 200*time.Millisecond)
	if len(got) != 2 {
		t.Fatalf("got %d messages, want 2: %v", len(got), got)
	}
	if got[1] != "d" {
		t.Errorf("newest message must be 'd', got %v", got[1])
	}
}

func TestBrokerSubscribeAtLeastOnce(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("events")
	as := b.SubscribeAtLeastOnce(context.Background(), topicObj,
		subscription.WithAckTimeout(200*time.Millisecond),
		subscription.WithMaxRetries(3),
	)

	b.Publish(context.Background(), message.NewMessage(topicObj, "once"))

	// Receive and ack the first delivery.
	select {
	case env := <-as.EnvelopeChannel():
		if env.Msg.Data() != "once" {
			t.Errorf("data=%v, want once", env.Msg.Data())
		}
		env.Ack()
	case <-time.After(time.Second):
		t.Fatal("no envelope")
	}

	// No redelivery expected after ack.
	select {
	case env := <-as.EnvelopeChannel():
		t.Errorf("unexpected redelivery: %v", env.Msg.Data())
	case <-time.After(400 * time.Millisecond):
	}
}

func TestBrokerSubscribeAtLeastOnceUnsubscribeClosesChannel(t *testing.T) {
	b := broker.NewBroker()
	defer b.Close()

	topicObj, _ := topic.New("events")
	as := b.SubscribeAtLeastOnce(context.Background(), topicObj)
	b.Unsubscribe(as.ID())

	select {
	case _, ok := <-as.EnvelopeChannel():
		if ok {
			t.Error("envelope channel should be closed after Unsubscribe")
		}
	case <-time.After(2 * time.Second):
		t.Error("envelope channel did not close after Unsubscribe")
	}
}
