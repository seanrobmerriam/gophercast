package broker

import (
	"context"
	"sync"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/subscription"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

// entry pairs a subscription with the pattern it was registered against.
type entry struct {
	pattern topic.Pattern
	sub     *subscription.Subscription
}

// Broker is the central hub that manages topics and routes messages to subscribers.
// It is safe for concurrent use by multiple goroutines.
//
// Delivery is governed per-subscription by the subscription.DeliveryPolicy.
// Publishing never blocks for longer than required by the slowest Blocking subscriber.
type Broker struct {
	entries []entry
	mutex   sync.RWMutex
}

// NewBroker creates a new message broker.
func NewBroker() *Broker {
	return &Broker{}
}

// Subscribe registers an exact-match subscription for the given topic.
// When ctx is cancelled the subscription is automatically removed and closed.
// Pass context.Background() for a subscription with no automatic teardown.
func (b *Broker) Subscribe(ctx context.Context, t topic.Topic, opts ...subscription.Option) *subscription.Subscription {
	return b.SubscribePattern(ctx, t.AsPattern(), opts...)
}

// SubscribePattern registers a subscription that receives every message whose
// topic matches p. See topic.Pattern for the supported wildcard syntax.
func (b *Broker) SubscribePattern(ctx context.Context, p topic.Pattern, opts ...subscription.Option) *subscription.Subscription {
	sub := subscription.NewSubscription(topic.FromString(p.String()), opts...)

	b.mutex.Lock()
	b.entries = append(b.entries, entry{pattern: p, sub: sub})
	b.mutex.Unlock()

	if ctx != nil && ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			b.Unsubscribe(sub.ID())
		}()
	}
	return sub
}

// SubscribeAtLeastOnce registers an exact-match subscription with at-least-once
// delivery semantics. Messages are redelivered via the returned
// AckedSubscription.EnvelopeChannel() until acknowledged or max retries reached.
func (b *Broker) SubscribeAtLeastOnce(ctx context.Context, t topic.Topic, opts ...subscription.AckOption) *subscription.AckedSubscription {
	return b.SubscribePatternAtLeastOnce(ctx, t.AsPattern(), opts...)
}

// SubscribePatternAtLeastOnce is the wildcard variant of SubscribeAtLeastOnce.
func (b *Broker) SubscribePatternAtLeastOnce(ctx context.Context, p topic.Pattern, opts ...subscription.AckOption) *subscription.AckedSubscription {
	// Use a small buffer; AckedSubscription drains it promptly.
	inner := b.SubscribePattern(ctx, p, subscription.WithBufferSize(500))
	return subscription.NewAckedSubscription(inner, opts...)
}

// Unsubscribe removes a subscription from the broker and closes its channel.
// Calling Unsubscribe with an unknown id is a no-op.
func (b *Broker) Unsubscribe(subscriptionID string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for i, e := range b.entries {
		if e.sub.ID() == subscriptionID {
			e.sub.Close()
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			return
		}
	}
}

// Publish offers msg to every subscription whose pattern matches msg.Topic().
// It returns how many subscribers accepted the message and how many dropped it
// (because their buffer was full or they were closed). Publish honours ctx
// only insofar as a cancelled ctx causes it to return (0, 0) without
// delivering; it never blocks on subscriber buffers.
func (b *Broker) Publish(ctx context.Context, msg message.Message) (delivered, dropped int) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return 0, 0
		default:
		}
	}

	b.mutex.RLock()
	matched := make([]*subscription.Subscription, 0, len(b.entries))
	for _, e := range b.entries {
		if e.pattern.Matches(msg.Topic()) {
			matched = append(matched, e.sub)
		}
	}
	b.mutex.RUnlock()

	for _, s := range matched {
		if s.SendMessage(msg) {
			delivered++
		} else {
			dropped++
		}
	}
	return delivered, dropped
}

// Close closes all subscriptions and shuts down the broker.
// After closing, the broker should not be used.
func (b *Broker) Close() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, e := range b.entries {
		e.sub.Close()
	}
	b.entries = nil
}
