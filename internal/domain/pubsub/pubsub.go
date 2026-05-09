// Package pubsub defines the abstract Publisher, Subscriber, and Broker
// interfaces. The concrete in-process implementation lives in the broker
// package; alternative transports (e.g. networked brokers) can satisfy the
// same contract.
package pubsub

import (
	"context"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/subscription"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

// Publisher sends messages into the system.
type Publisher interface {
	// Publish offers msg to every matching subscriber and returns how many
	// accepted it (delivered) versus how many dropped it because their
	// buffer was full or they were closed (dropped).
	Publish(ctx context.Context, msg message.Message) (delivered, dropped int)
}

// Subscriber registers interest in topics.
type Subscriber interface {
	// Subscribe registers an exact-topic subscription. Cancelling ctx
	// removes and closes the subscription. Optional subscription.Option
	// values control buffer size and delivery policy.
	Subscribe(ctx context.Context, t topic.Topic, opts ...subscription.Option) *subscription.Subscription
	// SubscribePattern registers a wildcard subscription.
	SubscribePattern(ctx context.Context, p topic.Pattern, opts ...subscription.Option) *subscription.Subscription
	// SubscribeAtLeastOnce wraps Subscribe with at-least-once semantics.
	SubscribeAtLeastOnce(ctx context.Context, t topic.Topic, opts ...subscription.AckOption) *subscription.AckedSubscription
	// SubscribePatternAtLeastOnce is the wildcard variant of SubscribeAtLeastOnce.
	SubscribePatternAtLeastOnce(ctx context.Context, p topic.Pattern, opts ...subscription.AckOption) *subscription.AckedSubscription
	// Unsubscribe removes a subscription by id; unknown ids are a no-op.
	Unsubscribe(id string)
}

// Broker combines publishing and subscribing with a lifecycle.
type Broker interface {
	Publisher
	Subscriber
	Close()
}
