package subscription

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

// DeliveryPolicy controls how SendMessage behaves when the subscriber's
// buffer is full.
type DeliveryPolicy int

const (
	// BestEffort (default) is non-blocking: if the buffer is full the
	// message is dropped and DroppedCount is incremented.
	BestEffort DeliveryPolicy = iota

	// DropOldest evicts the oldest buffered message to make room for the
	// new one. The evicted message is counted as dropped.
	DropOldest

	// Blocking waits until the subscriber reads from its buffer or the
	// subscription is closed. Use carefully: a slow subscriber blocks the
	// Publish call for as long as it takes to drain at least one slot.
	Blocking
)

// Option configures a Subscription at creation time.
type Option func(*config)

type config struct {
	policy     DeliveryPolicy
	bufferSize int // -1 = use default (200)
}

// WithPolicy sets the delivery policy. Defaults to BestEffort.
func WithPolicy(p DeliveryPolicy) Option {
	return func(c *config) { c.policy = p }
}

// WithBufferSize sets the message buffer capacity. Defaults to 200.
// Pass 0 for an unbuffered channel (useful with the Blocking policy).
func WithBufferSize(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.bufferSize = n
		}
	}
}

// Subscription represents a subscriber's registration to receive messages from a topic.
// Each subscription has its own channel for receiving messages.
type Subscription struct {
	id             string
	topic          topic.Topic
	policy         DeliveryPolicy
	messageChannel chan message.Message
	done           chan struct{} // closed once on Close
	createdAt      time.Time
	dropped        atomic.Uint64
	closed         bool
	mu             sync.Mutex
	// sendMu guards the Blocking send path against concurrent Close.
	// Blocking senders hold a read-lock while in their channel select;
	// Close() acquires the write-lock after signalling done, which waits
	// for all in-flight blocking sends to exit before closing the channel.
	sendMu sync.RWMutex
}

// NewSubscription creates a new subscription for the given topic.
func NewSubscription(t topic.Topic, opts ...Option) *Subscription {
	cfg := config{bufferSize: -1}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.bufferSize < 0 {
		cfg.bufferSize = 200
	}
	return &Subscription{
		id:             generateSubscriptionID(),
		topic:          t,
		policy:         cfg.policy,
		messageChannel: make(chan message.Message, cfg.bufferSize),
		done:           make(chan struct{}),
		createdAt:      time.Now(),
	}
}

// ID returns the unique subscription identifier.
func (s *Subscription) ID() string {
	return s.id
}

// Topic returns the topic this subscription is for.
func (s *Subscription) Topic() topic.Topic {
	return s.topic
}

// CreatedAt returns when the subscription was created.
func (s *Subscription) CreatedAt() time.Time {
	return s.createdAt
}

// MessageChannel returns the channel for receiving messages.
// Subscribers should read from this channel to receive messages.
func (s *Subscription) MessageChannel() <-chan message.Message {
	return s.messageChannel
}

// DroppedCount returns the number of messages dropped because the subscriber's
// channel was full or the subscription was closed at delivery time.
func (s *Subscription) DroppedCount() uint64 {
	return s.dropped.Load()
}

// SendMessage attempts to deliver a message according to the subscription's
// DeliveryPolicy. It reports true if the message was accepted for delivery.
func (s *Subscription) SendMessage(msg message.Message) bool {
	switch s.policy {
	case DropOldest:
		return s.sendDropOldest(msg)
	case Blocking:
		return s.sendBlocking(msg)
	default:
		return s.sendBestEffort(msg)
	}
}

func (s *Subscription) sendBestEffort(msg message.Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		s.dropped.Add(1)
		return false
	}

	select {
	case s.messageChannel <- msg:
		return true
	default:
		s.dropped.Add(1)
		return false
	}
}

func (s *Subscription) sendDropOldest(msg message.Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		s.dropped.Add(1)
		return false
	}

	select {
	case s.messageChannel <- msg:
		return true
	default:
		// Channel full: evict the oldest message to make room.
		select {
		case <-s.messageChannel:
			s.dropped.Add(1)
		default:
		}
		select {
		case s.messageChannel <- msg:
			return true
		default:
			s.dropped.Add(1)
			return false
		}
	}
}

func (s *Subscription) sendBlocking(msg message.Message) bool {
	// Hold a read-lock for the entirety of the blocking send.
	// Close() takes the write-lock after signalling done, which means
	// it cannot close s.messageChannel until we release here.
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()

	// Check closed before entering the channel select.
	select {
	case <-s.done:
		s.dropped.Add(1)
		return false
	default:
	}

	select {
	case s.messageChannel <- msg:
		return true
	case <-s.done:
		s.dropped.Add(1)
		return false
	}
}

// Close closes the message channel.
// After closing, no more messages can be sent to this subscription.
func (s *Subscription) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.done)
	s.mu.Unlock()

	// Acquire the write-lock to wait for all in-flight blocking sends
	// (which hold read-locks) to observe the done signal and exit their
	// select before we close the channel.
	s.sendMu.Lock()
	s.sendMu.Unlock() //nolint:staticcheck // intentional barrier, not a critical section
	close(s.messageChannel)
}

// generateSubscriptionID creates a unique identifier for a subscription.
func generateSubscriptionID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}
