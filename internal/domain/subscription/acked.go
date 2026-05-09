package subscription

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

// Envelope wraps a Message with an Ack function for at-least-once delivery.
// Call Ack() exactly once after successfully processing the message to prevent
// redelivery. Calling Ack() after the deadline has no effect.
type Envelope struct {
	Msg message.Message
	Ack func()
}

// AckOption configures an AckedSubscription.
type AckOption func(*ackConfig)

type ackConfig struct {
	ackTimeout time.Duration
	maxRetries int
}

// WithAckTimeout sets how long the broker waits for an Ack before redelivering.
// Defaults to 30 seconds.
func WithAckTimeout(d time.Duration) AckOption {
	return func(c *ackConfig) {
		if d > 0 {
			c.ackTimeout = d
		}
	}
}

// WithMaxRetries sets the maximum number of redelivery attempts before a message
// is permanently discarded. Defaults to 3. A value of 0 means unlimited retries.
func WithMaxRetries(n int) AckOption {
	return func(c *ackConfig) { c.maxRetries = n }
}

// pendingMsg tracks a message awaiting acknowledgement.
type pendingMsg struct {
	msg       message.Message
	retries   int
	nextRetry time.Time
}

// AckedSubscription provides at-least-once delivery semantics. Messages are
// redelivered via EnvelopeChannel until the caller acknowledges them or the
// maximum retry count is reached.
//
// The inner Subscription is owned by the AckedSubscription; callers must not
// read from inner.MessageChannel() directly.
type AckedSubscription struct {
	inner      *Subscription
	ackTimeout time.Duration
	maxRetries int

	envelopes chan Envelope
	dropped   atomic.Uint64

	mu      sync.Mutex
	pending map[string]*pendingMsg

	wg sync.WaitGroup
}

// NewAckedSubscription wraps inner with at-least-once delivery semantics.
// inner must not be read from by any other goroutine after this call.
func NewAckedSubscription(inner *Subscription, opts ...AckOption) *AckedSubscription {
	cfg := ackConfig{
		ackTimeout: 30 * time.Second,
		maxRetries: 3,
	}
	for _, o := range opts {
		o(&cfg)
	}

	as := &AckedSubscription{
		inner:      inner,
		ackTimeout: cfg.ackTimeout,
		maxRetries: cfg.maxRetries,
		envelopes:  make(chan Envelope, 200),
		pending:    make(map[string]*pendingMsg),
	}

	as.wg.Add(1)
	go as.run()
	return as
}

// ID returns the subscription identifier (same as the inner subscription).
func (as *AckedSubscription) ID() string { return as.inner.ID() }

// Topic returns the topic this subscription is for.
func (as *AckedSubscription) Topic() topic.Topic { return as.inner.Topic() }

// CreatedAt returns when the subscription was created.
func (as *AckedSubscription) CreatedAt() time.Time { return as.inner.CreatedAt() }

// EnvelopeChannel returns the channel from which callers receive messages.
// Each Envelope contains an Ack function; call it after processing the message.
// The channel is closed when the underlying subscription is closed.
func (as *AckedSubscription) EnvelopeChannel() <-chan Envelope { return as.envelopes }

// DroppedCount returns how many messages were permanently discarded (max retries
// exceeded) plus how many were dropped by the inner subscription's buffer.
func (as *AckedSubscription) DroppedCount() uint64 {
	return as.dropped.Load() + as.inner.DroppedCount()
}

func (as *AckedSubscription) run() {
	defer func() {
		as.wg.Done()
		close(as.envelopes)
	}()

	retryInterval := as.ackTimeout / 4
	if retryInterval < 50*time.Millisecond {
		retryInterval = 50 * time.Millisecond
	}

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-as.inner.MessageChannel():
			if !ok {
				return
			}
			as.enqueue(msg)
		case <-ticker.C:
			as.redeliver()
		}
	}
}

func (as *AckedSubscription) enqueue(msg message.Message) {
	as.mu.Lock()
	as.pending[msg.ID()] = &pendingMsg{
		msg:       msg,
		retries:   0,
		nextRetry: time.Now().Add(as.ackTimeout),
	}
	env := as.makeEnvelope(msg.ID())
	as.mu.Unlock()

	select {
	case as.envelopes <- env:
	default:
		// Envelope buffer full; the redeliver loop will retry.
	}
}

// makeEnvelope must be called with as.mu held.
func (as *AckedSubscription) makeEnvelope(msgID string) Envelope {
	p := as.pending[msgID]
	var once sync.Once
	ack := func() {
		once.Do(func() {
			as.mu.Lock()
			delete(as.pending, msgID)
			as.mu.Unlock()
		})
	}
	return Envelope{Msg: p.msg, Ack: ack}
}

func (as *AckedSubscription) redeliver() {
	now := time.Now()

	as.mu.Lock()
	var toRedeliver []Envelope
	var toDiscard []string

	for id, p := range as.pending {
		if now.Before(p.nextRetry) {
			continue
		}
		if as.maxRetries > 0 && p.retries >= as.maxRetries {
			toDiscard = append(toDiscard, id)
			continue
		}
		p.retries++
		p.nextRetry = now.Add(as.ackTimeout)
		toRedeliver = append(toRedeliver, as.makeEnvelope(id))
	}
	for _, id := range toDiscard {
		delete(as.pending, id)
		as.dropped.Add(1)
	}
	as.mu.Unlock()

	for _, env := range toRedeliver {
		select {
		case as.envelopes <- env:
		default:
		}
	}
}
