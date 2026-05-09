# gophercast

A zero-dependency Go pub/sub library with in-process and TCP-networked modes.

## What is GopherCast?

GopherCast routes messages from publishers to subscribers via a broker. Publishers and subscribers are decoupled — they don't need to know about each other.

```
┌──────────────────────────────────────────┐
│                 Broker                   │
│  routes messages · manages subscriptions │
└────────┬─────────────────────┬───────────┘
         │                     │
         ▼                     ▼
  ┌─────────────┐       ┌─────────────┐
  │  Publisher  │       │  Subscriber │
  │ (sends msg) │       │ (recv msg)  │
  └─────────────┘       └─────────────┘
```

Both modes share the same broker API. Use the in-process broker when everything runs in one binary; use the TCP transport to connect separate processes.

## Features

- Zero external dependencies (Go standard library only)
- In-process pub/sub with a thread-safe broker
- TCP transport — run the broker as a standalone server, publish and subscribe from remote clients
- Wildcard subscriptions (`*` single segment, `#` one-or-more trailing segments)
- Message headers with first-class `correlation-id` support
- Three delivery policies: `BestEffort` (drop-on-full), `DropOldest`, `Blocking`
- At-least-once delivery via `AckedSubscription` with configurable timeout and retry limit
- Context-based lifecycle — cancelling a context auto-unsubscribes

## Installation

```bash
go get github.com/gophercast/gophercast
```

## Quick start — in-process

```go
package main

import (
    "context"
    "fmt"

    "github.com/gophercast/gophercast/internal/domain/broker"
    "github.com/gophercast/gophercast/internal/domain/message"
    "github.com/gophercast/gophercast/internal/domain/topic"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    b := broker.NewBroker()
    defer b.Close()

    t, _ := topic.New("events.created")

    sub := b.Subscribe(ctx, t)
    go func() {
        for msg := range sub.MessageChannel() {
            fmt.Println("received:", msg.Data())
        }
    }()

    delivered, dropped := b.Publish(ctx, message.NewMessage(t, "hello"))
    fmt.Printf("delivered=%d dropped=%d\n", delivered, dropped)
}
```

## Quick start — networked (TCP)

**Broker server** (`--addr` defaults to `:7650`):

```bash
go run cmd/broker/main.go --addr :7650
```

**Subscriber** (connect to remote broker):

```bash
go run cmd/subscriber/main.go --broker localhost:7650
```

**Publisher** (connect to remote broker):

```bash
go run cmd/publisher/main.go --broker localhost:7650
```

Or connect from your own code:

```go
import "github.com/gophercast/gophercast/internal/transport"

client, _ := transport.Dial(ctx, "localhost:7650")
defer client.Close()

t, _ := topic.New("events.created")

sub, _ := client.Subscribe(ctx, t)
go func() {
    for msg := range sub.MessageChannel() {
        fmt.Println("received:", msg.Data())
    }
}()

client.Publish(ctx, message.NewMessage(t, "hello from afar"))
```

## Wildcard subscriptions

Topic names use `.` as a segment separator. Two wildcards are supported in patterns:

| Wildcard | Matches | Example pattern | Matches | Does not match |
|----------|---------|-----------------|---------|----------------|
| `*` | Exactly one segment | `users.*` | `users.created` | `users.created.v2`, `users` |
| `#` | One or more trailing segments (must be last) | `users.#` | `users.created`, `users.created.v2` | `users` |

```go
pat, _ := topic.NewPattern("users.#")
sub := b.SubscribePattern(ctx, pat)
```

The same API works with the TCP client:

```go
pat, _ := topic.NewPattern("users.#")
sub, _ := client.SubscribePattern(ctx, pat)
```

## Message headers

```go
msg := message.NewMessageWithHeaders(t, payload, map[string]string{
    "correlation-id": "req-42",
    "content-type":   "application/json",
})

// On the receiver:
cid := msg.CorrelationID()          // "req-42"
ct, ok := msg.Header("content-type")
headers := msg.Headers()             // full copy of all headers
```

## Delivery policies

Control what happens when a subscriber's buffer is full:

```go
import "github.com/gophercast/gophercast/internal/domain/subscription"

// BestEffort (default) — drop the incoming message if buffer is full
sub := b.Subscribe(ctx, t)

// DropOldest — evict the oldest buffered message to make room
sub := b.Subscribe(ctx, t, subscription.WithPolicy(subscription.DropOldest))

// Blocking — block Publish until the subscriber drains a slot
sub := b.Subscribe(ctx, t, subscription.WithPolicy(subscription.Blocking))

// Custom buffer size (default is 200)
sub := b.Subscribe(ctx, t, subscription.WithBufferSize(500))
```

## At-least-once delivery

`AckedSubscription` wraps a subscription and redelivers messages that are not acknowledged within a timeout. Messages are delivered as `Envelope` values that carry an `Ack()` function.

```go
as := b.SubscribeAtLeastOnce(ctx, t,
    subscription.WithAckTimeout(10*time.Second),
    subscription.WithMaxRetries(5),   // 0 = unlimited
)

go func() {
    for envelope := range as.EnvelopeChannel() {
        process(envelope.Msg)
        envelope.Ack() // prevent redelivery
    }
}()
```

Wildcard patterns are supported too:

```go
pat, _ := topic.NewPattern("orders.#")
as := b.SubscribePatternAtLeastOnce(ctx, pat)
```

## Concepts

| Term | Description |
|------|-------------|
| **Topic** | A named channel, e.g. `"users.created"`. Segments separated by `.`. |
| **Pattern** | A topic matcher that may contain `*` or `#` wildcards. |
| **Message** | An immutable value carrying a topic, payload, headers, ID, and timestamp. |
| **Subscription** | A per-subscriber channel registered with the broker. |
| **Broker** | The central hub that routes published messages to matching subscriptions. |

