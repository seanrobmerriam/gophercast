# gophercast

A simple, modular publish-subscribe (pub/sub) system written in Go with zero external dependencies.

## What is GopherCast?

GopherCast is a pub/sub messaging system where:

- **Publishers** send messages to topics.
- **Subscribers** receive messages from topics they're 'subscribed' to.
- **Brokers** manage the routing of messages from publishers to subscribers.

Publishers and subscribers are decoupled; they don't need to know about each other.

## Features

- Simple, clear code that is easily understood
- Zero external dependencies (Go standard library only)
- Modular design (each component can run independently)
- Thread-safe (safe for concurrent use)
- Non-blocking message delivery using goroutines
- In-process only (no network transport)

## Installation

### go get

```bash
go get github.com/gophercast/gophercast
```
## git

```bash
git clone github.com/gophercast/gophercast
```

## Quick Start

gophercast has a simple implementation you can easily build more features around:

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

    // Create broker
    b := broker.NewBroker()
    defer b.Close()

    // Create topic
    t, _ := topic.New("events")

    // Subscribe (cancelling ctx auto-unsubscribes)
    sub := b.Subscribe(ctx, t)

    // Listen for messages
    go func() {
        for msg := range sub.MessageChannel() {
            fmt.Println("Received:", msg.Data())
        }
    }()

    // Publish
    delivered, dropped := b.Publish(ctx, message.NewMessage(t, "Hello, World!"))
    fmt.Printf("delivered=%d dropped=%d\n", delivered, dropped)
}
```

### Wildcard subscriptions

Patterns use `.` segment separators and support two wildcards:

- `*` matches exactly one segment (`users.*` matches `users.created` but not `users.created.v2`).
- `#` matches one or more trailing segments and may only appear last (`users.#` matches `users.created` and `users.created.v2`, but not `users`).

```go
pat, _ := topic.NewPattern("users.*")
sub := b.SubscribePattern(ctx, pat)
```

### Concepts:

**Topic**: A named channel (e.g., "user.created", "order.placed").

**Message**: Data being sent (includes topic, data, timestamp).

**Subscription**: Registration to receive messages from a topic.

**Broker**: Central hub that manages distribution.


```
┌─────────────────────────────────────────┐
│              Broker                     │
│  - Manages topics and subscriptions     │
│  - Routes messages to subscribers       │
└──────────┬────────────────┬─────────────┘
           │                │
           ▼                ▼
    ┌─────────────┐   ┌─────────────┐
    │ Publisher   │   │ Subscriber  │
    │ (sends msg) │   │ (recv msg)  │
    └─────────────┘   └─────────────┘
```

## Usage Examples

### Example 1: Basic Pub/Sub

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gophercast/gophercast/internal/domain/broker"
	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

func main() {
	fmt.Println("=== GopherCast System Example ===")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Step 1: Create broker
	fmt.Println("1. Creating broker...")
	b := broker.NewBroker()
	defer b.Close()

	// Step 2: Create topics
	fmt.Println("2. Creating topics...")
	usersTopic, _ := topic.New("users")
	ordersTopic, _ := topic.New("orders")

	// Step 3: Create subscribers (cancelling ctx auto-unsubscribes them)
	fmt.Println("3. Creating subscribers...")

	sub1 := b.Subscribe(ctx, usersTopic)

	go func() {
		fmt.Println("   [Subscriber 1] Listening to 'users' topic...")
		for msg := range sub1.MessageChannel() {
			fmt.Printf("   [Subscriber 1] Received: %s\n", msg.String())
		}
	}()

	sub2 := b.Subscribe(ctx, usersTopic)

	go func() {
		fmt.Println("   [Subscriber 2] Listening to 'users' topic...")
		for msg := range sub2.MessageChannel() {
			fmt.Printf("   [Subscriber 2] Received: %s\n", msg.String())
		}
	}()

	sub3 := b.Subscribe(ctx, ordersTopic)

	go func() {
		fmt.Println("   [Subscriber 3] Listening to 'orders' topic...")
		for msg := range sub3.MessageChannel() {
			fmt.Printf("   [Subscriber 3] Received: %s\n", msg.String())
		}
	}()

	// Give subscribers time to start
	time.Sleep(100 * time.Millisecond)

	// Step 4: Publish messages
	fmt.Println("\n4. Publishing messages...")

	// Publish to users topic
	fmt.Println("   Publishing to 'users' topic...")
	b.Publish(ctx, message.NewMessage(usersTopic, "User Alice created"))

	time.Sleep(100 * time.Millisecond)

	b.Publish(ctx, message.NewMessage(usersTopic, "User Bob created"))

	time.Sleep(100 * time.Millisecond)

	// Publish to orders topic
	fmt.Println("\n   Publishing to 'orders' topic...")
	b.Publish(ctx, message.NewMessage(ordersTopic, "Order #123 placed"))

	// Wait for messages to be received
	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n=== Example Complete ===")
	fmt.Println("\nObservations:")
	fmt.Println("- Subscribers 1 and 2 both received messages from 'users' topic")
	fmt.Println("- Subscriber 3 only received messages from 'orders' topic")
	fmt.Println("- Each subscriber got their own copy of the messages")
}
```

### Example 2: Multiple Topics

```go
ctx := context.Background()
b := broker.NewBroker()

userTopic, _ := topic.New("users")
orderTopic, _ := topic.New("orders")

userSub := b.Subscribe(ctx, userTopic)
orderSub := b.Subscribe(ctx, orderTopic)

// Publish to different topics
b.Publish(ctx, message.NewMessage(userTopic, "User created"))
b.Publish(ctx, message.NewMessage(orderTopic, "Order placed"))
```

### Example 3: Multiple Subscribers

```go
ctx := context.Background()
b := broker.NewBroker()
t, _ := topic.New("events")

// Multiple subscribers to same topic
sub1 := b.Subscribe(ctx, t)
sub2 := b.Subscribe(ctx, t)
sub3 := b.Subscribe(ctx, t)

// All three receive the same message
b.Publish(ctx, message.NewMessage(t, "Event occurred"))
```

## Running Examples

```bash
# Run the complete example
go run examples/basic/main.go

# Run standalone broker
go run cmd/broker/main.go

# Run publisher example
go run cmd/publisher/main.go

# Run subscriber example
go run cmd/subscriber/main.go
```
