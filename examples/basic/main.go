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

	fmt.Println("1. Creating broker...")
	b := broker.NewBroker()
	defer b.Close()

	fmt.Println("2. Creating topics...")
	usersTopic, _ := topic.New("users")
	ordersTopic, _ := topic.New("orders")

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

	time.Sleep(100 * time.Millisecond)

	fmt.Println("\n4. Publishing messages...")

	fmt.Println("   Publishing to 'users' topic...")
	b.Publish(ctx, message.NewMessage(usersTopic, "User Alice created"))
	time.Sleep(100 * time.Millisecond)
	b.Publish(ctx, message.NewMessage(usersTopic, "User Bob created"))
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\n   Publishing to 'orders' topic...")
	b.Publish(ctx, message.NewMessage(ordersTopic, "Order #123 placed"))

	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n=== Example Complete ===")
}
