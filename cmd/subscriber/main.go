package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gophercast/gophercast/internal/domain/topic"
	"github.com/gophercast/gophercast/internal/transport"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:7650", "broker address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := transport.Dial(ctx, *brokerAddr)
	if err != nil {
		log.Fatalf("subscriber: dial: %v", err)
	}
	defer client.Close()

	usersTopic, err := topic.New("users.created")
	if err != nil {
		log.Fatalf("subscriber: topic: %v", err)
	}

	sub, err := client.Subscribe(ctx, usersTopic)
	if err != nil {
		log.Fatalf("subscriber: subscribe: %v", err)
	}

	fmt.Printf("Subscribed to topic: %s\n", usersTopic.String())
	fmt.Println("Waiting for messages... Press Ctrl+C to stop.")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for msg := range sub.MessageChannel() {
			fmt.Printf("Received on %s: data=%v\n", msg.Topic(), msg.Data())
		}
	}()

	<-quit
	fmt.Println("\nShutting down subscriber...")
}
