package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/topic"
	"github.com/gophercast/gophercast/internal/transport"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:7650", "broker address")
	flag.Parse()

	ctx := context.Background()

	client, err := transport.Dial(ctx, *brokerAddr)
	if err != nil {
		log.Fatalf("publisher: dial: %v", err)
	}
	defer client.Close()

	usersTopic, err := topic.New("users.created")
	if err != nil {
		log.Fatalf("publisher: topic: %v", err)
	}

	fmt.Println("Publishing messages...")

	for i := 1; i <= 5; i++ {
		data := map[string]interface{}{
			"user_id": fmt.Sprintf("user-%d", i),
			"email":   fmt.Sprintf("user%d@example.com", i),
		}
		raw, _ := json.Marshal(data)
		msg := message.NewMessage(usersTopic, json.RawMessage(raw))

		delivered, dropped, err := client.Publish(ctx, msg)
		if err != nil {
			log.Printf("publish error: %v", err)
			continue
		}
		fmt.Printf("Published message %d (delivered=%d dropped=%d)\n", i, delivered, dropped)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("All messages published!")
}
