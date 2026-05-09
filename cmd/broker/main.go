package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gophercast/gophercast/internal/domain/broker"
	"github.com/gophercast/gophercast/internal/transport"
)

func main() {
	addr := flag.String("addr", ":7650", "TCP address to listen on")
	flag.Parse()

	b := broker.NewBroker()
	defer b.Close()

	srv := transport.NewServer(b)
	actual, err := srv.Listen(*addr)
	if err != nil {
		log.Fatalf("broker: listen: %v", err)
	}
	fmt.Printf("GopherCast broker listening on %s\n", actual)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\nShutting down broker...")
	srv.Close()
}
