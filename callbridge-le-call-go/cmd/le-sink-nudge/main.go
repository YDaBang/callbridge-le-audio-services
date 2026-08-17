package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"callbridge.local/callbridge-sms-go/internal/bluez"
)

const version = "0.1.0-v4"

func main() {
	var adapter string
	var device string
	var timeout time.Duration
	var showVersion bool
	flag.StringVar(&adapter, "adapter", "hci0", "BlueZ adapter")
	flag.StringVar(&device, "device", "", "exact bonded phone Bluetooth address")
	flag.DurationVar(&timeout, "timeout", 10*time.Second, "total one-shot timeout")
	flag.BoolVar(&showVersion, "version", false, "print version")
	flag.Parse()
	if showVersion {
		fmt.Println(version)
		return
	}
	if device == "" || timeout <= 0 || timeout > 30*time.Second {
		log.Fatal("-device is required and -timeout must be greater than zero and at most 30s")
	}

	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	logger := log.New(os.Stderr, "le-sink-nudge: ", log.Ldate|log.Ltime|log.LUTC)
	if err := bluez.SinkNudge(ctx, adapter, device, logger); err != nil {
		log.Fatal(err)
	}
}
