package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"

	"callbridge.local/callbridge-sms-go/internal/ami"
	"callbridge.local/callbridge-sms-go/internal/bridge"
	"callbridge.local/callbridge-sms-go/internal/config"
	"callbridge.local/callbridge-sms-go/internal/safelog"
)

func main() {
	version := flag.Bool("version", false, "print version and exit")
	check := flag.Bool("check", false, "validate configuration without changing runtime state")
	_ = flag.String("supervisor-tag", "", "process-discovery compatibility tag")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errorsText("unexpected positional argument"))
	}
	if *version {
		fmt.Println(config.Version)
		return
	}
	runtime.GOMAXPROCS(1)
	debug.SetMemoryLimit(64 << 20)
	cfg, err := config.Load()
	if err != nil {
		fatal(err)
	}
	if *check {
		if _, err := ami.New(cfg.AMIAddress, cfg.AMIUser, cfg.AMISecretFile); err != nil {
			fatal(err)
		}
		fmt.Printf("CONFIG_OK version=%s\n", config.Version)
		return
	}
	writer, err := safelog.Open(cfg.LogFile, 4<<20)
	if err != nil {
		fatal(err)
	}
	defer writer.Close()
	logger := log.New(writer, "callbridge-sms-go ", log.Ldate|log.Ltime|log.LUTC)
	service, err := bridge.New(cfg, logger)
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := service.Run(ctx); err != nil {
		logger.Printf("service stopped reason=%T", err)
		fatal(err)
	}
}

type errorsText string

func (e errorsText) Error() string { return string(e) }

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "callbridge-sms-go: %v\n", err)
	os.Exit(1)
}
