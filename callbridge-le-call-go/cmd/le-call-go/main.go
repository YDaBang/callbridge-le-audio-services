package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"callbridge.local/callbridge-sms-go/internal/bluez"
	"callbridge.local/callbridge-sms-go/lecall/internal/control"
	"callbridge.local/callbridge-sms-go/lecall/internal/gtbs"
	"callbridge.local/callbridge-sms-go/lecall/internal/protocol"
)

const version = "0.1.0-v35"

type options struct {
	mode               string
	adapter            string
	device             string
	mediaSocket        string
	controlSocket      string
	peerUID            int
	mediaStartupLease  time.Duration
	mediaProgressLease time.Duration
	normalReleaseWait  time.Duration
	showVersion        bool
}

func main() {
	var configured options
	flag.StringVar(&configured.mode, "mode", "serve", "serve or probe")
	flag.StringVar(&configured.adapter, "adapter", "hci0", "BlueZ adapter")
	flag.StringVar(&configured.device, "device", "", "expected phone Bluetooth address")
	flag.StringVar(&configured.mediaSocket, "media-socket", "/run/asterisk-leaudio/leaudio.sock", "LC3 media handoff socket")
	flag.StringVar(&configured.controlSocket, "control-socket", "/run/asterisk-leaudio/lecall.sock", "LE call-control socket")
	flag.IntVar(&configured.peerUID, "peer-uid", 0, "required Asterisk peer UID")
	flag.DurationVar(&configured.mediaStartupLease, "media-startup-lease", 60*time.Second,
		"maximum time to hold a call before its first real LC3 media progress")
	flag.DurationVar(&configured.mediaProgressLease, "media-progress-lease", 5*time.Second,
		"maximum no-progress interval after real LC3 media has started")
	flag.DurationVar(&configured.normalReleaseWait, "normal-release-wait", 0,
		"measured bounded wait after normal media progress stops; required for serve")
	flag.BoolVar(&configured.showVersion, "version", false, "print version")
	flag.Parse()
	if configured.showVersion {
		fmt.Println(version)
		return
	}
	if configured.device == "" {
		log.Fatal("-device is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if configured.mode == "probe" {
		if err := runProbe(ctx, configured); err != nil {
			log.Fatal(err)
		}
		return
	}
	if configured.mode != "serve" {
		log.Fatal("invalid mode")
	}
	if err := runServe(ctx, configured); err != nil {
		log.Fatal(err)
	}
}

func runProbe(parent context.Context, configured options) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	report, err := gtbs.Probe(ctx, configured.adapter, configured.device)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func runServe(parent context.Context, configured options) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	logger := log.New(os.Stderr, "le-call-go: ", log.Ldate|log.Ltime|log.LUTC)
	store := gtbs.NewStore()
	var controlServer *control.Server
	var mediaBroker *bluez.Broker
	var droppedForToken uint64
	client, err := gtbs.NewClient(configured.adapter, configured.device, store, logger, gtbs.Callbacks{
		Snapshot: func(snapshot gtbs.Snapshot) {
			if controlServer != nil {
				controlServer.BroadcastSnapshot(snapshot)
			}
			// Hand a handset-answered call back to the phone: hold the
			// advertisement so it cannot reconnect, then drop the link.
			// Dropping once per token, because the phone re-offers the
			// stream and every snapshot would otherwise drop again.
			if mediaBroker == nil {
				return
			}
			token := store.HandsetAnswered()
			mediaBroker.HoldAdvertisement(token != 0)
			if token == 0 {
				droppedForToken = 0
				return
			}
			if token != droppedForToken {
				droppedForToken = token
				broker := mediaBroker
				go broker.DropLEBearer("handset_answered_call")
			}
		},
		Ready: func(ready bool) {
			if controlServer != nil {
				controlServer.SetReady(ready)
			}
		},
		Result: func(opcode, index, result byte, token uint64) {
			if controlServer != nil {
				controlServer.BroadcastResult(opcode, index, result, token)
			}
		},
	})
	if err != nil {
		return err
	}
	mediaBroker, err = bluez.NewBroker(configured.adapter, configured.device,
		configured.mediaSocket, configured.peerUID, logger)
	if err != nil {
		return err
	}
	if err := mediaBroker.ConfigureMediaLeases(configured.mediaStartupLease,
		configured.mediaProgressLease); err != nil {
		return err
	}
	if err := mediaBroker.ConfigureNormalReleaseWait(configured.normalReleaseWait); err != nil {
		return err
	}
	if err := mediaBroker.ConfigureCallControl(store.CurrentCallToken, true); err != nil {
		return err
	}
	command := func(ctx context.Context, message protocol.Message) error {
		if message.Code == protocol.OpcodeTerminate {
			mediaBroker.ReleaseCallToken(message.Token)
		}
		if message.Code == protocol.OpcodeAccept {
			// Claim before the write: the phone can notify Active before
			// this call returns, and an unclaimed Active is treated as a
			// handset answer.
			store.ClaimCall(message.Index, message.Token)
		}
		return client.Command(ctx, message)
	}
	controlServer, err = control.NewServer(configured.controlSocket, configured.peerUID,
		client.Device(), command, store.Snapshot)
	if err != nil {
		return err
	}
	errorsSeen := make(chan error, 3)
	go func() { errorsSeen <- controlServer.Run(ctx) }()
	go func() { errorsSeen <- client.Run(ctx) }()
	go func() { errorsSeen <- mediaBroker.Run(ctx) }()
	logger.Printf("started version=%s adapter=%s sms=false call_control=gtbs media=lc3", version, configured.adapter)
	first := <-errorsSeen
	cancel()
	for completed := 1; completed < 3; completed++ {
		select {
		case err := <-errorsSeen:
			if first == nil && err != nil {
				first = err
			}
		case <-time.After(3 * time.Second):
			if first == nil {
				first = errors.New("LE call Go shutdown timed out")
			}
			return first
		}
	}
	return first
}
