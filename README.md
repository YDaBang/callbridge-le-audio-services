# Call Bridge LE Audio services

The two processes that sit between BlueZ and Asterisk in a Bluetooth LE Audio
phone bridge.  Together with [asterisk-chan-mobile-leaudio][chan] and
[bluez-leaudio-server-fixes][bluez] this is the whole chain: a phone's LE Audio
unicast stream and its call control reach Asterisk as a SIP trunk.

*한국어: [README.ko.md](README.ko.md)*

**Setting the whole chain up: [BUILDING.md](BUILDING.md).**

    callbridge-le-call-go/   LE Audio + cellular call control
    callbridge-sms-go/       MAP/MNS SMS bridge; also holds internal/bluez,
                             the BAP endpoint and LC3 handoff code both use

## Why these exist

`bluetoothd` owns the LE link, the ASE state machine and the CIS.  Asterisk
cannot reach any of that without reimplementing BAP, so `chan_mobile` does not
open the stream — it connects to two unix sockets and waits for someone else to
do the work:

    /run/asterisk-leaudio/leaudio.sock    the paired LC3 ISO descriptors
    /run/asterisk-leaudio/lecall.sock     call snapshots, commands, results

`le-call-go` is what listens on them.  It registers the BAP/CAP/TMAP endpoint
surface over D-Bus, subscribes to GTBS Call State and Call Control Point, and
passes the ISO file descriptors across with `SCM_RIGHTS` once a call is real.

That handoff is the piece the module cannot supply for itself, so without this
repository `asterisk-chan-mobile-leaudio` builds and tests but cannot carry a
call.

## le-call-go

    le-call-go -mode probe -device <bonded-address>    inventory and bounded GTBS reads
    le-call-go -mode serve -device <bonded-address>    register endpoints and serve

Both sockets are root-only `SOCK_SEQPACKET` and accept exactly one configured
peer UID.  Every media handoff has to carry the token of exactly one live call;
ambiguous multi-call state fails closed rather than guessing, because the
process never infers a cellular answer or hangup from media alone.

Three behaviours are worth knowing before you debug something:

**A call answered on the handset is given back to the phone.**  Android decides
where call audio goes when an LE Audio device is connected, and it does not
reconsider: refuse the stream and it re-offers the same stream while nothing
plays anywhere.  So when GTBS reports a call leaving `Incoming` without an
Accept from this side, the broker holds the advertisement and then drops the LE
link, and the phone falls back to its own earpiece.

Order matters.  Dropping before holding rearms the advertisement on the
disconnect signal, the phone answers it about five seconds later, and the bridge
takes the audio again — which is exactly the bug.  Releasing the hold has to
rearm explicitly, because by then the disconnect signal is long past.

Outgoing calls are excluded by the GTBS outgoing flag: this side originated
them, so no Accept is expected and none is required.


**Advertising is targeted and conditional.**  BAP and CAP Targeted Announcements
run only while the phone's LE bearer is disconnected.  The design has the bonded
phone initiate; the broker stops advertising on `Bearer.LE1 Connected=true` and
rearms on `false`.  There is no outbound connect.

**GTBS session validity is bound to that same property.**  A disconnect
immediately publishes readiness false and clears notification and command state,
so a reconnect performs a fresh inventory rather than reusing a stale GATT
session.  Signals from child service or transport paths cannot invalidate it.

`le-sink-nudge` is a one-shot helper for prodding a sink that has gone quiet.

## sms-go

MAP/MNS over RFCOMM: it subscribes to the phone's message notification service,
pulls new messages, and forwards them into Asterisk over AMI.  Outbound goes the
other way through a small HTTP endpoint.

It is configured entirely from the environment — `BT_DEVICE`, `AMI_HOST`,
`AMI_USER`, `AMI_SECRET_FILE`, `MAS_CHANNEL`, `MNS_CHANNEL`, `OUTBOX_LISTEN`,
`SMS_RECIPIENTS` and the rest are listed in `internal/config/config.go`.  The
RFCOMM channel numbers are not fixed across phones; discover them rather than
assuming.

SMS is independent of the LE Audio path.  If you only want the phone bridge, you
still need this module, because `internal/bluez` lives here — but you do not
need to run `sms-bridge`.

## Building

    cd callbridge-sms-go     && go build ./...
    cd callbridge-le-call-go && go build ./...

The module paths are `callbridge.local/...` on purpose.  These are built from a
checkout — `callbridge-le-call-go` reaches its sibling through a `replace`
directive — not fetched with `go get`, so the paths are deliberately not
resolvable URLs.

## Scope and honesty

This is the code that runs one bridge, published so the chain is complete.  It
was developed against one Android phone and one adapter, and the LE path assumes
the 32 kHz / 10 ms / 80-octet LC3 configuration that phone offers.

Identifiers in this copy are placeholders: the handset address in the tests, the
advertised local name, the AMI account, and every phone number.  Numbers use the
`010-0000-0000` block, which is not allocatable, so nothing here can dial a real
subscriber.

GTBS carries no DTMF.  A command that gets no Control Point result and no
following Call State update is left to Asterisk's call timeout; the process does
not guess.

## License

GPL-2.0-only.  See `LICENSE`.

[chan]: https://github.com/YDaBang/asterisk-chan-mobile-leaudio
[bluez]: https://github.com/YDaBang/bluez-leaudio-server-fixes
