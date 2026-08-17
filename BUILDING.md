# Building the whole chain

Four layers, three repositories.  Each one is documented where it lives; this is
the order they have to go in and how to tell a layer is actually working before
you build the next one.

*한국어: [BUILDING.ko.md](BUILDING.ko.md)*

    1  BlueZ            bluez-leaudio-server-fixes          patch and install
    2  le-call-go       this repository                     creates two sockets
    3  chan_mobile      asterisk-chan-mobile-leaudio        connects to them
    4  configuration    chan_mobile.conf                    opt in per device

The order is not stylistic.  Step 3 connects to sockets that step 2 creates, and
step 1 decides whether a second call ever works.

## 1 — BlueZ

Apply the patches from [bluez-leaudio-server-fixes][bluez] and install the
result.  223 is the one you cannot skip: without it a Unicast Server that still
holds a transport reference never leaves `Releasing`, the phone's LE Audio
watchdog drops the group about 3.5 seconds later, and every call after the first
fails with nothing in any log.

**Check before continuing.**  The unit test the patch adds,
`BAP/USR/SCC/IO-RELEASE-01`, hangs without the fix and passes with it.

## 2 — le-call-go

    cd callbridge-sms-go     && go build ./...
    cd callbridge-le-call-go && go build ./...
    le-call-go -mode probe -device <bonded-address>

`probe` only reads: it inventories the BlueZ objects and does bounded GTBS reads.
Run it before `serve`.

**Check before continuing.**  `probe` finds exactly one GTBS service on the
bonded device.  Then start `serve` and confirm both sockets exist:

    /run/asterisk-leaudio/leaudio.sock
    /run/asterisk-leaudio/lecall.sock

If they are not there, stop here — the next step has nothing to connect to.

A killed process leaves the socket files behind and the next start refuses to
replace them.  Remove them only after confirming no owner.

## 3 — chan_mobile

Apply the patch and copy the new files as described in
[asterisk-chan-mobile-leaudio][chan], then build.

**Check before continuing.**  The nine media-lifecycle tests pass.  They need
only liblc3, so this check is independent of everything above.

Loading the module needs the adapter's voice setting applied *first*:

    hciconfig <dev> voice 0x0060
    asterisk -rx "module load chan_mobile"

Out of order, the load fails and the bridge stays down.

## 4 — Configuration

    audiotransport=le-canary
    callcontrol=le-gtbs

Both, per device.  Either one alone gives you a device that answers and carries
no audio.  Both default to `classic`, so a device you do not configure keeps
behaving exactly as it did before.

## The phone has to initiate

The broker advertises BAP/CAP Targeted Announcements only while the phone's LE
bearer is disconnected, and stops once it connects.  Nothing here calls out to
the phone.

That also means a restart of `le-call-go` would strand it: `bluetoothd` owns the
link, so the phone never sees the restart, keeps its group active, and stops
offering a CIS — a call that connects with no audio and no error anywhere.  The
broker handles this itself.  If LE is already connected at startup it drops the
bearer, waits for the property to actually go false, and rearms the
advertisement, so the phone observes the loss and rebuilds its state.  There is
nothing to do by hand.

## When a call connects but has no audio

1. **Only one of the two config lines is set.**  LE media with classic call
   control, or the reverse.
2. **BlueZ is unpatched** and the previous call left the ASE in `Releasing`, so
   this is the second call rather than the first.
3. **The phone is on classic.**  Check the negotiated codec, not the profile
   list.  `android/profile-policy-helper` can forbid HFP for that one device if
   it turns out to be choosing it.

None of these log an error.  That is the hard part.

[bluez]: https://github.com/YDaBang/bluez-leaudio-server-fixes
[chan]: https://github.com/YDaBang/asterisk-chan-mobile-leaudio
