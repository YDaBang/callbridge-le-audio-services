# profile-policy-helper

Sets the Android connection policy for the HFP and LE Audio profiles on one
bonded device, so the phone stops preferring classic Bluetooth over LE Audio.

    status          read both policies, change nothing
    apply-both      allow HFP and allow LE Audio
    apply-le-only   forbid HFP, allow LE Audio

**The bridge does not need this.**  It is a diagnostic left in because it was
hard to write and there is no other way to read or set those policies.  Nothing
in `le-call-go` or `chan_mobile` calls it, and a working bridge never runs it.

Reach for it when a call connects and the audio is 8 or 16 kHz while every LE
Audio component looks healthy — that is a phone quietly preferring HFP.  `status`
will tell you, and `apply-le-only` removes the choice.

## Why it looks like this

`setConnectionPolicy` and `getConnectionPolicy` are hidden APIs.  There is no
`adb shell` command for them and no public SDK entry point, so the helper runs
as the shell UID under `app_process`, exempts itself from hidden-API
restrictions through `VMRuntime.setHiddenApiExemptions`, and synthesises a
system `Context` via `ActivityThread`/`ContextImpl` because there is no
Application to inherit one from.

Profile 22 is `BluetoothProfile.LE_AUDIO`, and policy 100 / 0 are
`CONNECTION_POLICY_ALLOWED` / `CONNECTION_POLICY_FORBIDDEN`.  They are written
as literals because the constants are not in the public SDK either.

The device is matched by **name**, and only when exactly one bonded device has
it — two matches abort rather than guess.  Pass the name as the second
argument; it defaults to `Callbridge-Asterisk`.

## Building and running

Compile against an `android.jar` from the platform you target, convert to dex,
push, and run:

    javac -cp <android.jar> -d classes src/dev/callbridge/bt/ProfilePolicyHelper.java
    d8 --output . classes/dev/callbridge/bt/*.class
    adb push classes.dex /data/local/tmp/pph.dex
    adb shell CLASSPATH=/data/local/tmp/pph.dex app_process / \
        dev.callbridge.bt.ProfilePolicyHelper status "My-Bridge-Name"

Every line of output is `key=value`, and the last is `result=PASS` or
`result=FAIL`.  Exit codes: 2 adapter off, 3 no unique bonded match, 4 proxy
request refused, 5 profile disconnected, 6 timeout, 7 policy did not take, 8
reflection failed.

No root is needed — shell UID is enough — but the phone has to be bonded to the
bridge already, and Bluetooth has to be on.

## Caveat

This reaches into hidden platform internals.  The reflective paths here work on
the Android version this was developed against; they are not a stable contract
and a platform update can move them.  Everything fails closed, so a break shows
up as a non-zero exit rather than a silently wrong policy.
