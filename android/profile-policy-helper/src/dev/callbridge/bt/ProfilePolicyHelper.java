package dev.callbridge.bt;

import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothProfile;
import android.content.Context;
import android.os.Handler;
import android.os.Looper;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ProfilePolicyHelper {
    private static final String DEFAULT_TARGET_NAME = "Callbridge-Asterisk";
    private static final int PROFILE_HEADSET = 1;
    private static final int PROFILE_LE_AUDIO = 22;
    private static final int POLICY_FORBIDDEN = 0;
    private static final int POLICY_ALLOWED = 100;
    private static final long TIMEOUT_MS = 15_000L;

    private final String mode;
    private final BluetoothAdapter adapter;
    private final BluetoothDevice target;
    private final Handler handler;
    private final Map<Integer, BluetoothProfile> proxies = new HashMap<>();
    private boolean finished;

    private ProfilePolicyHelper(String mode, BluetoothAdapter adapter,
            BluetoothDevice target) {
        this.mode = mode;
        this.adapter = adapter;
        this.target = target;
        this.handler = new Handler(Looper.getMainLooper());
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2
                || !("status".equals(args[0])
                || "apply-both".equals(args[0])
                || "apply-le-only".equals(args[0]))) {
            System.err.println(
                    "usage=status|apply-both|apply-le-only [bonded-device-name]");
            System.exit(64);
        }
        String targetName = args.length == 2 ? args[1] : DEFAULT_TARGET_NAME;

        System.out.println("bootstrap=begin");
        exemptBluetoothHiddenApis();
        System.out.println("bootstrap=hidden_api_ready");
        Context context = createSystemContext();
        System.out.println("bootstrap=context_ready");
        BluetoothAdapter adapter = BluetoothAdapter.getDefaultAdapter();
        if (adapter == null || !adapter.isEnabled()) {
            System.err.println("adapter_ready=false");
            System.exit(2);
        }

        BluetoothDevice target = findExactTarget(adapter.getBondedDevices(), targetName);
        if (target == null) {
            System.err.println("target_exact_match=false name=" + targetName);
            System.exit(3);
        }

        ProfilePolicyHelper helper = new ProfilePolicyHelper(args[0], adapter, target);
        helper.start(context);
        Looper.loop();
    }

    private static void exemptBluetoothHiddenApis() {
        try {
            Class<?> vmRuntime = Class.forName("dalvik.system.VMRuntime");
            Method getRuntime = vmRuntime.getDeclaredMethod("getRuntime");
            Method setExemptions = vmRuntime.getDeclaredMethod(
                    "setHiddenApiExemptions", String[].class);
            Object runtime = getRuntime.invoke(null);
            setExemptions.invoke(runtime, (Object) new String[] {
                    "Landroid/app/ActivityThread;",
                    "Landroid/bluetooth/"
            });
        } catch (ReflectiveOperationException ignored) {
            // Shell UID may already be exempt. The exact API calls below remain fail-closed.
        }
    }

    private static Context createSystemContext() throws Exception {
        Class<?> activityThread = Class.forName("android.app.ActivityThread");
        java.lang.reflect.Constructor<?> constructor =
                activityThread.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object thread = constructor.newInstance();

        Class<?> contextImpl = Class.forName("android.app.ContextImpl");
        Method createSystemContext = contextImpl.getDeclaredMethod(
                "createSystemContext", activityThread);
        createSystemContext.setAccessible(true);
        return (Context) createSystemContext.invoke(null, thread);
    }

    private static BluetoothDevice findExactTarget(Set<BluetoothDevice> bonded,
            String targetName) {
        BluetoothDevice match = null;
        int matches = 0;
        for (BluetoothDevice device : bonded) {
            if (targetName.equals(device.getName())) {
                match = device;
                matches++;
            }
        }
        return matches == 1 ? match : null;
    }

    private void start(Context context) {
        System.out.println("mode=" + mode);
        System.out.println("target_exact_match=true");

        BluetoothProfile.ServiceListener listener = new BluetoothProfile.ServiceListener() {
            @Override
            public void onServiceConnected(int profile, BluetoothProfile proxy) {
                if (finished || (profile != PROFILE_HEADSET && profile != PROFILE_LE_AUDIO)) {
                    return;
                }
                proxies.put(profile, proxy);
                if (proxies.size() == 2) {
                    runTransaction();
                }
            }

            @Override
            public void onServiceDisconnected(int profile) {
                if (!finished) {
                    fail("profile_disconnected=" + profile, 5);
                }
            }
        };

        boolean headsetRequested = adapter.getProfileProxy(
                context, listener, PROFILE_HEADSET);
        boolean leRequested = adapter.getProfileProxy(
                context, listener, PROFILE_LE_AUDIO);
        System.out.println("headset_proxy_requested=" + headsetRequested);
        System.out.println("le_audio_proxy_requested=" + leRequested);
        if (!headsetRequested || !leRequested) {
            fail("proxy_request_failed=true", 4);
            return;
        }

        handler.postDelayed(new Runnable() {
            @Override
            public void run() {
                fail("timeout=true", 6);
            }
        }, TIMEOUT_MS);
    }

    private void runTransaction() {
        try {
            BluetoothProfile headset = proxies.get(PROFILE_HEADSET);
            BluetoothProfile leAudio = proxies.get(PROFILE_LE_AUDIO);
            int headsetBefore = getPolicy(headset);
            int leBefore = getPolicy(leAudio);
            System.out.println("headset_policy_before=" + headsetBefore);
            System.out.println("le_audio_policy_before=" + leBefore);

            if ("apply-both".equals(mode)) {
                boolean leResult = setPolicy(leAudio, POLICY_ALLOWED);
                boolean headsetResult = setPolicy(headset, POLICY_ALLOWED);
                System.out.println("le_audio_set_allowed=" + leResult);
                System.out.println("headset_set_allowed=" + headsetResult);
            } else if ("apply-le-only".equals(mode)) {
                boolean headsetResult = setPolicy(headset, POLICY_FORBIDDEN);
                boolean leResult = setPolicy(leAudio, POLICY_ALLOWED);
                System.out.println("headset_set_forbidden=" + headsetResult);
                System.out.println("le_audio_set_allowed=" + leResult);
            }

            int headsetAfter = getPolicy(headset);
            int leAfter = getPolicy(leAudio);
            System.out.println("headset_policy_after=" + headsetAfter);
            System.out.println("le_audio_policy_after=" + leAfter);

            boolean expected = "status".equals(mode)
                    || ("apply-both".equals(mode)
                    && headsetAfter == POLICY_ALLOWED
                    && leAfter == POLICY_ALLOWED)
                    || ("apply-le-only".equals(mode)
                    && headsetAfter == POLICY_FORBIDDEN
                    && leAfter == POLICY_ALLOWED);
            finish(expected ? 0 : 7);
        } catch (ReflectiveOperationException | RuntimeException e) {
            fail("transaction_error=" + e.getClass().getSimpleName(), 8);
        }
    }

    private int getPolicy(BluetoothProfile proxy) throws ReflectiveOperationException {
        Method method = proxy.getClass().getMethod(
                "getConnectionPolicy", BluetoothDevice.class);
        return (Integer) method.invoke(proxy, target);
    }

    private boolean setPolicy(BluetoothProfile proxy, int policy)
            throws ReflectiveOperationException {
        Method method = proxy.getClass().getMethod(
                "setConnectionPolicy", BluetoothDevice.class, int.class);
        return (Boolean) method.invoke(proxy, target, policy);
    }

    private void fail(String message, int code) {
        if (!finished) {
            System.err.println(message);
            finish(code);
        }
    }

    private void finish(int code) {
        if (finished) {
            return;
        }
        finished = true;
        handler.removeCallbacksAndMessages(null);
        for (Map.Entry<Integer, BluetoothProfile> entry : proxies.entrySet()) {
            adapter.closeProfileProxy(entry.getKey(), entry.getValue());
        }
        System.out.println("result=" + (code == 0 ? "PASS" : "FAIL"));
        System.out.flush();
        System.err.flush();
        System.exit(code);
    }
}
