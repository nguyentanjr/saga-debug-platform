package com.example.inventory.admin;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Demo fault injection: when armed, the next payment-completed message makes this process
 * exit abruptly, as a real crash in the middle of processing would.
 *
 * <p>The exit happens before the listener returns, so the message's offset is not committed
 * and the restarted process (compose {@code restart: on-failure}) handles it again. The
 * container runtime records a real {@code die} and {@code start}, which is the evidence the
 * observer's R23 reads. Disarmed on use: one crash per arming.
 */
@Component
public class CrashSwitch {

    private final AtomicBoolean armed = new AtomicBoolean(false);
    private final Runnable crash;

    public CrashSwitch() {
        this(() -> Runtime.getRuntime().halt(1));
    }

    CrashSwitch(Runnable crash) {
        this.crash = crash;
    }

    public void arm() { armed.set(true); }

    public boolean isArmed() { return armed.get(); }

    /** Crashes the process if armed; otherwise returns and processing continues. */
    public void crashIfArmed() {
        if (armed.compareAndSet(true, false)) {
            crash.run();
        }
    }
}
