package com.example.inventory.admin;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** The demo crash fires once per arming and never unarmed. */
class CrashSwitchTest {

    private final AtomicInteger crashes = new AtomicInteger();
    private final CrashSwitch crashSwitch = new CrashSwitch(crashes::incrementAndGet);

    @Test
    void unarmedNeverCrashes() {
        crashSwitch.crashIfArmed();
        crashSwitch.crashIfArmed();
        assertThat(crashes).hasValue(0);
    }

    @Test
    void armedCrashesOnTheNextMessageOnly() {
        crashSwitch.arm();

        crashSwitch.crashIfArmed();
        crashSwitch.crashIfArmed();

        assertThat(crashes).hasValue(1);
        assertThat(crashSwitch.isArmed()).isFalse();
    }
}
