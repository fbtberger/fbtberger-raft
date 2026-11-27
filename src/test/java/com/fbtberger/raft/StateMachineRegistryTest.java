/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The registry decides which state machine a name addresses and how several
 * state machines share one snapshot. Both are things a cluster cannot repair
 * afterwards if they are wrong, so the layout is pinned down byte by byte.
 */
class StateMachineRegistryTest {

    @Test
    void theEmptyNameIsTheApplicationAndANameIsItsOwnStateMachine() {
        TextStateMachine app = new TextStateMachine("app");
        TextStateMachine values = new TextStateMachine("values");
        StateMachineRegistry registry = new StateMachineRegistry(app, Map.of("fbt.values", values));

        assertSame(app, registry.require(""));
        assertSame(app, registry.require(null));
        assertSame(app, registry.application());
        assertSame(values, registry.require("fbt.values"));
        assertEquals(List.of("fbt.values"), List.copyOf(registry.names()));
    }

    @Test
    void anUnknownNameIsRefusedAndSaysWhatIsRegistered() {
        StateMachineRegistry registry =
                new StateMachineRegistry(new TextStateMachine("app"), Map.of("a", new TextStateMachine("a")));

        UnknownStateMachineException e =
                assertThrows(UnknownStateMachineException.class, () -> registry.require("b"));

        assertEquals("b", e.stateMachine());
        assertTrue(e.getMessage().contains("[a]"), e.getMessage());
    }

    @Test
    void namesAreCheckedWhenTheyAreRegistered() {
        TextStateMachine sm = new TextStateMachine("x");
        for (String bad : List.of("", "with space", "ümlaut", "a/b", "x".repeat(65))) {
            assertThrows(IllegalArgumentException.class,
                    () -> new StateMachineRegistry(sm, Map.of(bad, sm)), bad);
            assertThrows(IllegalArgumentException.class, () -> new StateMachineRegistration(bad, sm), bad);
        }
        new StateMachineRegistration("fbt.values_1-a", sm);
        new StateMachineRegistration("x".repeat(64), sm);
    }

    @Test
    void twoRegistrationsWithOneNameAreRefused() {
        TextStateMachine sm = new TextStateMachine("x");
        List<StateMachineRegistration> twice =
                List.of(new StateMachineRegistration("a", sm), new StateMachineRegistration("a", sm));

        assertThrows(IllegalArgumentException.class, () -> StateMachineRegistry.of(sm, twice));
    }

    @Test
    void withoutNamedStateMachinesTheSnapshotIsTheApplicationsOwnBytes() {
        TextStateMachine app = new TextStateMachine("application state");
        StateMachineRegistry registry = StateMachineRegistry.of(app);

        assertArrayEquals(utf8("application state"), registry.takeSnapshot());
        assertArrayEquals(utf8("application state"), registry.prepareCowSnapshot().get());
    }

    @Test
    void withNamedStateMachinesTheSnapshotIsAContainerInNameOrder() {
        StateMachineRegistry registry = new StateMachineRegistry(new TextStateMachine("APP"),
                Map.of("b", new TextStateMachine("BB"), "a", new TextStateMachine("")));

        byte[] expected = {
                0x00, 0x46, 0x42, 0x54, 0x53, 0x4D, 0x53, 0x01,
                0, 0, 0, 0, 0, 3, 'A', 'P', 'P',
                0, 1, 'a', 0, 0, 0, 0,
                0, 1, 'b', 0, 0, 0, 2, 'B', 'B'};
        assertArrayEquals(expected, registry.takeSnapshot());
        assertArrayEquals(expected, registry.prepareCowSnapshot().get());
    }

    @Test
    void aSnapshotGivesEveryStateMachineItsOwnSectionBack() {
        TextStateMachine app = new TextStateMachine("orders");
        TextStateMachine values = new TextStateMachine("sequence=7");
        byte[] snapshot = new StateMachineRegistry(app, Map.of("fbt.values", values)).takeSnapshot();

        TextStateMachine app2 = new TextStateMachine("?");
        TextStateMachine values2 = new TextStateMachine("?");
        new StateMachineRegistry(app2, Map.of("fbt.values", values2)).restoreSnapshot(snapshot);

        assertEquals("orders", app2.state);
        assertEquals("sequence=7", values2.state);
    }

    @Test
    void theCapturedSnapshotDoesNotSeeLaterChanges() {
        TextStateMachine app = new TextStateMachine("before");
        TextStateMachine values = new TextStateMachine("1");
        StateMachineRegistry registry = new StateMachineRegistry(app, Map.of("v", values));

        Supplier<byte[]> captured = registry.prepareCowSnapshot();
        app.state = "after";
        values.state = "2";

        TextStateMachine app2 = new TextStateMachine("?");
        TextStateMachine values2 = new TextStateMachine("?");
        new StateMachineRegistry(app2, Map.of("v", values2)).restoreSnapshot(captured.get());
        assertEquals("before", app2.state);
        assertEquals("1", values2.state);
    }

    @Test
    void aSnapshotFromBeforeNamedStateMachinesGoesToTheApplicationAndResetsTheOthers() {
        TextStateMachine app = new TextStateMachine("?");
        TextStateMachine values = new TextStateMachine("left over");
        StateMachineRegistry registry = new StateMachineRegistry(app, Map.of("fbt.values", values));

        registry.restoreSnapshot(utf8("plain application snapshot"));

        assertEquals("plain application snapshot", app.state);
        assertEquals("", values.state);
    }

    @Test
    void aSectionForAStateMachineThatIsNotRegisteredFailsTheRestoreAndChangesNothing() {
        byte[] snapshot = new StateMachineRegistry(new TextStateMachine("orders"),
                Map.of("fbt.values", new TextStateMachine("7"))).takeSnapshot();
        TextStateMachine app = new TextStateMachine("untouched");

        UnknownStateMachineException e = assertThrows(UnknownStateMachineException.class,
                () -> StateMachineRegistry.of(app).restoreSnapshot(snapshot));

        assertEquals("fbt.values", e.stateMachine());
        assertEquals("untouched", app.state);
    }

    @Test
    void aContainerThatIsCutOffIsRefused() {
        byte[] snapshot = new StateMachineRegistry(new TextStateMachine("orders"),
                Map.of("v", new TextStateMachine("12345"))).takeSnapshot();
        StateMachineRegistry registry =
                new StateMachineRegistry(new TextStateMachine("?"), Map.of("v", new TextStateMachine("?")));

        for (int cut = 9; cut < snapshot.length; cut++) {
            byte[] truncated = Arrays.copyOf(snapshot, cut);
            if (cut == 8 + 2 + 4 + "orders".length()) {
                continue; // ends exactly after the application's section: a valid container
            }
            assertThrows(IllegalArgumentException.class, () -> registry.restoreSnapshot(truncated), "cut at " + cut);
        }
    }

    // ---- the same layout, as streams --------------------------------------

    @Test
    void theStreamedSnapshotIsByteForByteTheArrayOne() throws Exception {
        StateMachineRegistry plain = StateMachineRegistry.of(new TextStateMachine("orders"));
        StateMachineRegistry both = new StateMachineRegistry(new TextStateMachine("orders"),
                Map.of("values", new TextStateMachine("seven"), "audit", new TextStateMachine("")));

        assertArrayEquals(plain.takeSnapshot(), streamed(plain));
        assertArrayEquals(both.takeSnapshot(), streamed(both));

        java.io.ByteArrayOutputStream captured = new java.io.ByteArrayOutputStream();
        both.prepareSnapshotWriter().writeTo(captured);
        assertArrayEquals(both.takeSnapshot(), captured.toByteArray());
    }

    @Test
    void theCapturedStreamDoesNotSeeLaterChanges() throws Exception {
        TextStateMachine values = new TextStateMachine("seven");
        StateMachineRegistry registry =
                new StateMachineRegistry(new TextStateMachine("orders"), Map.of("values", values));
        byte[] before = registry.takeSnapshot();

        StateMachine.SnapshotWriter writer = registry.prepareSnapshotWriter();
        values.apply(utf8("eight"));
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        writer.writeTo(out);

        assertArrayEquals(before, out.toByteArray());
    }

    @Test
    void aStreamedSnapshotGivesEveryStateMachineItsOwnSectionBack() throws Exception {
        StateMachineRegistry source = new StateMachineRegistry(new TextStateMachine("orders"),
                Map.of("values", new TextStateMachine("seven"), "audit", new TextStateMachine("log")));
        TextStateMachine app = new TextStateMachine("?");
        TextStateMachine values = new TextStateMachine("?");
        TextStateMachine audit = new TextStateMachine("?");
        StateMachineRegistry target =
                new StateMachineRegistry(app, Map.of("values", values, "audit", audit));

        target.restoreSnapshot(new java.io.ByteArrayInputStream(source.takeSnapshot()));

        assertEquals("orders", app.state);
        assertEquals("seven", values.state);
        assertEquals("log", audit.state);
    }

    @Test
    void aStateMachineThatDoesNotReadItsSectionToTheEndDoesNotDisturbTheNext() throws Exception {
        StateMachineRegistry source = new StateMachineRegistry(new TextStateMachine("orders"),
                Map.of("values", new TextStateMachine("seven")));
        TextStateMachine values = new TextStateMachine("?");
        StateMachine lazy = new TextStateMachine("?") {
            @Override
            public void restoreSnapshot(java.io.InputStream in) throws java.io.IOException {
                state = String.valueOf((char) in.read());
            }
        };

        new StateMachineRegistry(lazy, Map.of("values", values))
                .restoreSnapshot(new java.io.ByteArrayInputStream(source.takeSnapshot()));

        assertEquals("seven", values.state);
    }

    @Test
    void aStreamedPlainSnapshotGoesToTheApplicationAndResetsTheOthers() throws Exception {
        TextStateMachine app = new TextStateMachine("?");
        TextStateMachine values = new TextStateMachine("old");
        StateMachineRegistry registry = new StateMachineRegistry(app, Map.of("values", values));

        registry.restoreSnapshot(new java.io.ByteArrayInputStream(utf8("orders from before")));

        assertEquals("orders from before", app.state);
        assertEquals("", values.state);

        // Shorter than the magic: still the application's own bytes.
        registry.restoreSnapshot(new java.io.ByteArrayInputStream(utf8("x")));
        assertEquals("x", app.state);
    }

    @Test
    void aStreamedSectionWithoutAStateMachineOrCutOffFailsTheRestore() {
        byte[] container = new StateMachineRegistry(new TextStateMachine("orders"),
                Map.of("values", new TextStateMachine("seven"))).takeSnapshot();

        assertThrows(UnknownStateMachineException.class,
                () -> StateMachineRegistry.of(new TextStateMachine("?"))
                        .restoreSnapshot(new java.io.ByteArrayInputStream(container)));
        // Where the application's section ends, a shorter container is a whole one
        // that merely lacks the other section; everywhere else it is cut off.
        int sectionEnd = StateMachineRegistry.SNAPSHOT_MAGIC.length + 2 + 4 + "orders".length();
        for (int cut = StateMachineRegistry.SNAPSHOT_MAGIC.length + 1; cut < container.length; cut++) {
            if (cut == sectionEnd) {
                continue;
            }
            byte[] shortened = java.util.Arrays.copyOf(container, cut);
            StateMachineRegistry registry = new StateMachineRegistry(new TextStateMachine("?"),
                    Map.of("values", new TextStateMachine("?")));
            assertThrows(IllegalArgumentException.class,
                    () -> registry.restoreSnapshot(new java.io.ByteArrayInputStream(shortened)),
                    "cut at " + cut);
        }
    }

    @Test
    void aWitnessRegistrySendsEveryNameToTheOneStateMachineThatKeepsNothing() throws Exception {
        WitnessStateMachine nothing = new WitnessStateMachine();
        StateMachineRegistry witness = StateMachineRegistry.forWitness(nothing);

        assertSame(nothing, witness.require(""));
        assertSame(nothing, witness.require("values"));
        assertTrue(witness.names().isEmpty());
        assertEquals(0, streamed(witness).length);
        // A data node's container handed to a witness is left alone, not refused.
        witness.restoreSnapshot(new java.io.ByteArrayInputStream(new StateMachineRegistry(
                new TextStateMachine("orders"), Map.of("values", new TextStateMachine("seven"))).takeSnapshot()));
    }

    private static byte[] streamed(StateMachineRegistry registry) throws java.io.IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        registry.writeSnapshot(out);
        return out.toByteArray();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Its whole state is one string, and its snapshot is that string. */
    static class TextStateMachine implements StateMachine {
        volatile String state;

        TextStateMachine(String state) {
            this.state = state;
        }

        @Override
        public byte[] apply(byte[] command) {
            state = new String(command, StandardCharsets.UTF_8);
            return command;
        }

        @Override
        public byte[] read(byte[] query) {
            return utf8(state);
        }

        @Override
        public byte[] takeSnapshot() {
            return utf8(state);
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
            state = new String(snapshot, StandardCharsets.UTF_8);
        }
    }
}
