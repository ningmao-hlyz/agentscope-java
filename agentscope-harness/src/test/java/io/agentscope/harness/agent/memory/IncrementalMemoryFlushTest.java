/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.harness.agent.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.harness.agent.filesystem.AbstractFilesystem;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.filesystem.BakedContextFilesystem;
import io.agentscope.harness.agent.filesystem.CompositeFilesystem;
import io.agentscope.harness.agent.filesystem.OverlayFilesystem;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.filesystem.remote.RemoteFilesystem;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.filesystem.remote.store.StoreItem;
import io.agentscope.harness.agent.filesystem.sandbox.SandboxBackedFilesystem;
import io.agentscope.harness.agent.memory.compaction.ConversationCompactor;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;

class IncrementalMemoryFlushTest {
    @TempDir Path workspace;
    private final RuntimeContext rc =
            RuntimeContext.builder().userId("alice").sessionId("session").build();

    private static Msg msg(String id, String text) {
        return Msg.builder()
                .id(id)
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    private static ChatResponse response(String text) {
        return ChatResponse.builder()
                .content(List.of(TextBlock.builder().text(text).build()))
                .finishReason("stop")
                .build();
    }

    private static Model model(List<String> prompts, String output) {
        Model model = mock(Model.class);
        when(model.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            List<Msg> input = invocation.getArgument(0);
                            prompts.add(input.get(1).getTextContent());
                            return Flux.just(response(output));
                        });
        return model;
    }

    private void flush(WorkspaceManager manager, Model model, List<Msg> messages) {
        new MemoryFlushManager(manager, model)
                .flushMemories(rc, messages)
                .block(Duration.ofSeconds(15));
    }

    private static String source(String prompt) {
        return prompt.substring(prompt.indexOf("Extract NEW memories"));
    }

    private String dailyPath() {
        return "memory/" + LocalDate.now() + ".md";
    }

    @Test
    void recreatedInstancesAndResubscriptionOnlyExtractNewMessages() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "NO_REPLY");
        Msg a = msg("a", "old-source");
        Msg b = msg("b", "new-source");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            var first = new MemoryFlushManager(manager, model).flushMemories(rc, List.of(a));
            first.block();
            first.block();
        }
        try (WorkspaceManager recreated = new WorkspaceManager(workspace)) {
            flush(recreated, model, List.of(a, b));
            flush(recreated, model, List.of(a)); // Late snapshot cannot rewind progress.
        }
        assertEquals(2, prompts.size());
        assertFalse(source(prompts.get(1)).contains("old-source"));
        assertTrue(source(prompts.get(1)).contains("new-source"));
    }

    @Test
    void compactionPrefixAndTailShareReceiptsWithoutArrayOffsets() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "NO_REPLY");
        Msg a = msg("a", "first");
        Msg b = msg("b", "second");
        Msg c = msg("c", "after-compaction");
        Msg summary =
                Msg.builder()
                        .id("summary")
                        .role(MsgRole.USER)
                        .name(ConversationCompactor.SUMMARY_MSG_NAME)
                        .textContent("do not extract summary")
                        .build();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, List.of(a));
            flush(manager, model, List.of(summary, b, c));
            flush(manager, model, List.of(a, b, c)); // Replay native history: nothing new.
        }
        assertEquals(2, prompts.size());
        assertFalse(source(prompts.get(1)).contains("do not extract summary"));
        assertTrue(source(prompts.get(1)).contains("second"));
    }

    @Test
    void repeatedContentWithDistinctMessageIdsIsNotMistakenForAnOldMessage() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, List.of(msg("a", "same content")));
            flush(manager, model, List.of(msg("b", "same content")));
        }
        assertEquals(2, prompts.size());
    }

    @Test
    void duplicateMessageIdsWithinOneSnapshotOnlyEnterTheModelOnce() {
        List<String> prompts = new ArrayList<>();
        Msg source = msg("duplicate", "unique-source-sentinel");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model(prompts, "NO_REPLY"), List.of(source, source));
        }
        assertEquals(1, source(prompts.get(0)).split("unique-source-sentinel", -1).length - 1);
    }

    @Test
    void deduplicationKeepsMultilineFactsTogether() {
        String oldFact = "- deployment\n  region: east";
        String newFact = "- deployment\n  region: west\n  - fallback: north";
        Model model = model(new ArrayList<>(), oldFact + "\n" + newFact + "\n" + newFact);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            manager.appendMemoryFileUtf8(rc, dailyPath(), oldFact + "\n");
            flush(manager, model, List.of(msg("multiline", "new deployment")));
            String daily = manager.atomicMemoryFile(rc, dailyPath()).read();
            assertEquals(1, daily.split("region: east", -1).length - 1);
            assertEquals(1, daily.split("region: west", -1).length - 1);
            assertEquals(1, daily.split("fallback: north", -1).length - 1);
            assertEquals(2, daily.split("- deployment", -1).length - 1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void overlayAppendsPreserveSeededMemoryAndLedger(boolean remote) {
        AbstractFilesystem upper =
                remote
                        ? new RemoteFilesystem(new InMemoryStore())
                        : new LocalFilesystem(workspace.resolve("upper"));
        AbstractFilesystem lower =
                remote
                        ? new RemoteFilesystem(new InMemoryStore())
                        : new LocalFilesystem(workspace.resolve("lower"));
        lower.atomicTextFile(rc, "MEMORY.md", true).compareAndSet(null, "- shared memory\n");
        lower.atomicTextFile(rc, dailyPath(), true).compareAndSet(null, "- shared ledger\n");
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new OverlayFilesystem(upper, lower), null)) {
            manager.appendMemoryFileUtf8(rc, "MEMORY.md", "- user memory\n");
            flush(
                    manager,
                    model(new ArrayList<>(), "- new ledger"),
                    List.of(msg("overlay", "source")));
            assertTrue(manager.readMemoryFileUtf8(rc, "MEMORY.md").contains("shared memory"));
            assertTrue(manager.readMemoryFileUtf8(rc, "MEMORY.md").contains("user memory"));
            assertTrue(manager.readMemoryFileUtf8(rc, dailyPath()).contains("shared ledger"));
            assertTrue(manager.readMemoryFileUtf8(rc, dailyPath()).contains("new ledger"));
            assertFalse(lower.atomicTextFile(rc, dailyPath(), true).read().contains("new ledger"));
        }
    }

    @Test
    void remoteAtomicUpdatesPreserveCreationTimeAndAreVisibleByRelativePath() {
        InMemoryStore store = new InMemoryStore();
        RemoteFilesystem filesystem = new RemoteFilesystem(store);
        AtomicTextFile file = filesystem.atomicTextFile(rc, "MEMORY.md", true);
        String created = "2020-01-01T00:00:00Z";
        store.put(
                List.of("filesystem"),
                "MEMORY.md",
                Map.of("content", "- first\n", "encoding", "utf-8", "created_at", created));
        assertTrue(file.compareAndSet("- first\n", "- updated\n"));
        assertEquals(
                created, store.get(List.of("filesystem"), "MEMORY.md").value().get("created_at"));
        assertTrue(filesystem.exists(rc, "MEMORY.md"));
    }

    @Test
    void remoteUpdateCannotOverwriteRecreatedContentWithReusedVersion() {
        AtomicBoolean recreate = new AtomicBoolean();
        InMemoryStore store =
                new InMemoryStore() {
                    @Override
                    public StoreItem get(List<String> namespace, String key) {
                        StoreItem observed = super.get(namespace, key);
                        if (observed != null && recreate.getAndSet(false)) {
                            assertTrue(super.deleteIfUnchanged(namespace, key, observed));
                            super.put(
                                    namespace,
                                    key,
                                    Map.of("content", "new fact", "encoding", "utf-8"));
                            assertEquals(observed.version(), super.get(namespace, key).version());
                        }
                        return observed;
                    }
                };
        AtomicTextFile file = new RemoteFilesystem(store).atomicTextFile(rc, dailyPath(), false);
        assertTrue(file.compareAndSet(null, "old fact"));
        recreate.set(true);
        assertFalse(file.compareAndSet("old fact", "old fact + stale append"));
        assertEquals("new fact", file.read());
        assertTrue(file.compareAndSet("new fact", "new fact + retry"));
    }

    @Test
    void overlayDoesNotInheritConsumptionReceiptsFromSharedLayer() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "NO_REPLY");
        Path lowerPath = workspace.resolve("lower");
        List<Msg> input = List.of(msg("shared-source", "source"));
        try (WorkspaceManager lower = new WorkspaceManager(lowerPath)) {
            flush(lower, model, input);
        }
        try (WorkspaceManager overlay =
                new WorkspaceManager(
                        workspace,
                        new OverlayFilesystem(
                                new LocalFilesystem(workspace.resolve("upper")),
                                new LocalFilesystem(lowerPath)),
                        null)) {
            flush(overlay, model, input);
        }
        assertEquals(2, prompts.size());
    }

    @Test
    void bakedWorkspaceRetainsAtomicUpdatesAndItsBoundIdentity() {
        InMemoryStore store = new InMemoryStore();
        RemoteFilesystem remote =
                new RemoteFilesystem(store, context -> List.of(context.getUserId()));
        BakedContextFilesystem bound = new BakedContextFilesystem(remote, rc);
        RuntimeContext caller = RuntimeContext.builder().userId("bob").build();
        AtomicTextFile file = bound.atomicTextFile(caller, "MEMORY.md", true);

        assertTrue(file.compareAndSet(null, "- alice memory"));
        assertFalse(file.compareAndSet(null, "- overwrite"));
        assertEquals("- alice memory", remote.atomicTextFile(rc, "MEMORY.md", true).read());
        assertFalse(remote.exists(caller, "MEMORY.md"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"content_filter", "incomplete"})
    void explicitlyAbortedOutputDoesNotConsumeSource(String reason) {
        Model model = mock(Model.class);
        when(model.stream(anyList(), any(), any()))
                .thenReturn(
                        Flux.just(
                                ChatResponse.builder()
                                        .content(
                                                List.of(
                                                        TextBlock.builder()
                                                                .text("- partial")
                                                                .build()))
                                        .finishReason(reason)
                                        .build()),
                        Flux.just(response("- complete")));
        List<Msg> messages = List.of(msg("aborted", "source"));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(IllegalStateException.class, () -> flush(manager, model, messages));
            new MemoryFlushManager(manager, model).retryDeferredMemories(rc, messages).block();
            assertFalse(manager.atomicMemoryFile(rc, dailyPath()).read().contains("- partial"));
            assertTrue(manager.atomicMemoryFile(rc, dailyPath()).read().contains("- complete"));
        }
        verify(model, times(2)).stream(anyList(), any(), any());
    }

    @Test
    void malformedPreparedResultCannotAppendBeforeValidation() throws Exception {
        Model model = model(new ArrayList<>(), "NO_REPLY");
        Path state;
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, List.of(msg("first", "source")));
        }
        try (var files = Files.list(workspace.resolve("memory/.flush-state"))) {
            state = files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow();
        }
        Files.writeString(
                state,
                """
                {"version":1,"consumed":[],"pending":{"dailyPath":"%s","id":"bad",
                "timestamp":"now","output":"- must not be written"}}
                """
                        .formatted(dailyPath()),
                StandardCharsets.UTF_8);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(RuntimeException.class, () -> flush(manager, model, List.of()));
        }
        assertFalse(Files.exists(workspace.resolve(dailyPath())));
    }

    @ParameterizedTest
    @ValueSource(strings = {"MEMORY.md", "memory/.flush-state/other.json"})
    void preparedResultCannotWriteOutsideADailyLedger(String target) throws Exception {
        Model model = model(new ArrayList<>(), "NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, List.of(msg("first", "source")));
        }
        Path state;
        try (var files = Files.list(workspace.resolve("memory/.flush-state"))) {
            state = files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow();
        }
        Files.writeString(workspace.resolve(target), "existing", StandardCharsets.UTF_8);
        Files.writeString(
                state,
                """
                {"version":1,"consumed":[],"pending":{"identities":["part"],
                "dailyPath":"%s","id":"00000000-0000-0000-0000-000000000001",
                "timestamp":"2026-11-12T00:00:00Z","output":"- must not be written"}}
                """
                        .formatted(target),
                StandardCharsets.UTF_8);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(RuntimeException.class, () -> flush(manager, model, List.of()));
        }
        assertEquals(
                "existing", Files.readString(workspace.resolve(target), StandardCharsets.UTF_8));
    }

    @Test
    void exactDuplicateBulletsAreSuppressedAcrossSessionsAndWithinOneOutput() {
        Model model = model(new ArrayList<>(), "- existing fact\n* new   fact\n- new fact");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            manager.appendMemoryFileUtf8(rc, "MEMORY.md", "- existing fact\n");
            flush(manager, model, List.of(msg("a", "first source")));
            new MemoryFlushManager(manager, model)
                    .flushMemories(
                            RuntimeContext.builder().userId("alice").sessionId("other").build(),
                            List.of(msg("b", "second source")))
                    .block();
            String daily = manager.atomicMemoryFile(rc, dailyPath()).read();
            assertFalse(daily.contains("existing fact"));
            assertEquals(1, daily.split("new   fact", -1).length - 1);
            assertFalse(daily.contains("- new fact"));
        }
    }

    @Test
    void unsupportedAtomicBackendFailsBeforeModelCallWithoutLocalFallback() {
        Model model = model(new ArrayList<>(), "NO_REPLY");
        try (WorkspaceManager manager =
                new WorkspaceManager(
                        workspace,
                        mock(AbstractFilesystem.class, org.mockito.Mockito.CALLS_REAL_METHODS),
                        null)) {
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> flush(manager, model, List.of(msg("a", "source"))));
        }
        verifyNoInteractions(model);
        assertFalse(Files.exists(workspace.resolve("memory/.flush-state")));
    }

    @Test
    void modelFailureDoesNotAdvanceReceiptsAndNextInstanceRetries() {
        Model model = mock(Model.class);
        when(model.stream(anyList(), any(), any()))
                .thenReturn(
                        Flux.error(new IllegalStateException("provider down")),
                        Flux.just(response("NO_REPLY")));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    IllegalStateException.class,
                    () -> flush(manager, model, List.of(msg("a", "retry-source"))));
            new MemoryFlushManager(manager, model)
                    .retryDeferredMemories(rc, List.of(msg("a", "retry-source")))
                    .block();
            flush(manager, model, List.of(msg("a", "retry-source")));
        }
        verify(model, times(2)).stream(anyList(), any(), any());
    }

    @Test
    void cancellationBeforeModelCompletionLeavesSourceRetryable() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        Model model = mock(Model.class);
        when(model.stream(anyList(), any(), any()))
                .thenReturn(
                        Flux.<ChatResponse>never()
                                .doOnSubscribe(ignored -> started.countDown())
                                .doOnCancel(cancelled::countDown),
                        Flux.just(response("NO_REPLY")));
        List<Msg> input = List.of(msg("cancelled", "retry this source"));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            var subscription =
                    new MemoryFlushManager(manager, model).flushMemories(rc, input).subscribe();
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
            } finally {
                subscription.dispose();
            }
            assertTrue(cancelled.await(5, TimeUnit.SECONDS));
            flush(manager, model, input);
            verify(model, times(1)).stream(anyList(), any(), any());
            new MemoryFlushManager(manager, model).retryDeferredMemories(rc, input).block();
            flush(manager, model, input);
        }
        verify(model, times(2)).stream(anyList(), any(), any());
    }

    @Test
    void blankTruncatedAndToolOutputsRemainRetryable() {
        List<ChatResponse> invalid =
                List.of(
                        response(" "),
                        ChatResponse.builder()
                                .content(List.of(TextBlock.builder().text("- incomplete").build()))
                                .finishReason("length")
                                .build(),
                        ChatResponse.builder()
                                .content(
                                        List.of(
                                                ToolUseBlock.builder()
                                                        .id("call")
                                                        .name("write")
                                                        .input(Map.of())
                                                        .build()))
                                .build(),
                        response("x".repeat(MemoryFlushManager.MAX_OUTPUT_CHARS + 1)));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            for (int i = 0; i < invalid.size(); i++) {
                Model model = mock(Model.class);
                when(model.stream(anyList(), any(), any()))
                        .thenReturn(Flux.just(invalid.get(i)), Flux.just(response("NO_REPLY")));
                List<Msg> input = List.of(msg("bad-" + i, "source"));
                assertThrows(IllegalStateException.class, () -> flush(manager, model, input));
                new MemoryFlushManager(manager, model).retryDeferredMemories(rc, input).block();
                verify(model, times(2)).stream(anyList(), any(), any());
            }
        }
    }

    @Test
    void lostAppendAcknowledgementReplaysPendingOutputWithoutAnotherModelCall() throws Exception {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "- durable fact");
        AtomicBoolean failOnce = new AtomicBoolean(true);
        try (WorkspaceManager failing =
                new WorkspaceManager(workspace) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        AtomicTextFile delegate = super.atomicMemoryFile(context, path);
                        if (!path.endsWith(".md")) return delegate;
                        return new AtomicTextFile() {
                            public String read() {
                                return delegate.read();
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                boolean changed = delegate.compareAndSet(expected, updated);
                                if (changed && failOnce.getAndSet(false))
                                    throw new IllegalStateException("lost ack");
                                return changed;
                            }
                        };
                    }
                }) {
            assertThrows(
                    IllegalStateException.class,
                    () -> flush(failing, model, List.of(msg("a", "remember"))));
        }
        try (WorkspaceManager recreated = new WorkspaceManager(workspace)) {
            flush(recreated, model, List.of()); // Recover prepared output even without new source.
            flush(recreated, model, List.of(msg("a", "remember")));
        }
        assertEquals(1, prompts.size());
        String daily = Files.readString(workspace.resolve(dailyPath()), StandardCharsets.UTF_8);
        assertEquals(1, daily.split("durable fact", -1).length - 1);
        assertEquals(1, daily.split("agentscope-memory-flush:", -1).length - 1);
    }

    @Test
    void concurrentReplicasCommitOnlyOneResultForTheSameSource() throws Exception {
        List<String> prompts = new CopyOnWriteArrayList<>();
        Model model = mock(Model.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(model.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            prompts.add(
                                    ((List<Msg>) invocation.getArgument(0))
                                            .get(1)
                                            .getTextContent());
                            started.countDown();
                            assertTrue(release.await(10, TimeUnit.SECONDS));
                            return Flux.just(response("- exactly one ledger fact"));
                        });
        try (WorkspaceManager first = new WorkspaceManager(workspace);
                WorkspaceManager second = new WorkspaceManager(workspace)) {
            List<Msg> input = List.of(msg("a", "concurrent source"));
            CompletableFuture<Void> firstCall =
                    CompletableFuture.runAsync(() -> flush(first, model, input));
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
                CompletableFuture.runAsync(() -> flush(second, model, input))
                        .get(5, TimeUnit.SECONDS);
                assertEquals(1, prompts.size(), "the durable attempt fences another live caller");
            } finally {
                release.countDown();
            }
            firstCall.get(10, TimeUnit.SECONDS);
            flush(first, model, input);
            String daily = first.atomicMemoryFile(rc, dailyPath()).read();
            assertEquals(1, daily.split("exactly one ledger fact", -1).length - 1);
            assertEquals(1, prompts.size());
        }
    }

    @Test
    void largeSourceIsFullyBatchedAndOldContextIsBounded() throws Exception {
        Files.createDirectories(workspace.resolve("memory"));
        Files.writeString(
                workspace.resolve("MEMORY.md"), "m".repeat(100_000), StandardCharsets.UTF_8);
        Files.writeString(
                workspace.resolve(dailyPath()), "d".repeat(100_000), StandardCharsets.UTF_8);
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "NO_REPLY");
        String body = "source-".repeat(10_000) + "last-source-sentinel";
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, List.of(msg("big", body)));
            flush(manager, model, List.of(msg("big", body)));
        }
        assertTrue(prompts.size() >= 3);
        assertTrue(prompts.stream().allMatch(p -> p.length() < 41_000));
        assertTrue(prompts.get(prompts.size() - 1).contains("last-source-sentinel"));
        String reassembled =
                prompts.stream()
                        .map(IncrementalMemoryFlushTest::source)
                        .map(p -> p.substring(p.indexOf("\n\n") + 2))
                        .collect(java.util.stream.Collectors.joining());
        assertEquals("[USER]: " + body, reassembled);
    }

    @Test
    void routedPersistentMemoryWorksAfterSandboxLeaseEndsAndAcrossRecreatedManagers()
            throws Exception {
        Files.writeString(
                workspace.resolve("MEMORY.md"), "- host-side memory", StandardCharsets.UTF_8);
        InMemoryStore store = new InMemoryStore();
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "- persistent routed fact");
        for (int i = 0; i < 2; i++) {
            AbstractFilesystem fs =
                    new CompositeFilesystem(
                            new SandboxBackedFilesystem(),
                            Map.of("/memory/", new RemoteFilesystem(store, List.of("test"))));
            try (WorkspaceManager manager = new WorkspaceManager(workspace, fs, null)) {
                flush(manager, model, List.of(msg("a", "remember")));
                assertTrue(
                        manager.readMemoryFileUtf8(rc, dailyPath())
                                .contains("persistent routed fact"));
            }
        }
        assertEquals(1, prompts.size());
        assertTrue(prompts.get(0).contains("host-side memory"));
        assertFalse(
                Files.exists(workspace.resolve(dailyPath())),
                "memory route must not fall back to pod-local disk");
    }

    @Test
    void readErrorsAndCorruptReceiptsDoNotResetProgress() throws Exception {
        Model model = model(new ArrayList<>(), "NO_REPLY");
        try (WorkspaceManager failing =
                new WorkspaceManager(workspace) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        return new AtomicTextFile() {
                            public String read() {
                                throw new IllegalStateException("store unavailable");
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                fail("must not write after read error");
                                return false;
                            }
                        };
                    }
                }) {
            assertThrows(
                    IllegalStateException.class,
                    () -> flush(failing, model, List.of(msg("a", "source"))));
        }
        verifyNoInteractions(model);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, List.of(msg("a", "source")));
        }
        Path state;
        try (var files = Files.list(workspace.resolve("memory/.flush-state"))) {
            state = files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow();
        }
        Files.writeString(state, "broken", StandardCharsets.UTF_8);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    RuntimeException.class,
                    () -> flush(manager, model, List.of(msg("a", "source"))));
        }
        verify(model, times(1)).stream(anyList(), any(), any());
    }

    @Test
    void identicalBulletsAreFilteredAtCommitAgainstTheLatestLedger() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts, "- same fact\n* same fact\n- new fact");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            manager.appendMemoryFileUtf8(rc, dailyPath(), "- same fact\n");
            flush(manager, model, List.of(msg("a", "remember")));
            String daily = manager.atomicMemoryFile(rc, dailyPath()).read();
            assertEquals(1, daily.split("same fact", -1).length - 1);
            assertEquals(1, daily.split("new fact", -1).length - 1);
        }
    }
}
