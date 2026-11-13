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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.middleware.ModelCallInput;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.filesystem.OverlayFilesystem;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.filesystem.remote.RemoteFilesystem;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.middleware.MemoryMaintenanceMiddleware;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

class MemoryConsolidatorBatchTest {
    @TempDir Path workspace;
    private final InMemoryStore store = new InMemoryStore();
    private final List<String> namespace = List.of("consolidation-batches");
    private final RuntimeContext rc = RuntimeContext.empty();
    private static final String LEDGER = "memory/2026-11-12.md";

    private WorkspaceManager workspace() {
        return new WorkspaceManager(workspace, new RemoteFilesystem(store, namespace));
    }

    private void seed(String path, String text, Instant modifiedAt) {
        store.put(
                namespace,
                path,
                Map.of("content", text, "encoding", "utf-8", "modified_at", modifiedAt.toString()));
    }

    private static ChatResponse response(String text) {
        return ChatResponse.builder()
                .content(List.of(TextBlock.builder().text(text).build()))
                .finishReason("stop")
                .build();
    }

    private static final class RecordingModel implements Model {
        final List<String> inputs = new ArrayList<>();
        final CountDownLatch called = new CountDownLatch(1);
        int window = 4096;
        boolean fail;
        GenerateOptions defaults;
        final List<GenerateOptions> usedOptions = new ArrayList<>();
        final List<Long> estimatedInputs = new ArrayList<>();
        Sinks.One<ChatResponse> pending;

        public Flux<ChatResponse> stream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            ContextPolicy policy = ContextPolicy.defaults();
            ModelCallInput input = new ModelCallInput(messages, tools, options, this);
            assertTrue(
                    policy.estimator().estimate(input).tokens()
                            <= policy.budget(input).inputTokens());
            inputs.add(messages.get(1).getTextContent());
            usedOptions.add(options);
            estimatedInputs.add(policy.estimator().estimate(input).tokens());
            called.countDown();
            if (pending != null) return pending.asMono().flux();
            return fail
                    ? Flux.error(new IllegalStateException("model unavailable"))
                    : Flux.just(response("- merged"));
        }

        public String getModelName() {
            return "consolidation-batch-test";
        }

        public int getContextWindowSize() {
            return window;
        }

        public GenerateOptions getDefaultGenerateOptions() {
            return defaults;
        }
    }

    private void run(WorkspaceManager manager, Model model) {
        new MemoryConsolidator(manager, model, 100).consolidate(rc).block(Duration.ofSeconds(5));
    }

    private static String source(String input) {
        String header = "### 2026-11-12.md\n";
        return input.substring(input.indexOf(header) + header.length(), input.length() - 2);
    }

    @Test
    void consolidationPacksAgainstInheritedOutputAndUsesItsOwnPolicy() {
        RecordingModel model = new RecordingModel();
        model.window = 8192;
        model.defaults = GenerateOptions.builder().maxCompletionTokens(4096).build();
        seed(LEDGER, "- fact\n".repeat(10000), Instant.now());
        try (WorkspaceManager manager = workspace()) {
            ContextPolicy policy = ContextPolicy.fromMap(Map.of("maxInputTokens", 2000));
            new MemoryConsolidator(manager, model, null, 100, null, policy).consolidate(rc).block();
            assertEquals(1, model.inputs.size());
            assertEquals(4096, model.usedOptions.get(0).getMaxCompletionTokens());
            assertTrue(model.estimatedInputs.get(0) <= 2000);
        }
    }

    @Test
    void catchUpIsBoundedAndStopsWhenNoProgressIsMade() {
        RecordingModel model = new RecordingModel();
        seed(LEDGER, "- fact\n".repeat(20000), Instant.now());
        try (WorkspaceManager manager = workspace()) {
            MemoryConsolidator consolidator = new MemoryConsolidator(manager, model, 100);
            consolidator.catchUp(rc).block();
            assertEquals(4, model.inputs.size());
            model.fail = true;
            consolidator
                    .catchUp(rc)
                    .onErrorResume(error -> reactor.core.publisher.Mono.empty())
                    .block();
            assertEquals(5, model.inputs.size());
        }
    }

    @Test
    void retentionKeepsUnfinishedBatchesUntilConsolidated() throws Exception {
        String path = "memory/" + LocalDate.now().minusDays(3) + ".md";
        String content = "- durable detail ".repeat(2000);
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new LocalFilesystem(workspace))) {
            manager.writeMemoryFileUtf8(rc, path, content);
            MemoryMaintenanceMiddleware maintenance =
                    new MemoryMaintenanceMiddleware(
                            manager, new MemoryConsolidator(manager, model, 100), 1, Duration.ZERO);
            for (int i = 0; i < 20; i++) {
                maintenance
                        .onAgent(
                                null,
                                rc,
                                new AgentInput(List.of()),
                                input -> Flux.just(new AgentEndEvent("reply")))
                        .blockLast();
                assertTrue(MemoryBackgroundTasks.awaitQuiescence(5, TimeUnit.SECONDS));
                if (manager.atomicMemoryFile(rc, path).read() == null) break;
                assertNotNull(
                        manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
            }
            assertTrue(model.inputs.size() > 1);
            assertNull(manager.atomicMemoryFile(rc, path).read());
            assertEquals(
                    content,
                    manager.atomicMemoryFile(rc, path.replace("memory/", "memory/archive/"))
                            .read());
            assertFalse(
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH)
                            .read()
                            .contains(path));
        }
    }

    @Test
    void archivedProgressIsPrunedAfterRecreationWithoutCallingModelOrChangingReceipts() {
        seed(LEDGER, "- complete source", Instant.now());
        seed("memory/.flush-state/receipts.json", "keep receipts", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            assertTrue(manager.archiveMemoryFile(rc, LEDGER));
        }
        try (WorkspaceManager rebuilt = workspace()) {
            String watermark =
                    rebuilt.atomicMemoryFile(rc, MemoryConsolidator.STATE_REL_PATH).read();
            run(rebuilt, model);
            String progress =
                    rebuilt.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
            assertFalse(progress.contains(LEDGER));
            assertEquals(
                    watermark,
                    rebuilt.atomicMemoryFile(rc, MemoryConsolidator.STATE_REL_PATH).read());
            assertEquals(
                    "keep receipts",
                    rebuilt.atomicMemoryFile(rc, "memory/.flush-state/receipts.json").read());
            assertEquals(1, model.inputs.size());
            run(rebuilt, model);
            assertEquals(
                    progress,
                    rebuilt.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void maintenanceRetainsAppendAfterCoverageCheck(boolean remote) throws Exception {
        String path = "memory/" + LocalDate.now().minusDays(3) + ".md";
        var filesystem =
                remote ? new RemoteFilesystem(store, namespace) : new LocalFilesystem(workspace);
        AtomicBoolean appended = new AtomicBoolean();
        try (WorkspaceManager peer = new WorkspaceManager(workspace, filesystem);
                WorkspaceManager manager =
                        new WorkspaceManager(workspace, filesystem) {
                            @Override
                            public boolean archiveMemoryFile(
                                    RuntimeContext context, String file, String expected) {
                                if (appended.compareAndSet(false, true))
                                    peer.appendMemoryFileUtf8(context, file, "\n- late fact");
                                return super.archiveMemoryFile(context, file, expected);
                            }
                        }) {
            manager.writeMemoryFileUtf8(rc, path, "- original fact");
            RecordingModel model = new RecordingModel();
            run(manager, model);
            var maintenance =
                    new MemoryMaintenanceMiddleware(
                            manager, new MemoryConsolidator(manager, model, 100), 1, Duration.ZERO);
            maintenance
                    .onAgent(
                            null,
                            rc,
                            new AgentInput(List.of()),
                            input -> Flux.just(new AgentEndEvent("reply")))
                    .blockLast();
            assertTrue(MemoryBackgroundTasks.awaitQuiescence(5, TimeUnit.SECONDS));
            assertTrue(appended.get());
            assertEquals("- original fact\n- late fact", manager.atomicMemoryFile(rc, path).read());
            assertNull(
                    manager.atomicMemoryFile(rc, path.replace("memory/", "memory/archive/"))
                            .read());
            assertEquals(2, model.inputs.size());
            assertTrue(model.inputs.get(1).contains("- late fact"));
            run(manager, model);
            assertEquals(2, model.inputs.size());
        }
    }

    @Test
    void legacyWatermarkAloneCannotAuthorizeSnapshotArchival() {
        Instant legacy = Instant.parse("2025-01-01T00:00:00Z");
        seed(MemoryConsolidator.STATE_REL_PATH, legacy.toString(), legacy);
        seed(LEDGER, "- legacy source", legacy.minusSeconds(1));
        try (WorkspaceManager manager = workspace()) {
            var file =
                    manager.listMemoryFileInfos(rc).stream()
                            .filter(info -> info.path().endsWith("2026-11-12.md"))
                            .findFirst()
                            .orElseThrow();
            assertFalse(
                    new MemoryConsolidator(manager, new RecordingModel()).isConsolidated(rc, file));
            seed(LEDGER, "- legacy source\n- new fact", Instant.now());
            assertNull(
                    new MemoryConsolidator(manager, new RecordingModel())
                            .consolidatedSnapshot(rc, file));
        }
    }

    @Test
    void activeLedgerOffsetIsKeptEvenWhenMatchingArchiveExists() {
        seed(LEDGER, "- complete source", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            seed("memory/archive/2026-11-12.md", "- complete source", Instant.now());
            String progress =
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
            run(manager, model);
            assertEquals(
                    progress,
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
            assertEquals(1, model.inputs.size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unfinishedOrChangedArchiveKeepsOffset(boolean unfinished) {
        String content = unfinished ? "- source detail ".repeat(3000) : "- complete source";
        seed(LEDGER, content, Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            assertTrue(manager.archiveMemoryFile(rc, LEDGER));
            if (!unfinished)
                seed("memory/archive/2026-11-12.md", "- modified source", Instant.now());
            String progress =
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
            run(manager, model);
            assertEquals(
                    progress,
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
            assertEquals(1, model.inputs.size());
        }
    }

    @Test
    void archiveCleanupConflictPreservesPeerProgressAndRetriesFromLatestState() {
        seed(LEDGER, "- complete source", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            assertTrue(manager.archiveMemoryFile(rc, LEDGER));
        }
        AtomicBoolean conflict = new AtomicBoolean(true);
        String peerLedger = "memory/2026-11-13.md";
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new RemoteFilesystem(store, namespace)) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        AtomicTextFile delegate = super.atomicMemoryFile(context, path);
                        if (!MemoryConsolidator.PROGRESS_REL_PATH.equals(path)) return delegate;
                        return new AtomicTextFile() {
                            public String read() {
                                return delegate.read();
                            }

                            @SuppressWarnings("unchecked")
                            public boolean compareAndSet(String expected, String updated) {
                                if (conflict.getAndSet(false)) {
                                    Map<String, Object> peer =
                                            JsonUtils.getJsonCodec().fromJson(expected, Map.class);
                                    Map<String, Object> files =
                                            new HashMap<>((Map<String, Object>) peer.get("files"));
                                    files.put(
                                            peerLedger, Map.of("offset", 0, "prefixHash", "peer"));
                                    peer.put("files", files);
                                    assertTrue(
                                            delegate.compareAndSet(
                                                    expected,
                                                    JsonUtils.getJsonCodec().toJson(peer)));
                                    return false;
                                }
                                return delegate.compareAndSet(expected, updated);
                            }
                        };
                    }
                }) {
            run(manager, model);
            String progress =
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
            assertTrue(progress.contains(LEDGER));
            assertTrue(progress.contains(peerLedger));
        }
        try (WorkspaceManager rebuilt = workspace()) {
            run(rebuilt, model);
            String progress =
                    rebuilt.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
            assertFalse(progress.contains(LEDGER));
            assertTrue(progress.contains(peerLedger));
            assertEquals(1, model.inputs.size());
        }
    }

    @Test
    void unreadableArchiveLeavesProgressForLaterCleanup() {
        seed(LEDGER, "- complete source", Instant.now());
        RecordingModel model = new RecordingModel();
        String progress;
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            assertTrue(manager.archiveMemoryFile(rc, LEDGER));
            progress = manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
        }
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new RemoteFilesystem(store, namespace)) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        if (path.startsWith("memory/archive/")) {
                            throw new IllegalStateException("archive unavailable");
                        }
                        return super.atomicMemoryFile(context, path);
                    }
                }) {
            run(manager, model);
            assertEquals(
                    progress,
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
            assertEquals(1, model.inputs.size());
        }
        try (WorkspaceManager rebuilt = workspace()) {
            run(rebuilt, model);
            assertFalse(
                    rebuilt.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH)
                            .read()
                            .contains(LEDGER));
        }
    }

    @Test
    void oversizedLineResumesAcrossRecreationAndBudgetChangesWithoutOmission() {
        String ledger = "- before\n- " + "🙂内容".repeat(5000) + "\n- after\n";
        seed(LEDGER, ledger, Instant.now());
        RecordingModel model = new RecordingModel();
        StringBuilder merged = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            int calls = model.inputs.size();
            model.window = i % 2 == 0 ? 2048 : 4096;
            try (WorkspaceManager manager = workspace()) {
                run(manager, model);
            }
            if (calls == model.inputs.size()) break;
            merged.append(source(model.inputs.get(calls)));
        }
        assertTrue(model.inputs.size() > 2);
        assertEquals(ledger, merged.toString());
        int calls = model.inputs.size();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
        }
        assertEquals(calls, model.inputs.size(), "completed files must not be resubmitted");
    }

    @Test
    void failedLaterBatchResumesFromLastCommittedOffset() {
        seed(LEDGER, "- first\n" + "long source detail ".repeat(2000), Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            String progress =
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
            model.fail = true;
            assertThrows(IllegalStateException.class, () -> run(manager, model));
            assertEquals(
                    progress,
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
        }
        model.fail = false;
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
        }
        assertEquals(source(model.inputs.get(1)), source(model.inputs.get(2)));
        assertNotEquals(source(model.inputs.get(0)), source(model.inputs.get(2)));
    }

    @Test
    void appendUsesOffsetEvenWithOldMtimeAndLegacyWatermarkRemainsReadable() {
        Instant legacy = Instant.parse("2025-01-01T00:00:00Z");
        seed(MemoryConsolidator.STATE_REL_PATH, legacy.toString(), legacy);
        seed("memory/2024-01-01.md", "- already merged", legacy.minusSeconds(1));
        seed(LEDGER, "- first", legacy.plusSeconds(1));
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            assertFalse(model.inputs.get(0).contains("already merged"));
            assertTrue(new MemoryConsolidator(manager, model).readWatermark(rc).isAfter(legacy));
        }
        seed(LEDGER, "- first\n- appended", legacy.minusSeconds(2));
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
        }
        assertEquals("\n- appended", source(model.inputs.get(1)));
        assertFalse(source(model.inputs.get(1)).contains("first"));
    }

    @Test
    void sharedLegacyStoreWatermarkCannotSkipAnotherNamespacesFiles() {
        store.put(
                List.of("memory", "consolidation"),
                "watermark",
                Map.of("ts", Instant.now().plusSeconds(3600).toEpochMilli()));
        seed(LEDGER, "- this namespaces source", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            new MemoryConsolidator(manager, model, null, 100, store)
                    .consolidate(rc)
                    .block(Duration.ofSeconds(5));
            assertEquals(1, model.inputs.size());
            assertEquals("- this namespaces source", source(model.inputs.get(0)));
        }
    }

    @Test
    void overlayProgressAndMigrationCutoffDoNotInheritSharedState() {
        List<String> shared = List.of("shared");
        store.put(
                shared,
                MemoryConsolidator.PROGRESS_REL_PATH,
                Map.of("content", "unrelated shared progress", "encoding", "utf-8"));
        store.put(
                shared,
                MemoryConsolidator.STATE_REL_PATH,
                Map.of("content", Instant.now().plusSeconds(3600).toString(), "encoding", "utf-8"));
        store.put(shared, "MEMORY.md", Map.of("content", "- shared baseline", "encoding", "utf-8"));
        seed(LEDGER, "- private source", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager =
                new WorkspaceManager(
                        workspace,
                        new OverlayFilesystem(
                                new RemoteFilesystem(store, namespace),
                                new RemoteFilesystem(store, shared)))) {
            run(manager, model);
            assertEquals(1, model.inputs.size());
            assertTrue(model.inputs.get(0).contains("- shared baseline"));
            assertTrue(model.inputs.get(0).contains("- private source"));
            assertNotNull(store.get(namespace, MemoryConsolidator.PROGRESS_REL_PATH));
            assertEquals(
                    "unrelated shared progress",
                    store.get(shared, MemoryConsolidator.PROGRESS_REL_PATH).value().get("content"));
        }
    }

    @Test
    void rewrittenPrefixReplaysInsteadOfSkippingContent() {
        seed(LEDGER, "- old source", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
        }
        seed(LEDGER, "- replacement source", Instant.now());
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
        }
        assertEquals("- replacement source", source(model.inputs.get(1)));
    }

    @Test
    void recursiveRemoteGlobDoesNotReprocessArchivedFiles() {
        seed("memory/archive/2026-11-11.md", "- archived source", Instant.now());
        seed(LEDGER, "- active source", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            assertEquals(1, model.inputs.size());
            assertFalse(model.inputs.get(0).contains("archived source"));
            assertEquals("- active source", source(model.inputs.get(0)));
            run(manager, model);
            assertEquals(1, model.inputs.size());
        }
    }

    @Test
    void filesystemIoStaysOffCallerAndModelEventThreads() throws Exception {
        seed(LEDGER, "- source", Instant.now());
        RecordingModel model = new RecordingModel();
        model.pending = Sinks.one();
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new RemoteFilesystem(store, namespace)) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        assertFalse(Schedulers.isInNonBlockingThread());
                        AtomicTextFile delegate = super.atomicMemoryFile(context, path);
                        return new AtomicTextFile() {
                            public String read() {
                                assertFalse(Schedulers.isInNonBlockingThread());
                                return delegate.read();
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                assertFalse(Schedulers.isInNonBlockingThread());
                                return delegate.compareAndSet(expected, updated);
                            }
                        };
                    }
                }) {
            var ioDone =
                    new MemoryConsolidator(manager, model, 100)
                            .consolidate(rc)
                            .subscribeOn(Schedulers.parallel())
                            .toFuture();
            assertTrue(model.called.await(3, TimeUnit.SECONDS));
            Schedulers.parallel().schedule(() -> model.pending.tryEmitValue(response("- merged")));
            ioDone.orTimeout(3, TimeUnit.SECONDS).join();
            assertEquals("- merged", manager.readMemoryMd(rc));
        }
    }

    @Test
    void progressWriteFailureReplaysWithoutRollingBackCommittedMemory() {
        seed(LEDGER, "- retry source", Instant.now());
        RecordingModel model = new RecordingModel();
        AtomicBoolean fail = new AtomicBoolean(true);
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new RemoteFilesystem(store, namespace)) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        AtomicTextFile delegate = super.atomicMemoryFile(context, path);
                        if (!MemoryConsolidator.PROGRESS_REL_PATH.equals(path)) return delegate;
                        return new AtomicTextFile() {
                            public String read() {
                                return delegate.read();
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                if (fail.getAndSet(false))
                                    throw new IllegalStateException("progress unavailable");
                                return delegate.compareAndSet(expected, updated);
                            }
                        };
                    }
                }) {
            run(manager, model);
            assertEquals("- merged", manager.readMemoryMd(rc));
            assertEquals(Instant.EPOCH, new MemoryConsolidator(manager, model).readWatermark(rc));
            assertNull(manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
            run(manager, model);
            assertEquals(source(model.inputs.get(0)), source(model.inputs.get(1)));
            assertNotNull(
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
        }
    }

    @Test
    void losingReplicaCannotOverwriteCommittedProgress() throws Exception {
        seed(LEDGER, "- shared source", Instant.now());
        RecordingModel slow = new RecordingModel();
        slow.pending = Sinks.one();
        RecordingModel winner = new RecordingModel();
        try (WorkspaceManager manager = workspace();
                WorkspaceManager other = workspace()) {
            var inFlight = new MemoryConsolidator(manager, slow, 100).consolidate(rc).toFuture();
            // Waiting for input proves that this replica captured both snapshots before the winner.
            assertTrue(slow.called.await(3, TimeUnit.SECONDS));
            run(other, winner);
            String progress =
                    other.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read();
            slow.pending.tryEmitValue(response("- stale replica"));
            inFlight.orTimeout(3, TimeUnit.SECONDS).join();
            assertEquals("- merged", manager.readMemoryMd(rc));
            assertEquals(
                    progress,
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
            run(manager, winner);
            assertEquals(1, winner.inputs.size());
        }
    }

    @Test
    void malformedProgressDoesNotResetToBeginning() {
        seed(LEDGER, "- source", Instant.now());
        seed(MemoryConsolidator.PROGRESS_REL_PATH, "broken-json", Instant.now());
        RecordingModel model = new RecordingModel();
        try (WorkspaceManager manager = workspace()) {
            run(manager, model);
            assertTrue(model.inputs.isEmpty());
            assertNull(manager.atomicMemoryFile(rc, "MEMORY.md").read());
            assertEquals(
                    "broken-json",
                    manager.atomicMemoryFile(rc, MemoryConsolidator.PROGRESS_REL_PATH).read());
        }
    }
}
