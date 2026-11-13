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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.AgentBase;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.session.SessionKey;
import io.agentscope.core.session.SessionLog;
import io.agentscope.core.session.SessionRecorder;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.filesystem.remote.RemoteFilesystem;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import io.agentscope.harness.agent.memory.compaction.ConversationCompactor;
import io.agentscope.harness.agent.middleware.MemoryFlushMiddleware;
import io.agentscope.harness.agent.session.WorkspaceSessionLogStore;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class SessionMemoryFlushTest {
    @TempDir Path workspace;
    private final RuntimeContext rc =
            RuntimeContext.builder().userId("alice").sessionId("session").build();
    private final SessionKey key = new SessionKey("alice", "memory-agent", "session");

    private WorkspaceManager workspace(boolean remote, InMemoryStore store) {
        return remote
                ? new WorkspaceManager(workspace, new RemoteFilesystem(store), null)
                : new WorkspaceManager(workspace);
    }

    private SessionLog log(WorkspaceManager manager) {
        return new WorkspaceSessionLogStore(manager).open(key, rc);
    }

    private static Msg message(String id, String text) {
        return Msg.builder().id(id).role(MsgRole.USER).textContent(text).build();
    }

    private static ChatResponse response(String text) {
        return ChatResponse.builder()
                .id(UUID.randomUUID().toString())
                .content(List.of(TextBlock.builder().text(text).build()))
                .finishReason("stop")
                .build();
    }

    private static Model model(List<String> prompts) {
        Model model = mock(Model.class);
        doAnswer(
                invocation -> {
                    List<Msg> messages = invocation.getArgument(0);
                    prompts.add(
                            messages.stream()
                                    .map(Msg::getTextContent)
                                    .filter(
                                            text ->
                                                    text != null
                                                            && text.contains(
                                                                    "Extract NEW memories"))
                                    .findFirst()
                                    .orElseThrow());
                    return Flux.just(response("NO_REPLY"));
                })
                .when(model)
                .stream(anyList(), any(), any());
        return model;
    }

    private static void write(SessionLog log, Consumer<SessionRecorder> action) {
        SessionRecorder recorder = new SessionRecorder(log, UUID.randomUUID().toString());
        try {
            action.accept(recorder);
            recorder.flushNow();
        } finally {
            recorder.close();
        }
    }

    private static void append(SessionLog log, Msg message) {
        write(log, recorder -> recorder.append("message/user", Map.of("message", message)));
    }

    private void flush(WorkspaceManager manager, Model model, SessionLog log) {
        new MemoryFlushManager(manager, model)
                .flushSessionMemories(rc, log)
                .block(Duration.ofSeconds(15));
    }

    @Test
    void differentAgentsWithTheSameSessionKeepIndependentProgressAfterRecreation() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog first = log(manager);
            SessionLog second =
                    new WorkspaceSessionLogStore(manager)
                            .open(new SessionKey("alice", "other-agent", key.sessionId()), rc);
            append(first, message("first", "first-agent-source"));
            append(second, message("second", "second-agent-source"));
            flush(manager, model(prompts), first);
            flush(manager, model(prompts), second);
            assertEquals(2, prompts.size());
            assertTrue(source(prompts.get(1)).contains("second-agent-source"));
            assertEquals(
                    second.head().seq(), second.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model(prompts), log(manager));
            flush(
                    manager,
                    model(prompts),
                    new WorkspaceSessionLogStore(manager)
                            .open(new SessionKey("alice", "other-agent", key.sessionId()), rc));
        }
        assertEquals(2, prompts.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void switchingToNativeHistoryStillRetriesLegacySourceRemovedByCompaction(boolean oldNative)
            throws Exception {
        Model failing = mock(Model.class);
        when(failing.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("offline")));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            new MemoryFlushManager(manager, failing)
                                    .flushMemories(
                                            rc,
                                            List.of(
                                                    message(
                                                            "legacy",
                                                            "original-before-compaction")))
                                    .block());
            if (oldNative) {
                try (var paths = Files.list(manager.getMemoryDir(rc).resolve(".flush-state"))) {
                    Path legacy =
                            paths.filter(path -> path.toString().endsWith(".json"))
                                    .findFirst()
                                    .orElseThrow();
                    String filename = legacy.getFileName().toString();
                    Files.move(
                            legacy,
                            legacy.resolveSibling(
                                    filename.substring(0, filename.length() - 5) + ".log.json"));
                }
            }
        }
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("later", "native-source-after-compaction"));
            flush(manager, model(prompts), log);
            assertEquals(1, prompts.size(), "legacy cooldown does not block the native source");
            assertTrue(source(prompts.get(0)).contains("native-source-after-compaction"));
            new MemoryFlushManager(manager, model(prompts)).retryDeferredMemories(rc, log).block();
            assertEquals(2, prompts.size());
            assertTrue(source(prompts.get(1)).contains("original-before-compaction"));
            flush(manager, model(prompts), log);
            assertEquals(2, prompts.size());
        }
    }

    @Test
    void oldUnscopedWatermarkIsReplayedRatherThanClaimedByANewSource() throws Exception {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("current", "source-without-own-watermark"));
            Path directory = manager.getMemoryDir(rc).resolve(".flush-state");
            Files.createDirectories(directory);
            String scope =
                    java.util.HexFormat.of()
                            .formatHex(
                                    java.security.MessageDigest.getInstance("SHA-256")
                                            .digest(
                                                    io.agentscope.core.util.JsonUtils.getJsonCodec()
                                                            .toJson(List.of("alice", "session"))
                                                            .getBytes(
                                                                    java.nio.charset
                                                                            .StandardCharsets
                                                                            .UTF_8)));
            Files.writeString(
                    directory.resolve(scope + ".log.json"),
                    "{\"version\":1,\"consumed\":[],\"pending\":null,\"through\":999,\"deferred\":{}}",
                    java.nio.charset.StandardCharsets.UTF_8);
            log.advanceExportCursor(MemoryFlushManager.SESSION_LOG_SINK, 0, log.head().seq());
            flush(manager, model(prompts), log);
            assertEquals(1, prompts.size());
            assertTrue(source(prompts.get(0)).contains("source-without-own-watermark"));
        }
    }

    @Test
    void oldLongLogScansTheCompatibilityPrefixOnceAcrossPages() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = spy(log(manager));
            write(
                    log,
                    recorder -> {
                        for (int i = 0; i < 300; i++) {
                            Msg original = message("legacy-" + i, "legacy-source-" + i + ";");
                            recorder.append("message/user", Map.of("message", original));
                            recorder.append("turn/output", Map.of("message", original));
                        }
                        recorder.append(
                                "turn/output",
                                Map.of("message", message("terminal", "terminal-only-original")));
                    });
            AtomicInteger prefixes = new AtomicInteger();
            doAnswer(
                            invocation -> {
                                if ((long) invocation.getArgument(0) == 0)
                                    prefixes.incrementAndGet();
                                return invocation.callRealMethod();
                            })
                    .when(log)
                    .scan(anyLong(), anyLong());
            flush(manager, model(prompts), log);
            assertEquals(2, prefixes.get(), "one main scan and one shared compatibility scan");
            String all =
                    prompts.stream()
                            .map(SessionMemoryFlushTest::source)
                            .collect(java.util.stream.Collectors.joining());
            for (int i = 0; i < 300; i++)
                assertEquals(1, all.split("legacy-source-" + i + ";", -1).length - 1);
            assertTrue(all.contains("terminal-only-original"));
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }

    @Test
    void customBackendCanSupplyIdentityWithoutImplementingSourceId() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = spy(log(manager));
            append(log, message("source", "custom-source"));
            doReturn(null).when(log).sourceId();
            assertThrows(IllegalArgumentException.class, () -> flush(manager, model(prompts), log));
            new MemoryFlushManager(manager, model(prompts))
                    .flushSessionMemories(rc, log, key.storagePath())
                    .block();
            assertEquals(1, prompts.size());
        }
    }

    private static String source(String prompt) {
        return prompt.substring(prompt.indexOf("Extract NEW memories"));
    }

    private static void rescheduleFailures(Path stateFile, Function<Map<?, ?>, String> retryAt)
            throws Exception {
        Map<?, ?> state =
                JsonUtils.getJsonCodec()
                        .fromJson(Files.readString(stateFile, StandardCharsets.UTF_8), Map.class);
        Map<Object, Object> failures = new HashMap<>();
        ((Map<?, ?>) state.get("deferred"))
                .forEach(
                        (key, value) -> {
                            Map<?, ?> failure = (Map<?, ?>) value;
                            Map<Object, Object> updated = new HashMap<>(failure);
                            updated.put("retryAt", retryAt.apply(failure));
                            failures.put(key, updated);
                        });
        Map<Object, Object> updated = new HashMap<>(state);
        updated.put("deferred", failures);
        Files.writeString(
                stateFile, JsonUtils.getJsonCodec().toJson(updated), StandardCharsets.UTF_8);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retriesTheEarliestDueBatchAcrossLegacyAndNativeQueues(boolean oldNative) throws Exception {
        Model failing = mock(Model.class);
        when(failing.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("offline")));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            new MemoryFlushManager(manager, failing)
                                    .flushMemories(rc, List.of(message("legacy", "legacy-failure")))
                                    .block());
            Path directory = manager.getMemoryDir(rc).resolve(".flush-state");
            Path legacy;
            try (var paths = Files.list(directory)) {
                legacy =
                        paths.filter(path -> path.toString().endsWith(".json"))
                                .findFirst()
                                .orElseThrow();
            }
            if (oldNative) {
                String name = legacy.getFileName().toString();
                legacy =
                        Files.move(
                                legacy,
                                legacy.resolveSibling(
                                        name.substring(0, name.length() - 5) + ".log.json"));
            }
            SessionLog log = log(manager);
            append(log, message("first", "earliest-native-failure"));
            long firstEnd = log.head().seq();
            assertThrows(IllegalStateException.class, () -> flush(manager, failing, log));
            append(log, message("second", "latest-native-failure"));
            assertThrows(IllegalStateException.class, () -> flush(manager, failing, log));
            Path nativeState;
            try (var paths = Files.list(directory)) {
                nativeState =
                        paths.filter(path -> path.toString().endsWith(".source.json"))
                                .findFirst()
                                .orElseThrow();
            }
            // Whole seconds and fractional seconds must sort by time, not ISO string order.
            Instant earliest = Instant.parse("2000-01-01T00:00:00Z");
            rescheduleFailures(legacy, failure -> earliest.plusMillis(1).toString());
            rescheduleFailures(
                    nativeState,
                    failure -> {
                        long through =
                                ((Number) ((Map<?, ?>) failure.get("source")).get("through"))
                                        .longValue();
                        return (through == firstEnd ? earliest : earliest.plusMillis(2)).toString();
                    });
            List<String> prompts = new ArrayList<>();
            Model healthy = model(prompts);
            flush(manager, healthy, log);
            assertEquals(1, prompts.size());
            assertTrue(source(prompts.get(0)).contains("earliest-native-failure"));
            flush(manager, healthy, log);
            assertEquals(2, prompts.size());
            assertTrue(source(prompts.get(1)).contains("legacy-failure"));
            flush(manager, healthy, log);
            assertEquals(3, prompts.size());
            assertTrue(source(prompts.get(2)).contains("latest-native-failure"));
            flush(manager, healthy, log);
            assertEquals(3, prompts.size());
        }
    }

    @Test
    void retryCompletedByAnotherReplicaDoesNotReadItsSourceAgain() throws Exception {
        Model failing = mock(Model.class);
        when(failing.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("offline")));
        try (WorkspaceManager manager = spy(new WorkspaceManager(workspace))) {
            SessionLog log = spy(log(manager));
            append(log, message("original", "already-recovered-source"));
            assertThrows(IllegalStateException.class, () -> flush(manager, failing, log));
            List<String> prompts = new ArrayList<>();
            Model healthy = model(prompts);
            flush(manager, healthy, log);
            Path state;
            try (var paths = Files.list(manager.getMemoryDir(rc).resolve(".flush-state"))) {
                state =
                        paths.filter(path -> path.toString().endsWith(".source.json"))
                                .findFirst()
                                .orElseThrow();
            }
            rescheduleFailures(state, failure -> Instant.EPOCH.toString());
            String path = "memory/.flush-state/" + state.getFileName();
            AtomicTextFile file = manager.atomicMemoryFile(rc, path);
            AtomicTextFile concurrent = mock(AtomicTextFile.class);
            AtomicInteger reads = new AtomicInteger();
            when(concurrent.read())
                    .thenAnswer(
                            invocation -> {
                                String before = file.read();
                                if (reads.incrementAndGet() == 2) {
                                    Map<?, ?> snapshot =
                                            JsonUtils.getJsonCodec().fromJson(before, Map.class);
                                    Map<Object, Object> completed = new HashMap<>(snapshot);
                                    completed.put("deferred", Map.of());
                                    assertTrue(
                                            file.compareAndSet(
                                                    before,
                                                    JsonUtils.getJsonCodec().toJson(completed)));
                                }
                                return file.read();
                            });
            doReturn(concurrent).when(manager).atomicMemoryFile(rc, path);
            clearInvocations(log);
            doThrow(new IllegalStateException("stale source must not be read"))
                    .when(log)
                    .scan(anyLong(), anyLong());
            flush(manager, healthy, log);
            verify(log, never()).scan(anyLong(), anyLong());
            assertTrue(prompts.isEmpty());
        }
    }

    @Test
    void administrativeContextChangesRecordSourceBeforeCheckpointAndRemoval() {
        List<String> prompts = new ArrayList<>();
        Model extraction = model(prompts);
        try (WorkspaceManager manager = new WorkspaceManager(workspace);
                ReActAgent agent =
                        ReActAgent.builder()
                                .name(key.agentId())
                                .model(extraction)
                                .sessionLogStore(new WorkspaceSessionLogStore(manager))
                                .build()) {
            agent.updateAgentState(rc, "initialize", state -> state.setSummary("metadata only"));
            agent.updateAgentState(
                    rc,
                    "add source",
                    state ->
                            state.contextMutable()
                                    .add(message("admin-source", "source-before-context-removal")));
            agent.clearContext(rc);
            assertTrue(agent.inspectAgentState(rc).state().getContext().isEmpty());
            SessionLog log = agent.sessionLog(rc);
            flush(manager, extraction, log);
            assertEquals(1, prompts.size());
            assertTrue(source(prompts.get(0)).contains("source-before-context-removal"));
            agent.updateAgentState(rc, "metadata", state -> state.setSummary("later checkpoint"));
            flush(manager, extraction, log);
            assertEquals(1, prompts.size());
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void timeoutCancelsModelOrAdmissionAndPreservesLogCursor(boolean stalledAdmission)
            throws Exception {
        CountDownLatch cancelled = new CountDownLatch(1);
        Model stalled = mock(Model.class);
        when(stalled.stream(anyList(), any(), any()))
                .thenReturn(
                        stalledAdmission
                                ? Flux.just(response("- durable fact"))
                                : Flux.<ChatResponse>never().doOnCancel(cancelled::countDown));
        MemoryConfig config =
                MemoryConfig.builder()
                        .flushTimeout(Duration.ofSeconds(1))
                        .admission(
                                stalledAdmission
                                        ? input ->
                                                Mono.<MemoryAdmission.Decision>never()
                                                        .doOnCancel(cancelled::countDown)
                                        : null)
                        .build();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("timed-out", "retryable source"));
            assertThrows(
                    RuntimeException.class,
                    () ->
                            MemoryFlushManager.configured(manager, stalled, config)
                                    .flushSessionMemories(rc, log)
                                    .block(Duration.ofSeconds(5)));
            assertTrue(cancelled.await(3, TimeUnit.SECONDS));
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            flush(manager, model(prompts), log);
            assertTrue(prompts.isEmpty(), "a new instance must retain the attempt lease");
            new MemoryFlushManager(manager, model(prompts)).retryDeferredMemories(rc, log).block();
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        assertTrue(source(prompts.get(0)).contains("retryable source"));
    }

    @Test
    void timedOutFlushReleasesNamespaceQueue() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Model extraction = mock(Model.class);
        when(extraction.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            if (calls.getAndIncrement() == 0) {
                                started.countDown();
                                return Flux.never();
                            }
                            return Flux.just(response("NO_REPLY"));
                        });
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("queued", "queued source"));
            ReActAgent agent = mock(ReActAgent.class);
            when(agent.sessionLogEnabled()).thenReturn(true);
            when(agent.sessionLog(rc)).thenReturn(log);
            MemoryFlushMiddleware hook =
                    new MemoryFlushMiddleware(
                            manager,
                            extraction,
                            MemoryConfig.builder().flushTimeout(Duration.ofSeconds(1)).build(),
                            null,
                            null);
            hook.onAgent(
                            agent,
                            rc,
                            new AgentInput(List.of()),
                            input -> Flux.just(new AgentEndEvent("reply")))
                    .blockLast();
            assertTrue(started.await(3, TimeUnit.SECONDS));
            hook.onAgent(
                            agent,
                            rc,
                            new AgentInput(List.of()),
                            input -> Flux.just(new AgentEndEvent("reply")))
                    .blockLast();
            assertTrue(MemoryBackgroundTasks.awaitQuiescence(5, TimeUnit.SECONDS));
            assertEquals(1, calls.get());
            new MemoryFlushManager(manager, extraction).retryDeferredMemories(rc, log).block();
            assertEquals(2, calls.get());
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void recreationResumesLogCursorAfterContextReplacement(boolean remote) {
        InMemoryStore store = new InMemoryStore();
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts);
        long consumed;
        try (WorkspaceManager manager = workspace(remote, store)) {
            SessionLog log = log(manager);
            Msg original = message("old", "raw-before-compaction");
            append(log, original);
            write(
                    log,
                    recorder -> recorder.append("context/replaced", Map.of("messages", List.of())));
            flush(manager, model, log);
            consumed = log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK);
            assertEquals(log.head().seq(), consumed);
        }
        try (WorkspaceManager manager = workspace(remote, store)) {
            SessionLog log = spy(log(manager));
            append(log, message("new", "new-source"));
            clearInvocations(log);
            long target = log.head().seq();
            flush(manager, model, log);
            verify(log).scan(consumed, target);
            assertEquals(target, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            clearInvocations(log);
            flush(manager, model, log);
            verify(log, never()).scan(anyLong(), anyLong());
        }
        assertEquals(2, prompts.size());
        assertTrue(source(prompts.get(0)).contains("raw-before-compaction"));
        assertFalse(source(prompts.get(1)).contains("raw-before-compaction"));
        assertTrue(source(prompts.get(1)).contains("new-source"));
    }

    @Test
    void failedExtractionCanReplayOriginalAfterCompactionAndRestart() {
        List<String> prompts = new ArrayList<>();
        Model unavailable = mock(Model.class);
        when(unavailable.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("unavailable")));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("original", "original-source"));
            assertThrows(IllegalStateException.class, () -> flush(manager, unavailable, log));
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            write(
                    log,
                    recorder ->
                            recorder.append(
                                    "context/replaced",
                                    Map.of(
                                            "messages",
                                            List.of(message("summary", "summary-only")))));
        }
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            new MemoryFlushManager(manager, model(prompts))
                    .retryDeferredMemories(rc, log(manager))
                    .block();
        }
        assertEquals(1, prompts.size());
        assertTrue(source(prompts.get(0)).contains("original-source"));
        assertFalse(source(prompts.get(0)).contains("summary-only"));
    }

    @Test
    void legacyReceiptsAndRepeatedLogSnapshotsDoNotReextractMessages() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts);
        Msg original = message("original", "extract-once");
        Msg summary =
                Msg.builder()
                        .id("summary")
                        .role(MsgRole.USER)
                        .name(ConversationCompactor.SUMMARY_MSG_NAME)
                        .textContent("synthetic-summary")
                        .build();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            new MemoryFlushManager(manager, model).flushMemories(rc, List.of(original)).block();
            SessionLog log = log(manager);
            write(
                    log,
                    recorder -> {
                        recorder.append(
                                "migration/baseline",
                                Map.of(
                                        "stateJson",
                                        AgentState.builder()
                                                .addMessage(original)
                                                .build()
                                                .toJson()));
                        recorder.append(
                                "compaction/start",
                                Map.of("inputMessages", List.of(original, summary)));
                        recorder.append("message/user", Map.of("message", original));
                        recorder.append("turn/output", Map.of("message", original));
                        recorder.append(
                                "message/user", Map.of("message", message("new", "new-source")));
                    });
            flush(manager, model, log);
            flush(manager, model, log);
        }
        assertEquals(2, prompts.size());
        assertFalse(source(prompts.get(1)).contains("extract-once"));
        assertFalse(source(prompts.get(1)).contains("synthetic-summary"));
    }

    @Test
    void stablePrefixDefersEventsArrivingDuringExtraction() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("first", "first-source"));
            long originalHead = log.head().seq();
            Model model = model(prompts);
            doAnswer(
                    invocation -> {
                        List<Msg> messages = invocation.getArgument(0);
                        prompts.add(messages.get(1).getTextContent());
                        if (prompts.size() == 1) append(log, message("later", "later-source"));
                        return Flux.just(response("NO_REPLY"));
                    })
                    .when(model)
                    .stream(anyList(), any(), any());
            flush(manager, model, log);
            assertEquals(originalHead, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertTrue(log.head().seq() > originalHead);
            flush(manager, model, log);
            assertEquals(2, prompts.size());
            assertFalse(source(prompts.get(0)).contains("later-source"));
            assertTrue(source(prompts.get(1)).contains("later-source"));
        }
    }

    @Test
    void cursorCommitFailureDoesNotRepeatSuccessfulExtraction() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = spy(log(manager));
            append(log, message("original", "source"));
            doThrow(new IllegalStateException("cursor unavailable"))
                    .doCallRealMethod()
                    .when(log)
                    .advanceExportCursor(anyString(), anyLong(), anyLong());
            Model model = model(prompts);
            assertThrows(IllegalStateException.class, () -> flush(manager, model, log));
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            flush(manager, model, log);
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        assertEquals(1, prompts.size());
    }

    @Test
    void ledgerFailureRecoversPreparedOutputBeforeAdvancingCursor() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts);
        doAnswer(
                invocation -> {
                    prompts.add("called");
                    return Flux.just(response("- retained fact"));
                })
                .when(model)
                .stream(anyList(), any(), any());
        String daily = "memory/" + LocalDate.now() + ".md";
        try (WorkspaceManager manager = spy(new WorkspaceManager(workspace))) {
            SessionLog log = log(manager);
            append(log, message("original", "source"));
            AtomicTextFile file = manager.atomicMemoryFile(rc, daily);
            AtomicTextFile failing = mock(AtomicTextFile.class);
            when(failing.read()).thenAnswer(invocation -> file.read());
            when(failing.compareAndSet(any(), anyString()))
                    .thenThrow(new IllegalStateException("ledger unavailable"));
            doReturn(failing).when(manager).atomicMemoryFile(rc, daily);
            assertThrows(IllegalStateException.class, () -> flush(manager, model, log));
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, log(manager));
            assertEquals(
                    1,
                    manager.atomicMemoryFile(rc, daily).read().split("- retained fact", -1).length
                            - 1);
        }
        assertEquals(1, prompts.size());
    }

    @Test
    void completedPageSurvivesFailureInNextPage() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            write(
                    log,
                    recorder -> {
                        recorder.append(
                                "message/user",
                                Map.of("message", message("first", "first-page-source")));
                        for (int i = 0; i < 255; i++)
                            recorder.append("extension/test", Map.of("index", i), false);
                        recorder.append(
                                "message/user",
                                Map.of("message", message("second", "second-page-source")));
                    });
            Model model = model(prompts);
            doAnswer(
                    invocation -> {
                        List<Msg> messages = invocation.getArgument(0);
                        prompts.add(messages.get(1).getTextContent());
                        return prompts.size() == 2
                                ? Flux.error(new IllegalStateException("second page failed"))
                                : Flux.just(response("NO_REPLY"));
                    })
                    .when(model)
                    .stream(anyList(), any(), any());
            assertThrows(IllegalStateException.class, () -> flush(manager, model, log));
            assertEquals(256, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            new MemoryFlushManager(manager, model).retryDeferredMemories(rc, log).block();
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertEquals(3, prompts.size());
            assertFalse(source(prompts.get(2)).contains("first-page-source"));
            assertTrue(source(prompts.get(2)).contains("second-page-source"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void timeoutInLaterPageKeepsCompletedPageAndRetriesUnpreparedCandidates(boolean duringAdmission)
            throws Exception {
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Model stalled = mock(Model.class);
        when(stalled.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            if (calls.incrementAndGet() == 1)
                                return Flux.just(response("NO_REPLY"));
                            return duringAdmission
                                    ? Flux.just(response("- first candidate\n- second candidate"))
                                    : Flux.<ChatResponse>never().doOnCancel(cancelled::countDown);
                        });
        MemoryConfig config =
                MemoryConfig.builder()
                        .flushTimeout(Duration.ofSeconds(2))
                        .admission(
                                duringAdmission
                                        ? input ->
                                                input.candidate().contains("second")
                                                        ? Mono.<MemoryAdmission.Decision>never()
                                                                .doOnCancel(cancelled::countDown)
                                                        : Mono.just(
                                                                new MemoryAdmission.Decision(
                                                                        true,
                                                                        "timeout-test",
                                                                        Map.of()))
                                        : null)
                        .build();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            write(
                    log,
                    recorder -> {
                        recorder.append(
                                "message/user",
                                Map.of("message", message("first", "first-page-source")));
                        for (int i = 0; i < 255; i++)
                            recorder.append("extension/test", Map.of("index", i), false);
                        recorder.append(
                                "message/user",
                                Map.of("message", message("second", "second-page-source")));
                    });
            assertThrows(
                    RuntimeException.class,
                    () ->
                            MemoryFlushManager.configured(manager, stalled, config)
                                    .flushSessionMemories(rc, log)
                                    .block(Duration.ofSeconds(8)));
            assertTrue(cancelled.await(3, TimeUnit.SECONDS));
            assertEquals(256, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertEquals(2, calls.get());
            String daily = manager.atomicMemoryFile(rc, "memory/" + LocalDate.now() + ".md").read();
            assertTrue(daily == null || !daily.contains("first candidate"));
        }
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager rebuilt = new WorkspaceManager(workspace)) {
            SessionLog log = log(rebuilt);
            new MemoryFlushManager(rebuilt, model(prompts)).retryDeferredMemories(rc, log).block();
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        assertEquals(1, prompts.size());
        assertFalse(source(prompts.get(0)).contains("first-page-source"));
        assertTrue(source(prompts.get(0)).contains("second-page-source"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void twoConsumersDeliverOnePreparedWinner(boolean remote) throws Exception {
        InMemoryStore store = new InMemoryStore();
        List<String> prompts = new CopyOnWriteArrayList<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (WorkspaceManager first = workspace(remote, store);
                WorkspaceManager second = workspace(remote, store)) {
            SessionLog firstLog = log(first);
            SessionLog secondLog = log(second);
            append(firstLog, message("shared", "shared-source"));
            Model model = model(prompts);
            AtomicInteger calls = new AtomicInteger();
            doAnswer(
                    invocation -> {
                        calls.incrementAndGet();
                        started.countDown();
                        if (!release.await(5, TimeUnit.SECONDS))
                            throw new IllegalStateException("model release timed out");
                        return Flux.just(response("- winner"));
                    })
                    .when(model)
                    .stream(anyList(), any(), any());
            CompletableFuture<Void> firstCall =
                    CompletableFuture.runAsync(() -> flush(first, model, firstLog));
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
                CompletableFuture.runAsync(() -> flush(second, model, secondLog))
                        .get(5, TimeUnit.SECONDS);
                assertEquals(1, calls.get());
                assertEquals(0, firstLog.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            } finally {
                release.countDown();
            }
            firstCall.get(10, TimeUnit.SECONDS);
            flush(second, model, secondLog);
            String ledger = first.atomicMemoryFile(rc, "memory/" + LocalDate.now() + ".md").read();
            assertEquals(1, ledger.split("- winner", -1).length - 1);
            assertEquals(1, calls.get());
            assertEquals(
                    firstLog.head().seq(),
                    firstLog.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }

    @Test
    void unsupportedCursorUsesWorkspaceProgressAcrossRecreation() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = spy(log(manager));
            append(log, message("source", "source"));
            doThrow(new UnsupportedOperationException("cursor persistence unsupported"))
                    .when(log)
                    .advanceExportCursor(anyString(), anyLong(), anyLong());
            Model model = model(prompts);
            flush(manager, model, log);
            append(log, message("later", "later source"));
            flush(manager, model, log);
            flush(manager, model, log);
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        assertEquals(2, prompts.size());
        assertFalse(source(prompts.get(1)).contains("[USER]: source"));
    }

    @Test
    void failedBatchIsDurableAndDoesNotBlockNewSourcesDuringCooldown() throws Exception {
        Model failing = mock(Model.class);
        when(failing.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("poison batch")));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("poison", "original-failed-source"));
            assertThrows(IllegalStateException.class, () -> flush(manager, failing, log));
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = spy(log(manager));
            append(log, message("new", "later-healthy-source"));
            Model healthy = model(prompts);
            flush(manager, healthy, log);
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertEquals(1, prompts.size());
            assertTrue(source(prompts.get(0)).contains("later-healthy-source"));
            assertFalse(source(prompts.get(0)).contains("original-failed-source"));
            Path state;
            try (var paths = Files.list(manager.getMemoryDir(rc).resolve(".flush-state"))) {
                state =
                        paths.filter(path -> path.toString().endsWith(".source.json"))
                                .findFirst()
                                .orElseThrow();
            }
            assertFalse(Files.readString(state).contains("original-failed-source"));
            assertTrue(Files.readString(state).contains("\"source\""));
            clearInvocations(log);
            new MemoryFlushManager(manager, healthy).retryDeferredMemories(rc, log).block();
            verify(log, times(1)).scan(anyLong(), anyLong());
            assertEquals(2, prompts.size());
            assertTrue(source(prompts.get(1)).contains("original-failed-source"));
            assertFalse(Files.readString(state).contains("original-failed-source"));
            Map<?, ?> completed =
                    JsonUtils.getJsonCodec().fromJson(Files.readString(state), Map.class);
            assertEquals(Map.of(), completed.get("deferred"));
            flush(manager, healthy, log);
            assertEquals(2, prompts.size());
        }
    }

    @Test
    void completedPagesReclaimReceiptsWithoutReextractingOldSnapshots() throws Exception {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            write(
                    log,
                    recorder -> {
                        for (int i = 0; i < 300; i++)
                            recorder.append(
                                    "message/user",
                                    Map.of("message", message("id-" + i, "fact-" + i)));
                    });
            Model model = model(prompts);
            flush(manager, model, log);
            int calls = prompts.size();
            Path state;
            try (var paths = Files.list(manager.getMemoryDir(rc).resolve(".flush-state"))) {
                state =
                        paths.filter(path -> path.toString().endsWith(".source.json"))
                                .findFirst()
                                .orElseThrow();
            }
            Map<?, ?> saved =
                    io.agentscope.core.util.JsonUtils.getJsonCodec()
                            .fromJson(Files.readString(state), Map.class);
            assertEquals(List.of(), saved.get("consumed"));
            assertTrue(Files.size(state) < 200);
            write(
                    log,
                    recorder ->
                            recorder.append(
                                    "compaction/start",
                                    Map.of("inputMessages", List.of(message("id-0", "fact-0")))));
            flush(manager, model, log);
            assertEquals(calls, prompts.size());
        }
    }

    @Test
    void budgetFailureDoesNotAdvanceLogCursor() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("source", "source"));
            MemoryConfig config =
                    MemoryConfig.builder()
                            .flushContextPolicy(ContextPolicy.fromMap(Map.of("maxInputTokens", 1)))
                            .build();
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            MemoryFlushManager.configured(manager, model, config)
                                    .flushSessionMemories(rc, log)
                                    .block());
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertTrue(prompts.isEmpty());
            new MemoryFlushManager(manager, model).retryDeferredMemories(rc, log).block();
            assertEquals(1, prompts.size());
        }
    }

    @Test
    void missingSourceEventsAndFutureCursorFailClosed() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = spy(log(manager));
            append(log, message("source", "source"));
            long target = log.head().seq();
            doReturn(List.of()).when(log).scan(0, target);
            assertThrows(
                    io.agentscope.core.session.SessionLogException.class,
                    () -> flush(manager, model(prompts), log));
            assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            doReturn(target + 1).when(log).exportCursor(MemoryFlushManager.SESSION_LOG_SINK);
            assertThrows(
                    io.agentscope.core.session.SessionLogException.class,
                    () -> flush(manager, model(prompts), log));
            doReturn(-1L).when(log).exportCursor(MemoryFlushManager.SESSION_LOG_SINK);
            assertThrows(
                    io.agentscope.core.session.SessionLogException.class,
                    () -> flush(manager, model(prompts), log));
        }
        assertTrue(prompts.isEmpty());
    }

    @Test
    void oversizedSourceIsSplitToBudgetAndConsumedInOrder() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            String text = "x".repeat(20_000);
            append(log, message("large", text));
            MemoryConfig config =
                    MemoryConfig.builder()
                            .flushContextPolicy(
                                    ContextPolicy.fromMap(Map.of("maxInputTokens", 3_503)))
                            .build();

            MemoryFlushManager.configured(manager, model, config)
                    .flushSessionMemories(rc, log)
                    .block(Duration.ofSeconds(15));

            assertTrue(prompts.size() > 1);
            StringBuilder reconstructed = new StringBuilder();
            for (String prompt : prompts) {
                String body = prompt.substring(prompt.indexOf("\n\n") + 2);
                reconstructed.append(body);
            }
            assertEquals("[USER]: " + text, reconstructed.toString());
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }

    @Test
    void blockingExtractionLeavesDefaultSchedulerFreeForModelResponse() {
        List<String> prompts = new ArrayList<>();
        Model model = model(prompts);
        doAnswer(
                invocation -> {
                    prompts.add("called");
                    return Flux.just(response("NO_REPLY")).subscribeOn(Schedulers.boundedElastic());
                })
                .when(model)
                .stream(anyList(), any(), any());
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("source", "fresh-source"));
            new MemoryFlushManager(manager, model)
                    .flushSessionMemories(rc, log)
                    .block(Duration.ofSeconds(5));
            assertEquals(1, prompts.size());
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }

    @Test
    void actualRebuiltAgentExtractsFinalReplyFromCommittedLog() throws Exception {
        List<String> prompts = new CopyOnWriteArrayList<>();
        Model extraction = model(prompts);
        AtomicInteger answers = new AtomicInteger();
        Model main =
                new Model() {
                    @Override
                    public Flux<ChatResponse> stream(
                            List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
                        return Flux.just(
                                response(
                                        answers.getAndIncrement() == 0
                                                ? "reply-one"
                                                : "reply-two"));
                    }

                    @Override
                    public String getModelName() {
                        return "main-model";
                    }
                };
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            for (int i = 0; i < 2; i++) {
                ReActAgent agent =
                        ReActAgent.builder()
                                .name(key.agentId())
                                .model(main)
                                .sessionLogStore(new WorkspaceSessionLogStore(manager))
                                .middleware(new MemoryFlushMiddleware(manager, extraction))
                                .build();
                String id = i == 0 ? "first" : "second";
                agent.call(
                                List.of(message(id, id + "-user-source")),
                                RuntimeContext.builder(rc)
                                        .runId(UUID.randomUUID().toString())
                                        .build())
                        .block(Duration.ofSeconds(10));
                assertTrue(MemoryBackgroundTasks.awaitQuiescence(5, TimeUnit.SECONDS));
            }
            assertEquals(2, prompts.size());
            assertTrue(source(prompts.get(0)).contains("reply-one"));
            assertTrue(source(prompts.get(1)).contains("reply-two"));
            assertTrue(source(prompts.get(1)).contains("second-user-source"));
            assertFalse(source(prompts.get(1)).contains("first-user-source"));
        }
    }

    @Test
    void actualCompactionDuringExtractionOutageCanReplayItsSnapshotAfterRecreation()
            throws Exception {
        Model unavailable = mock(Model.class);
        when(unavailable.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("extractor unavailable")));
        Msg prefix = message("prefix", "recover-this-original " + "original detail ".repeat(200));
        List<Msg> original = List.of(prefix, message("tail", "tail-message"));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            SessionRecorder recorder = new SessionRecorder(log, UUID.randomUUID().toString());
            RuntimeContext execution = RuntimeContext.builder(rc).build();
            execution.put(SessionRecorder.CONTEXT_KEY, recorder);
            execution.setAgentState(AgentState.builder().context(original).build());
            Model summary =
                    new Model() {
                        @Override
                        public Flux<ChatResponse> stream(
                                List<Msg> messages,
                                List<ToolSchema> tools,
                                GenerateOptions options) {
                            return Flux.just(response("short-summary"));
                        }

                        @Override
                        public String getModelName() {
                            return "summary-model";
                        }
                    };
            try {
                List<Msg> compacted =
                        new ConversationCompactor(
                                        summary, new MemoryFlushManager(manager, unavailable))
                                .compactIfNeeded(
                                        execution,
                                        original,
                                        CompactionConfig.builder()
                                                .triggerMessages(1)
                                                .keepTokens(0)
                                                .keepMessages(1)
                                                .truncateArgs(null)
                                                .prune(null)
                                                .build(),
                                        key.agentId(),
                                        key.sessionId())
                                .contextWrite(
                                        context ->
                                                context.put(
                                                        AgentBase.RUNTIME_CONTEXT_KEY, execution))
                                .block(Duration.ofSeconds(5))
                                .orElseThrow();
                assertTrue(compacted.get(0).getTextContent().contains("short-summary"));
                assertFalse(
                        compacted.stream()
                                .anyMatch(
                                        msg ->
                                                msg.getTextContent()
                                                        .contains("recover-this-original")));
                recorder.checkpointNow(
                        AgentState.builder().context(compacted).build(), "compacted");
                assertTrue(MemoryBackgroundTasks.awaitQuiescence(5, TimeUnit.SECONDS));
                assertEquals(0, log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            } finally {
                recorder.close();
            }
        }
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            new MemoryFlushManager(manager, model(prompts))
                    .retryDeferredMemories(rc, log(manager))
                    .block();
        }
        assertEquals(1, prompts.size());
        assertTrue(source(prompts.get(0)).contains("recover-this-original"));
        assertFalse(source(prompts.get(0)).contains("short-summary"));
    }

    @Test
    void backgroundExtractionDoesNotReuseClosedTurnRecorder() {
        List<String> prompts = new ArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("source", "durable-source"));
            SessionRecorder closed = new SessionRecorder(log, UUID.randomUUID().toString());
            closed.close();
            rc.put(SessionRecorder.CONTEXT_KEY, closed);
            rc.setAgentState(AgentState.builder().build());
            new MemoryFlushManager(manager, model(prompts))
                    .flushSessionMemories(rc, log)
                    .contextWrite(context -> context.put(AgentBase.RUNTIME_CONTEXT_KEY, rc))
                    .block(Duration.ofSeconds(5));
            assertEquals(1, prompts.size());
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertEquals(closed, SessionRecorder.from(rc));
        }
    }

    @Test
    void completingMiddlewareReadsLogEvenWithoutWorkingStateOrRecorder() throws Exception {
        List<String> prompts = new CopyOnWriteArrayList<>();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            SessionLog log = log(manager);
            append(log, message("original", "only-in-durable-log"));
            ReActAgent agent = mock(ReActAgent.class);
            when(agent.sessionLogEnabled()).thenReturn(true);
            when(agent.sessionLog(rc)).thenReturn(log);
            new MemoryFlushMiddleware(manager, model(prompts))
                    .onAgent(
                            agent,
                            rc,
                            new AgentInput(List.of()),
                            input -> Flux.just(new AgentEndEvent("reply")))
                    .blockLast(Duration.ofSeconds(2));
            assertTrue(MemoryBackgroundTasks.awaitQuiescence(5, TimeUnit.SECONDS));
            assertEquals(1, prompts.size());
            assertTrue(source(prompts.get(0)).contains("only-in-durable-log"));
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }
}
