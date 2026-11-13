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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.core.session.SessionLog;
import io.agentscope.core.session.SessionRecorder;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.filesystem.sandbox.SandboxBackedFilesystem;
import io.agentscope.harness.agent.filesystem.spec.LocalFilesystemSpec;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import io.agentscope.harness.agent.middleware.MemoryMaintenanceMiddleware;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class MemoryRecoveryTest {
    @TempDir Path workspace;
    private final RuntimeContext rc =
            RuntimeContext.builder()
                    .userId("alice")
                    .sessionId(UUID.randomUUID().toString())
                    .build();
    private final Model primary = mock(Model.class);

    private static Model memoryModel() {
        Model memory = mock(Model.class);
        when(memory.getModelName()).thenReturn("memory-recovery-test");
        when(memory.stream(anyList(), any(), any()))
                .thenReturn(
                        Flux.just(
                                ChatResponse.builder()
                                        .content(
                                                List.of(
                                                        TextBlock.builder()
                                                                .text("- Alice prefers Java.")
                                                                .build()))
                                        .finishReason("stop")
                                        .build()));
        return memory;
    }

    private HarnessAgent.Builder builder(MemoryConfig config) {
        return HarnessAgent.builder()
                .agentId("recovery-agent")
                .name("memory-recovery")
                .workspace(workspace)
                .model(primary)
                .filesystem(new LocalFilesystemSpec().isolationScope(IsolationScope.SESSION))
                .memory(config)
                .disableSubagents()
                .disableWebTools()
                .disableCompaction()
                .disableToolsConfig()
                .disableDefaultWorkspaceSkills();
    }

    private static void append(SessionLog log) {
        SessionRecorder recorder = new SessionRecorder(log, UUID.randomUUID().toString());
        try {
            recorder.append(
                    "message/user",
                    Map.of(
                            "message",
                            Msg.builder()
                                    .role(MsgRole.USER)
                                    .textContent("I prefer Java.")
                                    .build()));
            recorder.flushNow();
        } finally {
            recorder.close();
        }
    }

    private static void recover(HarnessAgent agent, RuntimeContext rc) {
        agent.recoverMemory(rc).block(Duration.ofSeconds(10));
    }

    @Test
    void sandboxMaintenanceArchivesHostMetadataWithoutStartingSandbox() {
        var filesystem = spy(new SandboxBackedFilesystem());
        try (var manager = new WorkspaceManager(workspace, filesystem, null)) {
            String source = "memory/" + LocalDate.now().minusDays(100) + ".md";
            manager.appendMemoryFileUtf8(rc, source, "- accepted fact");
            new MemoryMaintenanceMiddleware(manager, null, 90, Duration.ZERO)
                    .maintain(rc)
                    .block(Duration.ofSeconds(5));
            assertTrue(
                    Files.isRegularFile(
                            manager.resolveRuntimeDataPath(
                                    rc, source.replace("memory/", "memory/archive/"))));
            assertTrue(Files.notExists(manager.resolveRuntimeDataPath(rc, source)));
            verify(filesystem, never()).execute(any(), anyString(), any());
        }
    }

    @Test
    void separateWorkspacesForTheSameUserDoNotShareMaintenanceThrottle() {
        Model memory = memoryModel();
        RuntimeContext shared =
                RuntimeContext.builder()
                        .userId(UUID.randomUUID().toString())
                        .sessionId(rc.getSessionId())
                        .build();
        MemoryConfig config =
                MemoryConfig.builder()
                        .model(memory)
                        .flushTrigger(MemoryConfig.FlushTrigger.never())
                        .build();
        for (String directory : List.of("one", "two")) {
            try (HarnessAgent agent =
                    builder(config)
                            .workspace(workspace.resolve(directory))
                            .filesystem(
                                    new LocalFilesystemSpec().isolationScope(IsolationScope.USER))
                            .build()) {
                agent.getWorkspaceManager()
                        .appendMemoryFileUtf8(
                                shared, "memory/" + LocalDate.now() + ".md", "- accepted fact");
                recover(agent, shared);
                assertTrue(
                        agent.getWorkspaceManager()
                                .readMemoryMd(shared)
                                .contains("Alice prefers Java"));
            }
        }
        verify(memory, times(2)).stream(anyList(), any(), any());
    }

    @Test
    void dormantSessionRecoversAfterRecreationWithoutATurnAndBecomesANoop() {
        Model unavailable = memoryModel();
        when(unavailable.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("offline")));
        try (HarnessAgent agent =
                builder(
                                MemoryConfig.builder()
                                        .model(unavailable)
                                        .consolidationMinGap(Duration.ZERO)
                                        .build())
                        .build()) {
            append(agent.sessionLog(rc));
            assertThrows(IllegalStateException.class, () -> recover(agent, rc));
            assertEquals(0, agent.sessionLog(rc).exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        Model memory = memoryModel();
        AtomicInteger admissions = new AtomicInteger();
        MemoryConfig config =
                MemoryConfig.builder()
                        .model(memory)
                        .consolidationMinGap(Duration.ZERO)
                        .flushTrigger(MemoryConfig.FlushTrigger.throttled(Duration.ofDays(1)))
                        .admission(
                                input -> {
                                    admissions.incrementAndGet();
                                    assertTrue(input.source().contains("I prefer Java"));
                                    return Mono.just(
                                            new MemoryAdmission.Decision(true, "test", Map.of()));
                                })
                        .build();
        long head;
        try (HarnessAgent agent = builder(config).build()) {
            head = agent.sessionLog(rc).head().seq();
            agent.retryDeferredMemory(rc).block(Duration.ofSeconds(10));
            assertEquals(
                    head, agent.sessionLog(rc).exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertTrue(
                    agent.getWorkspaceManager()
                            .readMemoryFileUtf8(rc, "MEMORY.md")
                            .contains("Alice prefers Java"));
            assertNotNull(
                    agent.getWorkspaceManager()
                            .readMemoryFileUtf8(rc, "memory/" + LocalDate.now() + ".md"));
            assertEquals(head, agent.sessionLog(rc).head().seq());
        }
        try (HarnessAgent agent = builder(config).build()) {
            recover(agent, rc);
            assertEquals(head, agent.sessionLog(rc).head().seq());
        }
        assertEquals(1, admissions.get());
        verify(memory, times(2)).stream(anyList(), any(), any());
        verifyNoInteractions(primary);
    }

    @Test
    void neverSkipsExtractionButStillConsolidatesExistingLedger() {
        Model memory = memoryModel();
        try (HarnessAgent agent =
                builder(
                                MemoryConfig.builder()
                                        .model(memory)
                                        .flushTrigger(MemoryConfig.FlushTrigger.never())
                                        .consolidationMinGap(Duration.ZERO)
                                        .build())
                        .build()) {
            append(agent.sessionLog(rc));
            agent.getWorkspaceManager()
                    .appendMemoryFileUtf8(
                            rc, "memory/" + LocalDate.now() + ".md", "- previously accepted fact");
            recover(agent, rc);
            assertEquals(0, agent.sessionLog(rc).exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
            assertNotNull(agent.getWorkspaceManager().readMemoryFileUtf8(rc, "MEMORY.md"));
        }
        verify(memory).stream(anyList(), any(), any());
        verifyNoInteractions(primary);
    }

    @Test
    void disabledHooksDoNotExtractOrConsolidate() {
        Model memory = memoryModel();
        try (HarnessAgent agent =
                builder(MemoryConfig.builder().model(memory).build())
                        .disableMemoryHooks()
                        .build()) {
            append(agent.sessionLog(rc));
            recover(agent, rc);
            assertEquals(0, agent.sessionLog(rc).exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        verify(memory, never()).stream(anyList(), any(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void disabledHooksStillPermitOrdinaryAndOverflowCompaction(boolean overflow) throws Exception {
        Model memory = memoryModel();
        AtomicInteger calls = new AtomicInteger();
        when(primary.getModelName()).thenReturn("memory-hooks-test");
        when(primary.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            if (calls.getAndIncrement() == 0 && overflow)
                                return Flux.error(
                                        new IllegalStateException("context_length_exceeded"));
                            return Flux.just(
                                    ChatResponse.builder()
                                            .finishReason("stop")
                                            .content(
                                                    List.of(
                                                            TextBlock.builder()
                                                                    .text("short summary")
                                                                    .build()))
                                            .build());
                        });
        try (HarnessAgent agent =
                HarnessAgent.builder()
                        .workspace(workspace)
                        .agentId("disabled-memory")
                        .model(primary)
                        .memory(MemoryConfig.builder().model(memory).build())
                        .compaction(
                                CompactionConfig.builder()
                                        .triggerMessages(overflow ? 1000 : 2)
                                        .keepMessages(1)
                                        .keepTokens(0)
                                        .build())
                        .disableMemoryHooks()
                        .disableMemoryTools()
                        .disableWorkspaceContext()
                        .disableFilesystemTools()
                        .disableShellTool()
                        .disableWebTools()
                        .disableSubagents()
                        .disableToolsConfig()
                        .disableDefaultWorkspaceSkills()
                        .disableDynamicSkills()
                        .build()) {
            List<Msg> input =
                    java.util.stream.IntStream.range(0, overflow ? 30 : 3)
                            .mapToObj(
                                    i ->
                                            Msg.builder()
                                                    .role(MsgRole.USER)
                                                    .textContent("source " + i + "x".repeat(100))
                                                    .build())
                            .toList();
            agent.call(input, rc).block(Duration.ofSeconds(10));
            assertTrue(
                    MemoryBackgroundTasks.awaitQuiescence(
                            5, java.util.concurrent.TimeUnit.SECONDS));
            verify(memory, never()).stream(anyList(), any(), any());
            assertTrue(calls.get() >= 2, "summary and reasoning still execute");
        }
    }

    @Test
    void requiresExplicitSessionAndNativeHistory() {
        MemoryConfig config = MemoryConfig.builder().model(memoryModel()).build();
        try (HarnessAgent agent = builder(config).build()) {
            assertThrows(
                    IllegalArgumentException.class, () -> recover(agent, RuntimeContext.empty()));
        }
        try (HarnessAgent agent = builder(config).legacySessionHistory(true).build()) {
            assertThrows(IllegalStateException.class, () -> recover(agent, rc));
        }
    }

    @Test
    void maintenanceRunsEvenWhenExtractionFails() {
        Model memory = memoryModel();
        when(memory.stream(anyList(), any(), any()))
                .thenAnswer(
                        call -> {
                            List<Msg> input = call.getArgument(0);
                            if (input.stream()
                                    .anyMatch(
                                            msg ->
                                                    msg.getTextContent()
                                                            .contains("Extract NEW memories")))
                                return Flux.error(new IllegalStateException("extraction offline"));
                            return Flux.just(
                                    ChatResponse.builder()
                                            .content(
                                                    List.of(
                                                            TextBlock.builder()
                                                                    .text("- older accepted fact")
                                                                    .build()))
                                            .finishReason("stop")
                                            .build());
                        });
        try (HarnessAgent agent =
                builder(
                                MemoryConfig.builder()
                                        .model(memory)
                                        .consolidationMinGap(Duration.ZERO)
                                        .build())
                        .build()) {
            append(agent.sessionLog(rc));
            agent.getWorkspaceManager()
                    .appendMemoryFileUtf8(
                            rc, "memory/" + LocalDate.now() + ".md", "- older accepted fact");
            assertThrows(IllegalStateException.class, () -> recover(agent, rc));
            assertTrue(
                    agent.getWorkspaceManager()
                            .readMemoryFileUtf8(rc, "MEMORY.md")
                            .contains("older accepted fact"));
            assertEquals(0, agent.sessionLog(rc).exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
    }
}
