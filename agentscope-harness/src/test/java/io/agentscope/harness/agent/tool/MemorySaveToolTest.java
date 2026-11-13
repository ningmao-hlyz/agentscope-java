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
package io.agentscope.harness.agent.tool;

import static io.agentscope.harness.agent.tool.ToolResultAssertions.assertText;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.core.tool.ToolCallParam;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.filesystem.remote.RemoteFilesystem;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.memory.MemoryConfig;
import io.agentscope.harness.agent.memory.MemoryConsolidator;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;

class MemorySaveToolTest {
    @TempDir Path workspace;
    private final RuntimeContext rc = RuntimeContext.empty();

    private WorkspaceManager workspace(boolean remote) {
        return new WorkspaceManager(
                workspace,
                remote
                        ? new RemoteFilesystem(new InMemoryStore(), List.of("memory-save"))
                        : new LocalFilesystem(workspace));
    }

    private String dailyPath() {
        return "memory/" + LocalDate.now() + ".md";
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void exactUtf16LimitIsAcceptedAndNextSaveLeavesBothFilesUnchanged(boolean remote) {
        try (WorkspaceManager manager = workspace(remote)) {
            MemorySaveTool tool = new MemorySaveTool(manager, 1);
            assertText(tool.memorySave(rc, "🙂"), ToolResultState.SUCCESS);
            assertEquals("\n🙂\n", manager.atomicMemoryFile(rc, "MEMORY.md").read());
            String ledger = manager.atomicMemoryFile(rc, dailyPath()).read();
            assertTrue(ledger.contains("🙂"));
            assertTrue(
                    assertText(tool.memorySave(rc, "x"), ToolResultState.ERROR)
                            .contains("4-character limit by 3"));
            assertEquals("\n🙂\n", manager.atomicMemoryFile(rc, "MEMORY.md").read());
            assertEquals(ledger, manager.atomicMemoryFile(rc, dailyPath()).read());
        }
    }

    @Test
    void defaultLimitRejectsBeforeCreatingMemoryOrLedger() {
        try (WorkspaceManager manager = workspace(false)) {
            MemorySaveTool tool = new MemorySaveTool(manager);
            assertText(tool.memorySave(null, "x".repeat(16_000)), ToolResultState.ERROR);
            assertNull(manager.atomicMemoryFile(rc, "MEMORY.md").read());
            assertNull(manager.atomicMemoryFile(rc, dailyPath()).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fullMemoryDoesNotPromiseThatIdleMaintenanceWillFreeCapacity(boolean remote) {
        String content = "- " + "a".repeat(15_996);
        Model model = mock(Model.class);
        when(model.stream(anyList(), any(), any()))
                .thenReturn(
                        Flux.just(
                                ChatResponse.builder()
                                        .content(List.of(TextBlock.builder().text(content).build()))
                                        .finishReason("stop")
                                        .build()));
        try (WorkspaceManager manager = workspace(remote)) {
            MemorySaveTool tool = new MemorySaveTool(manager);
            assertText(tool.memorySave(rc, content), ToolResultState.SUCCESS);
            MemoryConsolidator consolidator = new MemoryConsolidator(manager, model);
            consolidator.consolidate(rc).block(Duration.ofSeconds(10));
            String before = manager.atomicMemoryFile(rc, "MEMORY.md").read();
            String ledger = manager.atomicMemoryFile(rc, dailyPath()).read();
            assertEquals(content, before);
            String error = assertText(tool.memorySave(rc, "- b"), ToolResultState.ERROR);
            assertTrue(error.contains("Save less content"));
            assertTrue(
                    error.contains("maintenance without new ledger entries does not free space"));
            consolidator.consolidate(rc).block(Duration.ofSeconds(10));
            verify(model, times(1)).stream(anyList(), any(), any());
            assertEquals(before, manager.atomicMemoryFile(rc, "MEMORY.md").read());
            assertEquals(ledger, manager.atomicMemoryFile(rc, dailyPath()).read());
        }
    }

    @Test
    void harnessPassesConfiguredLimitEvenWhenAutomaticMemoryHooksAreDisabled() {
        try (HarnessAgent agent =
                HarnessAgent.builder()
                        .name("memory-save-budget")
                        .workspace(workspace)
                        .model(mock(Model.class))
                        .disableMemoryHooks()
                        .disableWebTools()
                        .disableSubagents()
                        .disableDynamicSubagents()
                        .memory(MemoryConfig.builder().consolidationMaxTokens(1).build())
                        .build()) {
            Map<String, Object> input = Map.of("content", "abc");
            ToolResultBlock result =
                    agent.getToolkit()
                            .getTool("memory_save")
                            .callAsync(
                                    ToolCallParam.builder()
                                            .toolUseBlock(
                                                    ToolUseBlock.builder()
                                                            .id("save")
                                                            .name("memory_save")
                                                            .input(input)
                                                            .build())
                                            .input(input)
                                            .runtimeContext(rc)
                                            .build())
                            .block();
            assertText(result, ToolResultState.ERROR);
            assertNull(agent.getWorkspaceManager().atomicMemoryFile(rc, "MEMORY.md").read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentSavesRecheckCapacityAfterConflict(boolean remote) throws Exception {
        InMemoryStore store = new InMemoryStore();
        CountDownLatch read = new CountDownLatch(2);
        AtomicInteger reads = new AtomicInteger();
        var filesystem =
                remote
                        ? new RemoteFilesystem(store, List.of("memory-save"))
                        : new LocalFilesystem(workspace);
        Supplier<WorkspaceManager> replica =
                () ->
                        new WorkspaceManager(workspace, filesystem) {
                            @Override
                            public AtomicTextFile atomicMemoryFile(
                                    RuntimeContext context, String path) {
                                AtomicTextFile file = super.atomicMemoryFile(context, path);
                                if (!path.equals("MEMORY.md")) return file;
                                return new AtomicTextFile() {
                                    public String read() {
                                        String snapshot = file.read();
                                        if (reads.incrementAndGet() <= 2) {
                                            read.countDown();
                                            try {
                                                assertTrue(read.await(5, TimeUnit.SECONDS));
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                                throw new IllegalStateException(e);
                                            }
                                        }
                                        return snapshot;
                                    }

                                    public boolean compareAndSet(String expected, String updated) {
                                        return file.compareAndSet(expected, updated);
                                    }
                                };
                            }
                        };
        try (WorkspaceManager first = replica.get();
                WorkspaceManager second = replica.get()) {
            var executor = Executors.newFixedThreadPool(2);
            try {
                var a =
                        executor.submit(
                                () -> new MemorySaveTool(first, 4).memorySave(rc, "- first"));
                var b =
                        executor.submit(
                                () -> new MemorySaveTool(second, 4).memorySave(rc, "- second"));
                List<ToolResultBlock> results =
                        List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS));
                assertEquals(
                        1,
                        results.stream()
                                .filter(r -> r.getState() == ToolResultState.SUCCESS)
                                .count());
                assertEquals(
                        1,
                        results.stream()
                                .filter(r -> r.getState() == ToolResultState.ERROR)
                                .count());
                String memory = first.atomicMemoryFile(rc, "MEMORY.md").read();
                assertTrue(memory.length() <= 16);
                String winner = memory.strip();
                assertTrue(first.atomicMemoryFile(rc, dailyPath()).read().contains(winner));
                try (WorkspaceManager rebuilt = new WorkspaceManager(workspace, filesystem)) {
                    assertText(
                            new MemorySaveTool(rebuilt, 4).memorySave(rc, "- third"),
                            ToolResultState.ERROR);
                    assertEquals(memory, rebuilt.atomicMemoryFile(rc, "MEMORY.md").read());
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void exhaustedConflictsDoNotAppendLedger() {
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new LocalFilesystem(workspace)) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        if (!path.equals("MEMORY.md")) return super.atomicMemoryFile(context, path);
                        return new AtomicTextFile() {
                            public String read() {
                                return null;
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                return false;
                            }
                        };
                    }
                }) {
            assertText(new MemorySaveTool(manager).memorySave(rc, "- fact"), ToolResultState.ERROR);
            assertNull(manager.atomicMemoryFile(rc, dailyPath()).read());
        }
    }

    @Test
    void unsupportedAtomicWritesFailWithoutLegacyAppend() {
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new LocalFilesystem(workspace)) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        throw new UnsupportedOperationException("no conditional writes");
                    }
                }) {
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> new MemorySaveTool(manager).memorySave(rc, "- fact"));
            assertTrue(Files.notExists(workspace.resolve("MEMORY.md")));
            assertTrue(Files.notExists(workspace.resolve(dailyPath())));
        }
    }
}
