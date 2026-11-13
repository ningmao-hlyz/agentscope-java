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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.ModelCallInput;
import io.agentscope.core.middleware.ReasoningInput;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.context.ContextTokenEstimator;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class MemoryFlushConfigurationTest {
    private static final String FLUSH_PROMPT = "Only extract approved project decisions.";
    private static final String PAD = " earlier conversation detail".repeat(40);
    @TempDir Path workspace;
    private final AtomicInteger estimates = new AtomicInteger();
    private final AtomicInteger judgments = new AtomicInteger();

    private static Model model(List<List<Msg>> calls, String output) {
        Model model = mock(Model.class);
        when(model.getModelName()).thenReturn("configuration-test");
        when(model.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            calls.add(List.copyOf(invocation.getArgument(0)));
                            List<Msg> input = invocation.getArgument(0);
                            if (FLUSH_PROMPT.equals(input.get(0).getTextContent())) {
                                GenerateOptions options = invocation.getArgument(2);
                                assertEquals(64, options.getMaxCompletionTokens());
                                assertNull(options.getMaxTokens());
                            }
                            return Flux.just(response(output));
                        });
        return model;
    }

    private static ChatResponse response(String text) {
        return ChatResponse.builder()
                .content(List.of(TextBlock.builder().text(text).build()))
                .finishReason("stop")
                .build();
    }

    private static List<Msg> history(int size) {
        return IntStream.range(0, size)
                .mapToObj(
                        i ->
                                Msg.builder()
                                        .role(MsgRole.USER)
                                        .textContent("turn " + i + PAD)
                                        .build())
                .toList();
    }

    private MemoryConfig.Builder memoryConfig(Model memory) {
        return MemoryConfig.builder()
                .model(memory)
                .flushPrompt(FLUSH_PROMPT)
                .flushContextPolicy(
                        new ContextPolicy(
                                100_000,
                                64,
                                8,
                                input -> {
                                    estimates.incrementAndGet();
                                    return ContextTokenEstimator.approximate().estimate(input);
                                },
                                manifest -> {}))
                .flushOptions(GenerateOptions.builder().maxCompletionTokens(64).build())
                .admission(
                        input -> {
                            judgments.incrementAndGet();
                            return Mono.just(
                                    new MemoryAdmission.Decision(
                                            true, "configuration-test", Map.of()));
                        })
                .flushTrigger(MemoryConfig.FlushTrigger.never());
    }

    private HarnessAgent.Builder builder(Model primary, Model memory, Model summary) {
        return HarnessAgent.builder()
                .name("flush-configuration")
                .workspace(workspace)
                .model(primary)
                .disableWebTools()
                .disableDynamicSubagents()
                .disableSubagents()
                .memory(memoryConfig(memory).build())
                .compaction(
                        CompactionConfig.builder()
                                .model(summary)
                                .triggerMessages(2)
                                .keepTokens(0)
                                .keepMessages(1)
                                .prune(null)
                                .build());
    }

    @Test
    void perCallFlushKeepsBudgetOptionsAndAdmission() throws Exception {
        List<List<Msg>> flushes = new CopyOnWriteArrayList<>();
        Model primary = model(new ArrayList<>(), "answer");
        Model memory = model(flushes, "- approved project decision");
        HarnessAgent agent =
                builder(primary, memory, primary)
                        .memory(
                                memoryConfig(memory)
                                        .flushTrigger(MemoryConfig.FlushTrigger.always())
                                        .build())
                        .build();
        agent.call(
                        Msg.builder()
                                .role(MsgRole.USER)
                                .textContent("Remember this project decision")
                                .build(),
                        RuntimeContext.builder().userId("alice").sessionId("per-call").build())
                .block(Duration.ofSeconds(10));
        assertTrue(MemoryBackgroundTasks.awaitQuiescence(10, TimeUnit.SECONDS));
        assertEquals(1, judgments.get());
        assertTrue(estimates.get() > 1);
        assertEquals(
                1,
                flushes.stream()
                        .filter(input -> FLUSH_PROMPT.equals(input.get(0).getTextContent()))
                        .count());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void standardCompactionUsesMemoryConfigurationBeforeConsumingSources(boolean dedicated) {
        List<List<Msg>> flushes = new ArrayList<>();
        List<List<Msg>> summaries = new ArrayList<>();
        Model primary = model(flushes, "- approved project decision");
        Model memory = dedicated ? model(flushes, "- approved project decision") : null;
        HarnessAgent agent = builder(primary, memory, model(summaries, "summary")).build();
        List<Msg> source = history(3);
        RuntimeContext rc = RuntimeContext.builder().userId("alice").sessionId("standard").build();
        var candidate =
                agent.getCompactionHook()
                        .prepareCandidate(
                                agent,
                                rc,
                                new ModelCallInput(source, List.of(), null, primary),
                                100_000,
                                source)
                        .block(Duration.ofSeconds(10));
        assertTrue(candidate.history().size() < source.size());
        assertEquals(1, flushes.size());
        assertEquals(FLUSH_PROMPT, flushes.get(0).get(0).getTextContent());
        assertEquals(1, summaries.size());
        assertEquals(1, judgments.get());
        assertTrue(estimates.get() > 1);
        new MemoryFlushManager(
                        agent.getWorkspaceManager(),
                        memory != null ? memory : primary,
                        FLUSH_PROMPT)
                .flushMemories(rc, source.subList(0, 2))
                .block(Duration.ofSeconds(10));
        assertEquals(1, flushes.size(), "compaction and per-call extraction share receipts");
    }

    @Test
    void nativeOverflowKeepsTheMemoryConfiguration() {
        List<List<Msg>> flushes = new ArrayList<>();
        Model primary = model(new ArrayList<>(), "answer");
        HarnessAgent harness =
                builder(
                                primary,
                                model(flushes, "- approved project decision"),
                                model(new ArrayList<>(), "summary"))
                        .build();
        List<Msg> source = history(3);
        RuntimeContext rc =
                RuntimeContext.builder()
                        .userId("alice")
                        .sessionId("native-overflow")
                        .agentState(AgentState.builder().context(source).build())
                        .build();
        ReActAgent executing = mock(ReActAgent.class);
        when(executing.sessionLogEnabled()).thenReturn(true);
        when(executing.sessionKey(any(RuntimeContext.class)))
                .thenReturn(
                        new io.agentscope.core.session.SessionKey(
                                "alice", "flush-configuration", "native-overflow"));
        when(executing.getName()).thenReturn("flush-configuration");
        when(executing.getModel(any(RuntimeContext.class))).thenReturn(primary);
        AtomicInteger attempts = new AtomicInteger();
        harness.getCompactionHook()
                .overflowRecovery()
                .onReasoning(
                        executing,
                        rc,
                        new ReasoningInput(source, List.of(), null),
                        input ->
                                attempts.getAndIncrement() == 0
                                        ? Flux.error(
                                                new IllegalStateException(
                                                        "context_length_exceeded"))
                                        : Flux.empty())
                .blockLast(Duration.ofSeconds(10));
        assertEquals(2, attempts.get());
        assertEquals(1, flushes.size());
        assertEquals(FLUSH_PROMPT, flushes.get(0).get(0).getTextContent());
        assertEquals(1, judgments.get());
        assertTrue(estimates.get() > 1);
    }

    @Test
    void legacyOverflowUsesTheDedicatedMemoryModel() {
        List<List<Msg>> flushes = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        Model primary = mock(Model.class);
        when(primary.getModelName()).thenReturn("legacy-primary");
        when(primary.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            List<?> tools = invocation.getArgument(1);
                            if (!tools.isEmpty() && attempts.getAndIncrement() == 0)
                                return Flux.error(
                                        new IllegalStateException("context_length_exceeded"));
                            return Flux.just(response(tools.isEmpty() ? "summary" : "answer"));
                        });
        InMemoryAgentStateStore store = new InMemoryAgentStateStore();
        String session = "legacy-overflow";
        store.save(null, session, "agent_state", AgentState.builder().context(history(25)).build());
        HarnessAgent agent =
                builder(primary, model(flushes, "- approved project decision"), primary)
                        .legacySessionHistory(true)
                        .stateStore(store)
                        .compaction(CompactionConfig.builder().triggerMessages(1_000).build())
                        .build();
        Msg result =
                agent.call(
                                Msg.builder()
                                        .role(MsgRole.USER)
                                        .textContent("latest request")
                                        .build(),
                                RuntimeContext.builder().sessionId(session).build())
                        .block(Duration.ofSeconds(20));
        assertEquals("answer", result.getTextContent());
        assertEquals(1, flushes.size());
        assertEquals(FLUSH_PROMPT, flushes.get(0).get(0).getTextContent());
        assertEquals(1, judgments.get());
        assertTrue(estimates.get() > 1);
    }
}
