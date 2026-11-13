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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.session.SessionKey;
import io.agentscope.core.session.SessionRecorder;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.context.ContextBudgetExceededException;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.context.ContextTokenEstimator;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.session.WorkspaceSessionLogStore;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class MemoryFlushBudgetTest {
    @TempDir Path workspace;
    private final RuntimeContext rc =
            RuntimeContext.builder().userId("alice").sessionId("budget").build();
    private final List<List<Msg>> calls = new ArrayList<>();
    private final List<GenerateOptions> options = new ArrayList<>();
    private final ContextTokenEstimator estimator =
            input ->
                    new ContextTokenEstimator.Estimate(
                            input.messages().stream()
                                    .mapToLong(m -> m.getTextContent().length())
                                    .sum(),
                            "test-characters",
                            true);

    private MemoryConfig.Builder config(int tokens) {
        return MemoryConfig.builder()
                .flushPrompt("Extract facts.")
                .flushContextPolicy(new ContextPolicy(tokens, 64, 8, estimator, manifest -> {}));
    }

    private Model model(String output) {
        Model model = mock(Model.class);
        when(model.getModelName()).thenReturn("test-budget");
        when(model.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            calls.add(List.copyOf(invocation.getArgument(0)));
                            options.add(invocation.getArgument(2));
                            return Flux.just(
                                    ChatResponse.builder()
                                            .content(
                                                    List.of(
                                                            TextBlock.builder()
                                                                    .text(output)
                                                                    .build()))
                                            .finishReason("stop")
                                            .build());
                        });
        return model;
    }

    private Msg source(String id, String text) {
        return Msg.builder().id(id).role(MsgRole.USER).textContent(text).build();
    }

    private void flush(
            WorkspaceManager manager, Model model, MemoryConfig config, List<Msg> messages) {
        MemoryFlushManager.configured(manager, model, config)
                .flushMemories(rc, messages)
                .block(Duration.ofSeconds(10));
    }

    private String ledger() {
        return "memory/" + LocalDate.now() + ".md";
    }

    @Test
    void extractionAndAllAdmissionCallsShareOneDeadline() throws Exception {
        Model model = model("- first rule\n- second rule\n- third rule");
        AtomicInteger judgments = new AtomicInteger();
        CountDownLatch cancelled = new CountDownLatch(1);
        MemoryAdmission gate =
                input -> {
                    judgments.incrementAndGet();
                    return Mono.delay(Duration.ofMillis(600))
                            .map(ignored -> new MemoryAdmission.Decision(true, "test", Map.of()))
                            .doOnCancel(cancelled::countDown);
                };
        List<Msg> input = List.of(source("deadline", "source evidence"));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    RuntimeException.class,
                    () ->
                            flush(
                                    manager,
                                    model,
                                    config(2000)
                                            .flushTimeout(Duration.ofSeconds(1))
                                            .admission(gate)
                                            .build(),
                                    input));
            assertTrue(cancelled.await(3, TimeUnit.SECONDS));
            assertTrue(judgments.get() < 3, "the deadline must not reset for each candidate");
            assertNull(manager.atomicMemoryFile(rc, ledger()).read());
            flush(manager, model, config(2000).build(), input);
            assertEquals(1, calls.size(), "cooldown must survive timeout cancellation");
            MemoryFlushManager.configured(manager, model, config(2000).build())
                    .retryDeferredMemories(rc, input)
                    .block();
            assertEquals(2, calls.size(), "a timed-out batch must remain retryable");
            assertTrue(manager.atomicMemoryFile(rc, ledger()).read().contains("third rule"));
        }
    }

    @Test
    void timeoutKeepsCompletedBatchReceiptsAndRetriesOnlyTheRemainingSource() throws Exception {
        Model extraction = model("NO_REPLY");
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch cancelled = new CountDownLatch(1);
        doAnswer(
                invocation -> {
                    calls.add(List.copyOf(invocation.getArgument(0)));
                    if (attempts.getAndIncrement() == 1)
                        return Flux.<ChatResponse>never().doOnCancel(cancelled::countDown);
                    return Flux.just(
                            ChatResponse.builder()
                                    .content(List.of(TextBlock.builder().text("NO_REPLY").build()))
                                    .finishReason("stop")
                                    .build());
                })
                .when(extraction)
                .stream(anyList(), any(), any());
        List<Msg> input = List.of(source("a", "a".repeat(1200)), source("b", "b".repeat(1200)));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    RuntimeException.class,
                    () ->
                            flush(
                                    manager,
                                    extraction,
                                    config(1600).flushTimeout(Duration.ofSeconds(1)).build(),
                                    input));
            assertTrue(cancelled.await(3, TimeUnit.SECONDS));
        }
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            MemoryFlushManager.configured(manager, extraction, config(1600).build())
                    .retryDeferredMemories(rc, input)
                    .block();
        }
        assertEquals(3, calls.size());
        String retried = calls.get(2).get(1).getTextContent();
        retried = retried.substring(retried.indexOf("Extract NEW memories"));
        assertFalse(retried.contains("a".repeat(100)));
        assertTrue(retried.contains("b".repeat(100)));
    }

    @Test
    void generationOptionsCannotBypassTheBudgetedModelOrOutputLimit() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MemoryConfig.builder()
                                .flushOptions(
                                        GenerateOptions.builder()
                                                .modelName("other-window")
                                                .build()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MemoryConfig.builder()
                                .flushOptions(
                                        GenerateOptions.builder()
                                                .maxTokens(64)
                                                .maxCompletionTokens(128)
                                                .build()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MemoryConfig.builder()
                                .flushOptions(GenerateOptions.builder().maxTokens(0).build()));
    }

    @Test
    void wholeRequestBudgetSplitsBatchesAndSuppliesGenerationLimit() {
        Model model = model("NO_REPLY");
        List<Msg> history =
                List.of(
                        source("a", "a".repeat(350)),
                        source("b", "b".repeat(350)),
                        source("c", "c".repeat(350)));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, config(1100).build(), history);
            assertTrue(calls.size() > 1);
            assertTrue(
                    calls.stream()
                            .allMatch(
                                    input ->
                                            input.stream()
                                                            .mapToInt(
                                                                    m ->
                                                                            m.getTextContent()
                                                                                    .length())
                                                            .sum()
                                                    <= 1100));
            assertTrue(options.stream().allMatch(value -> value.getMaxTokens() == 64));
            int count = calls.size();
            flush(manager, model, config(2000).build(), history);
            assertEquals(count, calls.size(), "changing packing must not change source identities");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void inheritedOutputLimitControlsPackingAndExplicitOverrideWins(boolean completionTokens) {
        Model model = model("NO_REPLY");
        when(model.getContextWindowSize()).thenReturn(4000);
        when(model.getDefaultGenerateOptions())
                .thenReturn(
                        completionTokens
                                ? GenerateOptions.builder().maxCompletionTokens(2048).build()
                                : GenerateOptions.builder().maxTokens(2048).build());
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(
                    manager,
                    model,
                    config(4000).build(),
                    List.of(source("inherited", "x".repeat(3000))));
            assertTrue(calls.size() > 1);
            for (int i = 0; i < calls.size(); i++) {
                GenerateOptions used = options.get(i);
                assertEquals(completionTokens ? null : 2000, used.getMaxTokens());
                assertEquals(completionTokens ? 2000 : null, used.getMaxCompletionTokens());
                assertTrue(
                        calls.get(i).stream().mapToInt(m -> m.getTextContent().length()).sum()
                                <= 1992);
            }
            calls.clear();
            options.clear();
            flush(
                    manager,
                    model,
                    config(4000)
                            .flushOptions(
                                    completionTokens
                                            ? GenerateOptions.builder().maxTokens(128).build()
                                            : GenerateOptions.builder()
                                                    .maxCompletionTokens(128)
                                                    .build())
                            .build(),
                    List.of(source("override", "y".repeat(3000))));
            assertEquals(1, calls.size());
            assertEquals(completionTokens ? 128 : null, options.get(0).getMaxTokens());
            assertEquals(completionTokens ? null : 128, options.get(0).getMaxCompletionTokens());
        }
    }

    @Test
    void oversizedInheritedOutputIsCappedInTheActualRequest() {
        Model model = model("NO_REPLY");
        when(model.getContextWindowSize()).thenReturn(4000);
        when(model.getDefaultGenerateOptions())
                .thenReturn(GenerateOptions.builder().maxCompletionTokens(8192).build());
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, config(4000).build(), List.of(source("source", "new fact")));
            assertEquals(2000, options.get(0).getMaxCompletionTokens());
            assertNull(options.get(0).getMaxTokens());
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            flush(
                                    manager,
                                    model,
                                    config(4000)
                                            .flushOptions(
                                                    GenerateOptions.builder()
                                                            .maxTokens(8192)
                                                            .build())
                                            .build(),
                                    List.of(source("explicit", "new fact"))));
            assertEquals(1, calls.size());
        }
    }

    @Test
    void existingMemoryIncludesWholeEntriesFromBothEnds() {
        Model model = model("NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            String first = "- first " + "a".repeat(3000);
            String last = "- last " + "z".repeat(3000);
            manager.writeMemoryFileUtf8(
                    rc, "MEMORY.md", first + "\n- middle " + "m".repeat(6000) + "\n" + last);
            flush(manager, model, config(20000).build(), List.of(source("source", "new fact")));
            String prompt = calls.get(0).get(1).getTextContent();
            assertTrue(prompt.contains(first));
            assertTrue(prompt.contains(last));
            assertFalse(prompt.contains("- middle"));
            assertTrue(prompt.indexOf(first) < prompt.indexOf(last));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-2, -1, 0})
    void nonPositiveModelOutputLimitsKeepTheFiniteFallback(int value) {
        Model model = model("NO_REPLY");
        when(model.getDefaultGenerateOptions())
                .thenReturn(GenerateOptions.builder().maxTokens(value).build());
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, config(2000).build(), List.of(source("source", "new fact")));
            assertEquals(64, options.get(0).getMaxTokens());
            assertNull(options.get(0).getMaxCompletionTokens());
        }
    }

    @Test
    void precedingContextIsReadOnlyAndDoesNotConsumeLaterSource() {
        Model model = model("NO_REPLY");
        Msg first = source("first", "Use Java 17 for this project.");
        Msg second = source("second", "Yes, keep that rule.");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, config(2000).build(), List.of(first));
            flush(manager, model, config(2000).build(), List.of(first, second));
            String prompt = calls.get(1).get(1).getTextContent();
            assertTrue(prompt.contains("Read-only preceding conversation"));
            assertTrue(prompt.indexOf("Use Java 17") < prompt.indexOf("Extract NEW memories"));
            assertFalse(
                    prompt.substring(prompt.indexOf("Extract NEW memories"))
                            .contains("Use Java 17"));
            flush(
                    manager,
                    model,
                    config(2000).build(),
                    List.of(first, second, source("third", "new source")));
            assertEquals(3, calls.size());
        }
    }

    @Test
    void oldMemoryEntriesAreNotCutInHalfToFit() {
        Model model = model("NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            manager.appendMemoryFileUtf8(
                    rc, "MEMORY.md", "- " + "long-entry".repeat(1000) + "END\n- next fact\n");
            manager.appendMemoryFileUtf8(rc, ledger(), "- earlier fact\n- latest complete fact\n");
            flush(manager, model, config(2000).build(), List.of(source("a", "new source")));
            String prompt = calls.get(0).get(1).getTextContent();
            assertFalse(prompt.contains("long-entry"));
            assertTrue(prompt.contains("- latest complete fact"));
        }
    }

    @Test
    void oversizedTemplateFailsBeforeCallAndDoesNotConsume() {
        Model model = model("NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            List<Msg> input = List.of(source("a", "x".repeat(2000)));
            assertThrows(
                    ContextBudgetExceededException.class,
                    () ->
                            flush(
                                    manager,
                                    model,
                                    config(3000).flushPrompt("x".repeat(4000)).build(),
                                    input));
            assertTrue(calls.isEmpty());
            MemoryFlushManager.configured(manager, model, config(3000).build())
                    .retryDeferredMemories(rc, input)
                    .block();
            assertEquals(1, calls.size());
        }
    }

    @Test
    void partialSourceResumesAfterRestartWithDifferentBudget() {
        Model model = model("NO_REPLY");
        Model failing = mock(Model.class);
        AtomicInteger attempts = new AtomicInteger();
        when(failing.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation ->
                                attempts.getAndIncrement() == 1
                                        ? Flux.error(new IllegalStateException("model unavailable"))
                                        : model.stream(
                                                invocation.getArgument(0),
                                                invocation.getArgument(1),
                                                invocation.getArgument(2)));
        String text = "a😀".repeat(700);
        List<Msg> input = List.of(source("large", text));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    IllegalStateException.class,
                    () -> flush(manager, failing, config(1000).build(), input));
            assertEquals(1, calls.size());
        }
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            MemoryFlushManager.configured(manager, model, config(1600).build())
                    .retryDeferredMemories(rc, input)
                    .block();
            int count = calls.size();
            flush(manager, model, config(2000).build(), input);
            assertEquals(count, calls.size());
        }
        StringBuilder reconstructed = new StringBuilder();
        for (List<Msg> call : calls) {
            String prompt = call.get(1).getTextContent();
            int start = prompt.indexOf("Extract NEW memories");
            String part = prompt.substring(prompt.indexOf("\n\n", start) + 2);
            assertFalse(Character.isLowSurrogate(part.charAt(0)));
            assertFalse(Character.isHighSurrogate(part.charAt(part.length() - 1)));
            reconstructed.append(part);
        }
        assertEquals("[USER]: " + text, reconstructed.toString());
    }

    @Test
    void deferredSourceCanResumeUnderASmallerBudgetWithoutRepeatingPrefixes() {
        Model failing = mock(Model.class);
        when(failing.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("offline")));
        List<Msg> input = List.of(source("deferred", "a😀".repeat(500)));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    IllegalStateException.class,
                    () -> flush(manager, failing, config(3000).build(), input));
        }
        Model healthy = model("NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            MemoryFlushManager.configured(manager, healthy, config(700).build())
                    .retryDeferredMemories(rc, input)
                    .block(Duration.ofSeconds(10));
            assertTrue(calls.size() > 1);
            StringBuilder recovered = new StringBuilder();
            for (List<Msg> call : calls) {
                String prompt = call.get(1).getTextContent();
                recovered.append(
                        prompt.substring(
                                prompt.indexOf("\n\n", prompt.indexOf("Extract NEW memories"))
                                        + 2));
            }
            assertEquals("[USER]: " + "a😀".repeat(500), recovered.toString());
            int count = calls.size();
            flush(manager, healthy, config(700).build(), input);
            assertEquals(count, calls.size());
        }
    }

    @Test
    void pageCleanupPreservesPartialDeferredProgressAcrossRecreation() {
        SessionKey key = new SessionKey("alice", "memory-agent", "budget");
        Model failing = mock(Model.class);
        when(failing.stream(anyList(), any(), any()))
                .thenReturn(Flux.error(new IllegalStateException("offline")));
        Model interrupted = mock(Model.class);
        AtomicInteger attempts = new AtomicInteger();
        when(interrupted.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            if (attempts.incrementAndGet() == 2)
                                return Flux.error(new IllegalStateException("offline again"));
                            calls.add(List.copyOf(invocation.getArgument(0)));
                            return Flux.just(
                                    ChatResponse.builder()
                                            .content(
                                                    List.of(
                                                            TextBlock.builder()
                                                                    .text("NO_REPLY")
                                                                    .build()))
                                            .finishReason("stop")
                                            .build());
                        });
        String text = "a😀".repeat(500);
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            var log = new WorkspaceSessionLogStore(manager).open(key, rc);
            var recorder = new SessionRecorder(log, "source");
            try {
                recorder.append("message/user", Map.of("message", source("deferred", text)));
                recorder.flushNow();
            } finally {
                recorder.close();
            }
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            MemoryFlushManager.configured(manager, failing, config(3000).build())
                                    .flushSessionMemories(rc, log)
                                    .block());
            MemoryFlushManager.configured(manager, interrupted, config(700).build())
                    .retryDeferredMemories(rc, log)
                    .block();
            assertEquals(2, attempts.get());
            assertEquals(log.head().seq(), log.exportCursor(MemoryFlushManager.SESSION_LOG_SINK));
        }
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            var log = new WorkspaceSessionLogStore(manager).open(key, rc);
            MemoryFlushManager.configured(manager, model("NO_REPLY"), config(700).build())
                    .retryDeferredMemories(rc, log)
                    .block();
        }
        StringBuilder recovered = new StringBuilder();
        for (List<Msg> call : calls) {
            String prompt = call.get(1).getTextContent();
            recovered.append(
                    prompt.substring(
                            prompt.indexOf("\n\n", prompt.indexOf("Extract NEW memories")) + 2));
        }
        assertEquals("[USER]: " + text, recovered.toString());
    }

    @Test
    void completionTokenOptionReachesModelWithoutConflictingLegacyField() {
        Model model = model("NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(
                    manager,
                    model,
                    config(2000)
                            .flushOptions(
                                    GenerateOptions.builder().maxCompletionTokens(128).build())
                            .build(),
                    List.of(source("a", "new source")));
            assertEquals(128, options.get(0).getMaxCompletionTokens());
            assertNull(options.get(0).getMaxTokens());
        }
    }

    @Test
    void admissionUsesNewBatchSourceAndConfirmedRejectionIsConsumed() {
        Model model = model("- durable rule\n- one-off task");
        List<MemoryAdmission.Input> judged = new ArrayList<>();
        MemoryAdmission gate =
                input -> {
                    judged.add(input);
                    return Mono.just(
                            new MemoryAdmission.Decision(
                                    input.candidate().contains("durable"),
                                    "test-v1",
                                    Map.of("durable", 0.9)));
                };
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            List<Msg> input = List.of(source("a", "source evidence"));
            flush(manager, model, config(2000).admission(gate).build(), input);
            flush(manager, model, config(2000).admission(gate).build(), input);
            assertEquals(2, judged.size());
            assertEquals("[USER]: source evidence", judged.get(0).source());
            assertEquals(List.of("- durable rule"), judged.get(1).existing());
            String daily = manager.atomicMemoryFile(rc, ledger()).read();
            assertTrue(daily.contains("durable rule"));
            assertFalse(daily.contains("one-off task"));
            assertEquals(1, calls.size());
        }
    }

    @Test
    void admissionIgnoresPrefaceAndHeadingsButKeepsMultilineBullets() {
        Model model = model("Here are the memories:\n# Facts\n- durable rule\n  scope: project");
        List<MemoryAdmission.Input> judged = new ArrayList<>();
        MemoryConfig config =
                config(2000)
                        .admission(
                                input -> {
                                    judged.add(input);
                                    return Mono.just(
                                            new MemoryAdmission.Decision(
                                                    true, "test-v1", Map.of()));
                                })
                        .build();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            List<Msg> input = List.of(source("preface", "source evidence"));
            flush(manager, model, config, input);
            flush(manager, model, config, input);
            assertEquals(1, calls.size());
            assertEquals(1, judged.size());
            assertEquals("- durable rule\n  scope: project", judged.get(0).candidate());
            String daily = manager.atomicMemoryFile(rc, ledger()).read();
            assertFalse(daily.contains("Here are"));
            assertFalse(daily.contains("# Facts"));
            assertTrue(daily.contains("scope: project"));
        }
    }

    @Test
    void admissionWithoutAnyBulletLeavesSourceRetryable() {
        Model model = model("Just a preface without candidates");
        MemoryConfig config =
                config(2000)
                        .admission(
                                input -> {
                                    throw new AssertionError("No candidates to judge");
                                })
                        .build();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            List<Msg> input = List.of(source("format", "source evidence"));
            assertThrows(IllegalStateException.class, () -> flush(manager, model, config, input));
            MemoryFlushManager.configured(manager, model("- valid candidate"), config(2000).build())
                    .retryDeferredMemories(rc, input)
                    .block();
            assertTrue(manager.atomicMemoryFile(rc, ledger()).read().contains("valid candidate"));
        }
    }

    @Test
    void admissionUsesUserIdentityAcrossSessionsAndSeesTheirExistingFacts() {
        Model model = model("- shared user fact");
        List<MemoryAdmission.Input> judged = new ArrayList<>();
        MemoryConfig config =
                config(2000)
                        .admission(
                                input -> {
                                    judged.add(input);
                                    return Mono.just(
                                            new MemoryAdmission.Decision(
                                                    true, "test-v1", Map.of()));
                                })
                        .build();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            for (String session : List.of("first", "second")) {
                MemoryFlushManager.configured(manager, model, config)
                        .flushMemories(
                                RuntimeContext.builder().userId("alice").sessionId(session).build(),
                                List.of(source(session, "user fact evidence")))
                        .block();
            }
            assertEquals(
                    List.of("alice", "alice"),
                    judged.stream().map(MemoryAdmission.Input::owner).toList());
            assertTrue(judged.get(1).existing().contains("- shared user fact"));
        }
    }

    @Test
    void unreadableExistingMemoryDoesNotStartExtractionOrConsumeSource() throws Exception {
        Model model = model("- new fact");
        List<Msg> input = List.of(source("a", "source evidence"));
        Files.write(workspace.resolve("MEMORY.md"), new byte[] {(byte) 0xc3, 0x28});
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    UncheckedIOException.class,
                    () -> flush(manager, model, config(2000).build(), input));
            assertEquals(0, calls.size());
            assertNull(manager.atomicMemoryFile(rc, ledger()).read());
            Files.writeString(workspace.resolve("MEMORY.md"), "- old fact");
            MemoryFlushManager.configured(manager, model, config(2000).build())
                    .retryDeferredMemories(rc, input)
                    .block();
            assertEquals(1, calls.size());
            assertTrue(manager.atomicMemoryFile(rc, ledger()).read().contains("new fact"));
        }
    }

    @Test
    void unreadableMemoryBeforeDeliveryPreservesPreparedOutput() throws Exception {
        Model model = model("- approved fact");
        AtomicInteger judgments = new AtomicInteger();
        MemoryAdmission gate =
                input ->
                        Mono.fromCallable(
                                () -> {
                                    judgments.incrementAndGet();
                                    Files.write(
                                            workspace.resolve("MEMORY.md"),
                                            new byte[] {(byte) 0xc3, 0x28});
                                    return new MemoryAdmission.Decision(true, "test-v1", Map.of());
                                });
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            assertThrows(
                    UncheckedIOException.class,
                    () ->
                            flush(
                                    manager,
                                    model,
                                    config(2000).admission(gate).build(),
                                    List.of(source("a", "source evidence"))));
            assertNull(manager.atomicMemoryFile(rc, ledger()).read());
        }
        Files.writeString(workspace.resolve("MEMORY.md"), "- old fact");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(manager, model, config(2000).admission(gate).build(), List.of());
            assertTrue(manager.atomicMemoryFile(rc, ledger()).read().contains("approved fact"));
        }
        assertEquals(1, calls.size());
        assertEquals(1, judgments.get());
    }

    @Test
    void admissionErrorDoesNotBecomeSuccessfulNoReply() {
        Model model = model("- candidate");
        AtomicInteger attempts = new AtomicInteger();
        MemoryAdmission gate =
                input ->
                        attempts.getAndIncrement() == 0
                                ? Mono.error(new IllegalStateException("judge unavailable"))
                                : Mono.just(
                                        new MemoryAdmission.Decision(false, "test-v1", Map.of()));
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            List<Msg> input = List.of(source("a", "source evidence"));
            assertThrows(
                    IllegalStateException.class,
                    () -> flush(manager, model, config(2000).admission(gate).build(), input));
            assertNull(manager.atomicMemoryFile(rc, ledger()).read());
            MemoryFlushManager.configured(manager, model, config(2000).admission(gate).build())
                    .retryDeferredMemories(rc, input)
                    .block();
            flush(manager, model, config(2000).admission(gate).build(), input);
            assertEquals(2, calls.size());
            assertEquals(2, attempts.get());
        }
    }

    @Test
    void preparedDecisionSurvivesRestartWithoutRejudgingOrRegenerating() {
        Model model = model("- approved fact");
        AtomicInteger judgments = new AtomicInteger();
        MemoryAdmission gate =
                input -> {
                    judgments.incrementAndGet();
                    return Mono.just(
                            new MemoryAdmission.Decision(true, "rule-v1", Map.of("durable", 0.95)));
                };
        AtomicBoolean loseAck = new AtomicBoolean(true);
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        AtomicTextFile delegate = super.atomicMemoryFile(context, path);
                        if (!path.endsWith(".json")) return delegate;
                        return new AtomicTextFile() {
                            public String read() {
                                return delegate.read();
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                boolean changed = delegate.compareAndSet(expected, updated);
                                if (changed
                                        && updated.contains("approved fact")
                                        && loseAck.getAndSet(false)) {
                                    assertTrue(updated.contains("approved fact"));
                                    throw new IllegalStateException("lost journal acknowledgement");
                                }
                                return changed;
                            }
                        };
                    }
                }) {
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            flush(
                                    manager,
                                    model,
                                    config(2000).admission(gate).build(),
                                    List.of(source("a", "evidence"))));
        }
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            flush(
                    manager,
                    model,
                    config(2000)
                            .admission(
                                    input ->
                                            Mono.error(
                                                    new AssertionError(
                                                            "must replay prepared decision")))
                            .build(),
                    List.of());
            assertTrue(manager.atomicMemoryFile(rc, ledger()).read().contains("approved fact"));
        }
        assertEquals(1, judgments.get());
        assertEquals(1, calls.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void pendingOutputWithOrWithoutAdmissionMetadataRecovers(boolean metadata) throws Exception {
        Model model = model("NO_REPLY");
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            new MemoryFlushManager(manager, model, null)
                    .flushMemories(rc, List.of(source("legacy", "evidence")))
                    .block();
            Path statePath;
            try (var paths = Files.list(manager.getMemoryDir(rc).resolve(".flush-state"))) {
                statePath =
                        paths.filter(path -> path.toString().endsWith(".json"))
                                .findFirst()
                                .orElseThrow();
            }
            AtomicTextFile journal =
                    manager.atomicMemoryFile(rc, "memory/.flush-state/" + statePath.getFileName());
            String before = journal.read();
            Map<String, Object> state =
                    new LinkedHashMap<>(JsonUtils.getJsonCodec().fromJson(before, Map.class));
            Map<String, Object> pending =
                    new LinkedHashMap<>(
                            Map.of(
                                    "identities",
                                    List.of("legacy:0"),
                                    "dailyPath",
                                    ledger(),
                                    "id",
                                    "legacy-delivery",
                                    "timestamp",
                                    "2026-01-01T00:00:00Z",
                                    "output",
                                    "- legacy fact"));
            if (metadata) {
                pending.put("sourceDigest", "old-source-digest");
                pending.put("memoryRevision", "old-memory-digest");
                pending.put("admissions", List.of(Map.of("candidate", "- legacy fact")));
            }
            state.put("pending", pending);
            assertTrue(journal.compareAndSet(before, JsonUtils.getJsonCodec().toJson(state)));
            flush(
                    manager,
                    model,
                    config(2000)
                            .admission(input -> Mono.error(new AssertionError("already prepared")))
                            .build(),
                    List.of());
            assertTrue(manager.atomicMemoryFile(rc, ledger()).read().contains("legacy fact"));
            assertEquals(1, calls.size());
        }
    }
}
