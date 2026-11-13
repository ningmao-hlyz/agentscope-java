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
package io.agentscope.extensions.judge.jev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.extensions.judge.jev.application.JevMemoryGate;
import io.agentscope.harness.agent.memory.MemoryAdmission;
import io.agentscope.harness.agent.memory.MemoryConfig;
import io.agentscope.harness.agent.memory.MemoryFlushManager;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class JevMemoryAdmissionTest {
    @TempDir Path workspace;

    private final MemoryAdmission.Input input =
            new MemoryAdmission.Input(
                    "owner", "Use Java 17", "The user requested Java 17", List.of("Use Java 11"));

    private MemoryAdmission gate(double durable, double conflict) {
        return gate(durable, conflict, request -> {}).admission();
    }

    private JevMemoryGate gate(
            double durable, double conflict, Consumer<SystemOneRequest> observer) {
        return new JevMemoryGate(
                new JevJudge(
                        request -> {
                            observer.accept(request);
                            return Mono.just(
                                    new SystemOneResult(
                                            "fake",
                                            Map.of(
                                                    "durable",
                                                    new NoulAnswer(durable),
                                                    "conflict",
                                                    new NoulAnswer(conflict)),
                                            null));
                        },
                        Duration.ofSeconds(1)));
    }

    @Test
    void legacyAssessKeepsOriginalQuestionsAndVersion() {
        List<SystemOneRequest> requests = new ArrayList<>();
        var result =
                gate(0.9, 0.1, requests::add)
                        .assess(
                                input.owner(),
                                new JevMemoryGate.Fact(
                                        input.owner(), input.candidate(), input.source()),
                                List.of())
                        .block();
        assertEquals("memory-admission-v1", result.definitionVersion());
        assertEquals(JevJudge.Status.PASS, result.status());
        assertEquals(1, requests.size());
        assertEquals(
                Map.of(
                        "durable",
                        new NoulQuestion(
                                "Is the candidate a useful durable fact supported by its source"
                                        + " rather than an instruction or temporary speculation?",
                                null),
                        "conflict",
                        new NoulQuestion(
                                "Does the candidate contradict an existing fact without clear"
                                        + " evidence that it supersedes it?",
                                null)),
                requests.get(0).questions());
    }

    @Test
    void adapterUsesHarnessPolicyAndForwardsSourceAndExistingFacts() {
        List<SystemOneRequest> requests = new ArrayList<>();
        var admitted = gate(0.9, 0.1, requests::add).admission().assess(input).block();
        assertTrue(admitted.accepted());
        assertEquals("harness-memory-admission-v1", admitted.policyVersion());
        assertEquals(1, requests.size());
        assertEquals(
                Map.of(
                        "candidate",
                        new JevMemoryGate.Fact(input.owner(), input.candidate(), input.source()),
                        "existing",
                        List.of(
                                new JevMemoryGate.Fact(
                                        input.owner(), "Use Java 11", "existing memory snapshot"))),
                requests.get(0).state());
        assertEquals(
                Map.of(
                        "durable",
                        new NoulQuestion(
                                "Does the source support the candidate as a fact, preference, rule"
                                    + " or decision useful beyond the current one-off task, rather"
                                    + " than a quote, unconfirmed proposal or speculation?",
                                null),
                        "conflict",
                        new NoulQuestion(
                                "Does the candidate contradict a supplied existing fact without"
                                    + " explicit source evidence of a correction or an already"
                                    + " effective change for the same subject and scope? A newer"
                                    + " statement alone is not evidence of supersession; quoted"
                                    + " claims and unconfirmed proposals do not establish a"
                                    + " change.",
                                null)),
                requests.get(0).questions());
    }

    @Test
    void assessRejectsCrossOwnerBeforeCallingTheModel() {
        AtomicInteger calls = new AtomicInteger();
        var gate = gate(0.9, 0.1, request -> calls.incrementAndGet());
        var own = new JevMemoryGate.Fact("owner", "fact", "source");
        var foreign = new JevMemoryGate.Fact("other", "fact", "source");
        assertThrows(
                IllegalArgumentException.class, () -> gate.assess("owner", foreign, List.of()));
        assertThrows(
                IllegalArgumentException.class, () -> gate.assess("owner", own, List.of(foreign)));
        assertEquals(0, calls.get());
    }

    @Test
    void bothRequiredBinaryChecksMustPass() {
        var accepted = gate(0.9, 0.1).assess(input).block();
        assertTrue(accepted.accepted());
        assertEquals("harness-memory-admission-v1", accepted.policyVersion());
        assertEquals(0.9, accepted.probabilities().get("durable"));
        assertEquals(0.9, accepted.probabilities().get("conflict"));
        assertTrue(gate(0.8, 0.2).assess(input).block().accepted());
        assertFalse(gate(0.1, 0.1).assess(input).block().accepted());
        assertFalse(gate(0.9, 0.9).assess(input).block().accepted());
    }

    @Test
    void uncertaintyAndBackendFailureAreRetryableErrors() {
        assertThrows(IllegalStateException.class, () -> gate(0.5, 0.1).assess(input).block());
        assertThrows(IllegalStateException.class, () -> gate(0.9, 0.5).assess(input).block());
        var unavailable =
                new JevMemoryGate(
                                new JevJudge(
                                        request -> Mono.error(new IllegalStateException("offline")),
                                        Duration.ofSeconds(1)))
                        .admission();
        assertThrows(IllegalStateException.class, () -> unavailable.assess(input).block());
    }

    @ParameterizedTest
    @CsvSource({"0.1, true", "0.9, false"})
    void adapterDecisionControlsLedgerAndSurvivesRecreation(double conflict, boolean accepted) {
        AtomicInteger judgments = new AtomicInteger();
        AtomicInteger extractions = new AtomicInteger();
        MemoryAdmission admission =
                gate(0.9, conflict, request -> judgments.incrementAndGet()).admission();
        RuntimeContext rc = RuntimeContext.empty();
        String dailyPath = "memory/" + LocalDate.now() + ".md";
        List<Msg> source = correction();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            manager.writeMemoryFileUtf8(rc, "MEMORY.md", "- Project uses Java 17");
            extractor(manager, admission, extractions).flushMemories(rc, source).block();
            String daily = manager.atomicMemoryFile(rc, dailyPath).read();
            if (accepted) assertTrue(daily.contains("- Project now uses Java 21"));
            else assertNull(daily);
            assertEquals("- Project uses Java 17", manager.readMemoryMd(rc));
        }
        try (WorkspaceManager recreated = new WorkspaceManager(workspace)) {
            extractor(recreated, admission, extractions).flushMemories(rc, source).block();
        }
        assertEquals(1, judgments.get());
        assertEquals(1, extractions.get());
    }

    @Test
    void inconclusiveCorrectionRemainsRetryableAfterRecreation() {
        AtomicInteger extractions = new AtomicInteger();
        RuntimeContext rc = RuntimeContext.empty();
        try (WorkspaceManager manager = new WorkspaceManager(workspace)) {
            manager.writeMemoryFileUtf8(rc, "MEMORY.md", "- Project uses Java 17");
            var extractor = extractor(manager, gate(0.9, 0.5), extractions);
            assertThrows(
                    IllegalStateException.class,
                    () -> extractor.flushMemories(rc, correction()).block());
            assertNull(manager.atomicMemoryFile(rc, "memory/" + LocalDate.now() + ".md").read());
        }
        try (WorkspaceManager recreated = new WorkspaceManager(workspace)) {
            var recovered = extractor(recreated, gate(0.9, 0.1), extractions);
            recovered.flushMemories(rc, correction()).block();
            assertEquals(1, extractions.get());
            assertNull(recreated.atomicMemoryFile(rc, "memory/" + LocalDate.now() + ".md").read());
            recovered.retryDeferredMemories(rc, correction()).block();
            assertTrue(
                    recreated
                            .atomicMemoryFile(rc, "memory/" + LocalDate.now() + ".md")
                            .read()
                            .contains("- Project now uses Java 21"));
        }
        assertEquals(2, extractions.get());
    }

    private static List<Msg> correction() {
        return List.of(
                Msg.builder()
                        .id("java-upgrade")
                        .role(MsgRole.USER)
                        .textContent("This project has upgraded from Java 17 to Java 21.")
                        .build());
    }

    private static MemoryFlushManager extractor(
            WorkspaceManager manager, MemoryAdmission admission, AtomicInteger calls) {
        Model model = mock(Model.class);
        when(model.stream(anyList(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            calls.incrementAndGet();
                            return Flux.just(
                                    ChatResponse.builder()
                                            .content(
                                                    List.of(
                                                            TextBlock.builder()
                                                                    .text(
                                                                            "- Project now uses"
                                                                                    + " Java 21")
                                                                    .build()))
                                            .finishReason("stop")
                                            .build());
                        });
        return MemoryFlushManager.configured(
                manager, model, MemoryConfig.builder().admission(admission).build());
    }
}
