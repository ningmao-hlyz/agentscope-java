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
package io.agentscope.builder.web.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.agentscope.builder.control.SessionResolveResult;
import io.agentscope.builder.web.config.BuilderE2bProperties;
import io.agentscope.builder.web.managed.EnvironmentDto;
import io.agentscope.builder.web.managed.EnvironmentSpecFactory;
import io.agentscope.builder.web.managed.ManagedSessionDto;
import io.agentscope.builder.web.workspace.SharedWorkspacePaths;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ModelRegistry;
import io.agentscope.core.session.SessionRecorder;
import io.agentscope.core.state.AgentStateStore;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.session.WorkspaceSessionLogStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;

class HarnessMemoryRecoveryBuildTest {
    @TempDir Path workspace;

    @ParameterizedTest
    @ValueSource(strings = {"local", "remote", "sandbox", "self_hosted"})
    void recoversInOriginalNamespaceWithModelOverrideWithoutToolsOrSandboxExecution(String type) {
        String sessionId = UUID.randomUUID().toString();
        String modelId = "memory-recovery-" + sessionId;
        Model fallback = mock(Model.class);
        Model memory = mock(Model.class);
        when(memory.getModelName()).thenReturn(modelId);
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
        ModelRegistry.register(modelId, memory);
        var session =
                new ManagedSessionDto(
                        sessionId,
                        "alice",
                        "catalog-agent",
                        "alice",
                        1,
                        "latest",
                        "{\"model\":\"" + modelId + "\"}",
                        null,
                        null,
                        null,
                        null,
                        null,
                        "idle",
                        null,
                        0,
                        0,
                        null);
        var environment =
                new EnvironmentDto(
                        "env",
                        "test",
                        type,
                        Map.of("apiKey", "unused-test-key", "isolationScope", "SESSION"),
                        "alice",
                        null,
                        0,
                        0,
                        null);
        var resolved =
                new SessionResolveResult(
                        session,
                        Map.of("name", "test", "model", "overridden-invalid-model"),
                        null,
                        null,
                        null,
                        Map.of(),
                        environment,
                        null,
                        null,
                        null,
                        null);
        AgentStateStore states = mock(AgentStateStore.class);
        var environments =
                new EnvironmentSpecFactory(
                        Optional.of(new InMemoryStore()),
                        Optional.of(states),
                        new BuilderE2bProperties());
        var paths = new SharedWorkspacePaths(workspace);
        var factory =
                new HarnessAgentBuildService(
                        Optional.of(fallback),
                        null,
                        paths,
                        environments,
                        null,
                        null,
                        null,
                        states,
                        null,
                        null,
                        null);
        var logs =
                new WorkspaceSessionLogStore(new LocalFilesystem(workspace.resolve("native-logs")));
        factory.setSessionLogStore(logs);
        RuntimeContext rc = RuntimeContext.builder().userId("alice").sessionId(sessionId).build();
        try {
            var builder =
                    HarnessAgent.builder()
                            .agentId(HarnessAgentBuildService.sessionLogAgentId(session))
                            .name("test")
                            .workspace(paths.resolveSessionDataPath("alice", sessionId))
                            .model(memory)
                            .sessionLogStore(logs)
                            .stateStore(states)
                            .disableSubagents()
                            .disableWebTools()
                            .disableToolsConfig();
            environments.applyEnvironment(builder, environment);
            try (HarnessAgent original = builder.build()) {
                var recorder = new SessionRecorder(original.sessionLog(rc), "original");
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
            try (HarnessAgent recovery = factory.buildMemoryRecoveryAgent(resolved)) {
                recovery.recoverMemory(rc).block(Duration.ofSeconds(10));
                assertThat(recovery.sessionLog(rc).exportCursor("agentscope.memory-flush.v1"))
                        .isEqualTo(recovery.sessionLog(rc).head().seq());
                assertThat(recovery.getWorkspaceManager().readMemoryMd(rc))
                        .contains("Alice prefers Java");
                recovery.recoverMemory(rc).block(Duration.ofSeconds(10));
            }
            verify(memory, times(2)).stream(anyList(), any(), any());
            verifyNoInteractions(fallback);
            verifyNoInteractions(states);
        } finally {
            ModelRegistry.unregister(modelId);
        }
    }
}
