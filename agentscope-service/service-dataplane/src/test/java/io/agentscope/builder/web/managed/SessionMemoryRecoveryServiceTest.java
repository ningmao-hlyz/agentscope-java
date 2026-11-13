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
package io.agentscope.builder.web.managed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.builder.control.ControlPlaneClient;
import io.agentscope.builder.control.SessionResolveResult;
import io.agentscope.builder.web.catalog.HarnessAgentBuildService;
import io.agentscope.builder.web.managed.service.DeletedSessionRegistry;
import io.agentscope.builder.web.persistence.jpa.SessionExportSourceEntity;
import io.agentscope.builder.web.persistence.jpa.SessionExportSourceRepository;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.HarnessAgent;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Mono;

class SessionMemoryRecoveryServiceTest {
    private final SessionExportSourceRepository sources = mock(SessionExportSourceRepository.class);
    private final ControlPlaneClient control = mock(ControlPlaneClient.class);
    private final HarnessAgentBuildService agents = mock(HarnessAgentBuildService.class);
    private final DeletedSessionRegistry deleted = new DeletedSessionRegistry();
    private final Clock clock = mock(Clock.class);
    private final HarnessAgent agent = mock(HarnessAgent.class);

    @BeforeEach
    void configure() {
        when(agent.recoverMemory(any())).thenReturn(Mono.empty());
        when(agents.buildMemoryRecoveryAgent(any())).thenReturn(agent);
        when(control.resolveSession(anyString(), anyString()))
                .thenAnswer(call -> resolved(call.getArgument(0)));
    }

    private static SessionResolveResult resolved(String id) {
        var session =
                new ManagedSessionDto(
                        id,
                        "alice",
                        "catalog-agent",
                        "alice",
                        1,
                        "latest",
                        null,
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
        return new SessionResolveResult(
                session,
                Map.of("name", "test"),
                null,
                null,
                null,
                Map.of(),
                null,
                null,
                null,
                null,
                null);
    }

    private static SessionExportSourceEntity source(String id) {
        var source = new SessionExportSourceEntity();
        source.sessionId = id;
        source.userId = "alice";
        source.agentId = HarnessAgentBuildService.sessionLogAgentId(resolved(id).session());
        return source;
    }

    private SessionMemoryRecoveryService worker() {
        return new SessionMemoryRecoveryService(sources, control, agents, deleted, 1000, 2, clock);
    }

    private void batch(SessionExportSourceEntity... entries) {
        when(sources.findByParentSessionIdIsNull(any()))
                .thenReturn(new PageImpl<>(List.of(entries)));
    }

    @Test
    void pollingIsOptInAndLifecycleOwned() {
        var runner =
                new ApplicationContextRunner()
                        .withUserConfiguration(SessionMemoryRecoveryService.class)
                        .withInitializer(
                                context -> {
                                    var beans = context.getBeanFactory();
                                    beans.registerSingleton("sources", sources);
                                    beans.registerSingleton("control", control);
                                    beans.registerSingleton("agents", agents);
                                    beans.registerSingleton("deleted", deleted);
                                });
        runner.run(
                context -> assertThat(context).doesNotHaveBean(SessionMemoryRecoveryService.class));
        runner.withPropertyValues("builder.memory-recovery.enabled=true")
                .run(
                        context ->
                                assertThat(context)
                                        .hasSingleBean(SessionMemoryRecoveryService.class));
        verify(sources, never()).findByParentSessionIdIsNull(any());
    }

    @Test
    void timerRecoversWithoutAnIncomingRequestAndStopCancelsInFlightWork() throws Exception {
        batch(source("dormant"));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        when(agent.recoverMemory(any()))
                .thenReturn(
                        Mono.<Void>never()
                                .doOnSubscribe(subscription -> started.countDown())
                                .doOnCancel(cancelled::countDown));
        doAnswer(
                        call -> {
                            closed.countDown();
                            return null;
                        })
                .when(agent)
                .close();
        var worker = worker();
        try {
            worker.start();
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            worker.stop();
        }
        assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
        verify(agent).recoverMemory(any());
    }

    @Test
    void failureBackoffDoesNotBlockOtherPagesAndRestartRetries() {
        var firstPage = PageRequest.of(0, 2, Sort.by("sessionId"));
        var secondPage = PageRequest.of(1, 2, Sort.by("sessionId"));
        when(sources.findByParentSessionIdIsNull(firstPage))
                .thenReturn(new PageImpl<>(List.of(source("bad"), source("good")), firstPage, 3));
        when(sources.findByParentSessionIdIsNull(secondPage))
                .thenReturn(new PageImpl<>(List.of(source("later")), secondPage, 3));
        when(control.resolveSession("bad", "alice"))
                .thenThrow(new IllegalStateException("offline"));
        var worker = worker();
        worker.recoverOnce();
        worker.recoverOnce();
        worker.recoverOnce();
        verify(control).resolveSession("bad", "alice");
        verify(control, times(2)).resolveSession("good", "alice");
        verify(control).resolveSession("later", "alice");
        worker.recoverOnce();
        when(clock.millis()).thenReturn(60_000L);
        worker.recoverOnce();
        verify(control, times(2)).resolveSession("bad", "alice");
        worker().recoverOnce();
        verify(control, times(3)).resolveSession("bad", "alice");
    }

    @Test
    void deletedAndMismatchedSourcesNeverBuildAnAgent() {
        var mismatch = source("mismatch");
        mismatch.userId = "another-owner";
        batch(source("deleted"), mismatch);
        deleted.markDeleted("deleted");
        worker().recoverOnce();
        verify(control, never()).resolveSession("deleted", "alice");
        verify(agents, never()).buildMemoryRecoveryAgent(any());
    }

    @Test
    void eachTemporaryRuntimeClosesAndReceivesTheSourceIdentity() {
        batch(source("first"), source("second"));
        worker().recoverOnce();
        var contexts = org.mockito.ArgumentCaptor.forClass(RuntimeContext.class);
        verify(agent, times(2)).recoverMemory(contexts.capture());
        assertThat(contexts.getAllValues())
                .extracting(RuntimeContext::getUserId)
                .containsOnly("alice");
        assertThat(contexts.getAllValues())
                .extracting(RuntimeContext::getSessionId)
                .containsExactly("first", "second");
        verify(agent, times(2)).close();
    }
}
