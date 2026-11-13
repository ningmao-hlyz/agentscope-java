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

import io.agentscope.builder.control.ControlPlaneClient;
import io.agentscope.builder.web.catalog.HarnessAgentBuildService;
import io.agentscope.builder.web.managed.service.DeletedSessionRegistry;
import io.agentscope.builder.web.persistence.jpa.SessionExportSourceEntity;
import io.agentscope.builder.web.persistence.jpa.SessionExportSourceRepository;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.HarnessAgent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Opt-in catch-up for dormant Managed sessions, independent of turn and export polling. */
@Service
@ConditionalOnProperty(name = "builder.memory-recovery.enabled", havingValue = "true")
public class SessionMemoryRecoveryService {
    private static final Logger log = LoggerFactory.getLogger(SessionMemoryRecoveryService.class);
    private static final Duration ATTEMPT_TIMEOUT = Duration.ofMinutes(5);
    private final SessionExportSourceRepository sources;
    private final ControlPlaneClient control;
    private final HarnessAgentBuildService agents;
    private final DeletedSessionRegistry deleted;
    private final long pollMs;
    private final int batchSize;
    private final Clock clock;
    private final Map<String, Retry> retries =
            new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Retry> eldest) {
                    return size() > 1024;
                }
            };
    private ScheduledExecutorService executor;
    private int page;

    @Autowired
    public SessionMemoryRecoveryService(
            SessionExportSourceRepository sources,
            ControlPlaneClient control,
            HarnessAgentBuildService agents,
            DeletedSessionRegistry deleted,
            @Value("${builder.memory-recovery.poll-ms:60000}") long pollMs,
            @Value("${builder.memory-recovery.batch-size:10}") int batchSize) {
        this(sources, control, agents, deleted, pollMs, batchSize, Clock.systemUTC());
    }

    SessionMemoryRecoveryService(
            SessionExportSourceRepository sources,
            ControlPlaneClient control,
            HarnessAgentBuildService agents,
            DeletedSessionRegistry deleted,
            long pollMs,
            int batchSize,
            Clock clock) {
        if (pollMs < 1000 || batchSize < 1 || batchSize > 256)
            throw new IllegalArgumentException(
                    "Memory recovery requires poll-ms >= 1000 and batch-size 1..256");
        this.sources = sources;
        this.control = control;
        this.agents = agents;
        this.deleted = deleted;
        this.pollMs = pollMs;
        this.batchSize = batchSize;
        this.clock = clock;
    }

    @PostConstruct
    void start() {
        executor =
                Executors.newSingleThreadScheduledExecutor(
                        task -> {
                            Thread thread = new Thread(task, "session-memory-recovery");
                            thread.setDaemon(true);
                            return thread;
                        });
        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        recoverOnce();
                    } catch (RuntimeException error) {
                        log.warn("Memory recovery scan pending retry: {}", error.toString());
                    }
                },
                pollMs,
                pollMs,
                TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        if (executor != null) executor.shutdownNow();
    }

    synchronized void recoverOnce() {
        var batch =
                sources.findByParentSessionIdIsNull(
                        PageRequest.of(page, batchSize, Sort.by("sessionId")));
        for (var source : batch) {
            if (Thread.currentThread().isInterrupted()) return;
            if (deleted.isDeleted(source.sessionId)) continue;
            Retry retry = retries.get(source.sessionId);
            if (retry != null && clock.millis() < retry.after()) continue;
            try {
                recover(source).block(ATTEMPT_TIMEOUT);
                retries.remove(source.sessionId);
            } catch (RuntimeException error) {
                if (Thread.currentThread().isInterrupted()) return;
                int failures = retry == null ? 1 : Math.min(retry.failures() + 1, 6);
                long delay = Math.min(30, 1L << (failures - 1));
                retries.put(
                        source.sessionId,
                        new Retry(failures, clock.millis() + Duration.ofMinutes(delay).toMillis()));
                log.warn(
                        "Session {} memory recovery retries in {} min: {}",
                        source.sessionId,
                        delay,
                        error.toString());
            }
        }
        page = batch.hasNext() ? page + 1 : 0;
    }

    private Mono<Void> recover(SessionExportSourceEntity source) {
        return Mono.defer(
                        () -> {
                            var resolved = control.resolveSession(source.sessionId, source.userId);
                            var session = resolved.session();
                            if (session == null
                                    || !source.sessionId.equals(session.id())
                                    || !source.userId.equals(session.ownerId())
                                    || !source.agentId.equals(
                                            HarnessAgentBuildService.sessionLogAgentId(session)))
                                return Mono.error(
                                        new IllegalStateException(
                                                "Memory recovery source identity mismatch"));
                            if (deleted.isDeleted(source.sessionId)) return Mono.empty();
                            var context =
                                    RuntimeContext.builder()
                                            .userId(source.userId)
                                            .sessionId(source.sessionId)
                                            .build();
                            return Mono.using(
                                    () -> agents.buildMemoryRecoveryAgent(resolved),
                                    agent ->
                                            agent.recoverMemory(context)
                                                    .publishOn(Schedulers.boundedElastic()),
                                    HarnessAgent::close);
                        })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private record Retry(int failures, long after) {}
}
