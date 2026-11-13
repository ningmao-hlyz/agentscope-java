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
package io.agentscope.harness.agent.middleware;

import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.coordination.LocalPeriodicGate;
import io.agentscope.harness.agent.coordination.PeriodicGate;
import io.agentscope.harness.agent.filesystem.model.FileInfo;
import io.agentscope.harness.agent.memory.MemoryBackgroundTasks;
import io.agentscope.harness.agent.memory.MemoryConsolidator;
import io.agentscope.harness.agent.workspace.WorkspaceConstants;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.time.Duration;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

/**
 * Middleware that performs periodic memory maintenance after each agent call.
 *
 * <p>Fires on the agent stream's completion signal (after {@link MemoryFlushMiddleware}) and is
 * throttled by a configurable minimum gap so it does not run on every single call. The
 * maintenance is <em>fire-and-forget</em>: the agent stream completes immediately while the
 * maintenance runs on a background scheduler. The completing subscription's Reactor context is
 * replayed on that background subscription, so context-scoped instrumentation (token metering,
 * tracing) on the consolidation model call sees the same context as the turn.
 *
 * <p>Maintenance steps executed in order:
 * <ol>
 *   <li>Archive expired daily files; when consolidation is enabled, retain unfinished files.</li>
 *   <li>Run LLM-based consolidation ({@link MemoryConsolidator#consolidate}) if a
 *       consolidator is configured.</li>
 * </ol>
 *
 * <p>The throttle window is tracked per <em>isolation key</em>, which matches the memory data
 * isolation in use:
 * <ul>
 *   <li>{@link IsolationScope#USER} (default) — one window per {@code userId}.</li>
 *   <li>{@link IsolationScope#SESSION} — one window per {@code sessionId}.</li>
 *   <li>{@link IsolationScope#AGENT} / {@link IsolationScope#GLOBAL} — one shared window for
 *       the whole agent instance (prevents concurrent maintenance races on shared memory files).</li>
 * </ul>
 */
public class MemoryMaintenanceMiddleware implements HarnessRuntimeMiddleware {

    private static final Logger log = LoggerFactory.getLogger(MemoryMaintenanceMiddleware.class);

    /** Default minimum gap between two maintenance runs. */
    public static final Duration DEFAULT_MIN_GAP = Duration.ofMinutes(30);

    private final WorkspaceManager workspaceManager;
    private final MemoryConsolidator consolidator;
    private final int dailyFileRetentionDays;
    private final Duration minGap;
    private final IsolationScope isolationScope;
    private final PeriodicGate periodicGate;

    public MemoryMaintenanceMiddleware(
            WorkspaceManager workspaceManager,
            MemoryConsolidator consolidator,
            int dailyFileRetentionDays,
            Duration minGap) {
        this(
                workspaceManager,
                consolidator,
                dailyFileRetentionDays,
                minGap,
                IsolationScope.USER,
                new LocalPeriodicGate());
    }

    public MemoryMaintenanceMiddleware(
            WorkspaceManager workspaceManager,
            MemoryConsolidator consolidator,
            int dailyFileRetentionDays,
            Duration minGap,
            IsolationScope isolationScope) {
        this(
                workspaceManager,
                consolidator,
                dailyFileRetentionDays,
                minGap,
                isolationScope,
                new LocalPeriodicGate());
    }

    public MemoryMaintenanceMiddleware(
            WorkspaceManager workspaceManager,
            MemoryConsolidator consolidator,
            int dailyFileRetentionDays,
            Duration minGap,
            IsolationScope isolationScope,
            PeriodicGate periodicGate) {
        this.workspaceManager = workspaceManager;
        this.consolidator = consolidator;
        this.dailyFileRetentionDays = dailyFileRetentionDays;
        this.minGap = minGap != null ? minGap : DEFAULT_MIN_GAP;
        this.isolationScope = isolationScope != null ? isolationScope : IsolationScope.USER;
        this.periodicGate = periodicGate != null ? periodicGate : new LocalPeriodicGate();
    }

    public MemoryMaintenanceMiddleware(
            WorkspaceManager workspaceManager, MemoryConsolidator consolidator) {
        this(workspaceManager, consolidator, 90, DEFAULT_MIN_GAP);
    }

    /** Narrow declaration: subclasses overriding more hooks must extend this set. */
    @Override
    public Set<ExtensionPoint> activePoints() {
        return EnumSet.of(ExtensionPoint.ON_AGENT);
    }

    @Override
    public Flux<AgentEvent> onAgent(
            Agent agent,
            RuntimeContext ctx,
            AgentInput input,
            Function<AgentInput, Flux<AgentEvent>> next) {
        final RuntimeContext rc = ctx != null ? ctx : RuntimeContext.empty();
        // The maintenance body — including the gate claim, which is remote I/O under a
        // store-backed gate — runs on the background scheduler. Only the in-flight counter is
        // updated synchronously, so a quiescence check can never observe an empty in-flight
        // set before the task is counted.
        return next.apply(input)
                .doOnEach(
                        signal -> {
                            if (signal.isOnComplete()) {
                                ContextView contextView = signal.getContextView();
                                scheduleMaintenance(rc, contextView);
                            }
                        });
    }

    private void scheduleMaintenance(RuntimeContext rc, ContextView contextView) {
        MemoryBackgroundTasks.begin();
        maintain(rc)
                .contextWrite(context -> context.putAll(contextView))
                .doFinally(signal -> MemoryBackgroundTasks.end())
                .subscribe(null, e -> log.warn("Memory maintenance failed: {}", e.getMessage()));
    }

    /** Runs the same throttled maintenance from a host recovery task. */
    public Mono<Void> maintain(RuntimeContext rc) {
        return Mono.defer(() -> doMaintenance(rc)).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Void> doMaintenance(RuntimeContext rc) {
        if (!periodicGate.tryClaim(compositeTimerKey(rc), minGap)) {
            // Throttled out; the in-flight slot acquired at dispatch is released when this
            // Mono completes.
            return Mono.empty();
        }
        // Run as one reactive chain rather than a blocking body: the consolidator's model call
        // must stay inside this subscription so it observes the replayed turn context.
        return Mono.fromRunnable(() -> expireDailyFiles(rc))
                .then(Mono.defer(() -> consolidateMemory(rc)));
    }

    /** Includes the workspace so separate memory roots cannot starve one another. */
    private String compositeTimerKey(RuntimeContext rc) {
        var workspace = workspaceManager != null ? workspaceManager.getWorkspace() : null;
        String root = workspace != null ? workspace.toAbsolutePath().normalize().toString() : "";
        return "memory-maintenance:" + isolationScope.name() + ":" + root + ":" + timerKeyFor(rc);
    }

    /**
     * Derives the per-call identity portion of the composite timer key from the configured
     * {@link IsolationScope} and the {@link RuntimeContext}, mirroring the memory data
     * namespace. See {@link MemoryFlushMiddleware#timerKeyFor(RuntimeContext)} for the
     * identical logic.
     */
    String timerKeyFor(RuntimeContext rc) {
        return switch (isolationScope) {
            case USER -> MemoryFlushMiddleware.blankToEmpty(rc != null ? rc.getUserId() : null);
            case SESSION ->
                    MemoryFlushMiddleware.blankToEmpty(rc != null ? rc.getSessionId() : null);
            case AGENT, GLOBAL -> "";
        };
    }

    private Mono<Void> consolidateMemory(RuntimeContext rc) {
        if (consolidator == null) {
            return Mono.empty();
        }
        return consolidator
                .catchUp(rc)
                .onErrorResume(
                        e -> {
                            log.warn("Memory consolidation failed: {}", e.getMessage());
                            return Mono.empty();
                        });
    }

    private void expireDailyFiles(RuntimeContext rc) {
        LocalDate cutoff = LocalDate.now().minusDays(dailyFileRetentionDays);
        for (FileInfo fi : workspaceManager.listMemoryFileInfos(rc)) {
            if (fi.isDirectory()) {
                continue;
            }
            String fileName = fileName(fi.path());
            if (fileName.startsWith(".")) {
                continue;
            }
            String baseName =
                    fileName.endsWith(".md")
                            ? fileName.substring(0, fileName.length() - 3)
                            : fileName;
            try {
                LocalDate fileDate = LocalDate.parse(baseName);
                if (fileDate.isBefore(cutoff)) {
                    String fromPath = WorkspaceConstants.MEMORY_DIR + "/" + fileName;
                    String snapshot =
                            consolidator == null
                                    ? workspaceManager.atomicMemoryFile(rc, fromPath).read()
                                    : consolidator.consolidatedSnapshot(rc, fi);
                    if (snapshot != null
                            && workspaceManager.archiveMemoryFile(rc, fromPath, snapshot))
                        log.debug("Archived expired daily file: {}", fileName);
                }
            } catch (Exception e) {
                if (!(e instanceof java.time.format.DateTimeParseException))
                    log.warn("Failed to archive daily file {}: {}", fileName, e.getMessage());
            }
        }
    }

    private static String fileName(String path) {
        if (path == null) {
            return "";
        }
        int slash = path.lastIndexOf('/');
        return slash >= 0 ? path.substring(slash + 1) : path;
    }
}
