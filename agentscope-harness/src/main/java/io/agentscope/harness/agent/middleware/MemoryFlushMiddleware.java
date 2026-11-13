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

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.model.Model;
import io.agentscope.core.state.AgentState;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.coordination.LocalPeriodicGate;
import io.agentscope.harness.agent.coordination.PeriodicGate;
import io.agentscope.harness.agent.memory.MemoryBackgroundTasks;
import io.agentscope.harness.agent.memory.MemoryConfig;
import io.agentscope.harness.agent.memory.MemoryFlushManager;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

/**
 * Middleware that triggers memory extraction at the end of each agent call.
 *
 * <p>Runs on the agent stream's completion signal ({@link #onAgent}) so long-term memories are
 * extracted and persisted after every call, even when conversation compaction was not triggered
 * during that call. The flush is <em>fire-and-forget</em>: the agent stream completes immediately
 * while the extraction runs on a background scheduler. The completing subscription's Reactor
 * context is replayed on that background subscription, so context-scoped instrumentation (token
 * metering, tracing) on the model call sees the same context as the turn. When
 * native history is enabled, extraction reads committed session events through an independent
 * cursor, including original messages removed from working context by compaction. Legacy
 * execution can recover removed source only after its failed batch has been durably saved.
 *
 * <p>Flush is gated by a {@link MemoryConfig.FlushTrigger}:
 * <ul>
 *   <li>{@link MemoryConfig.FlushMode#ALWAYS} (default) — flush after every call.</li>
 *   <li>{@link MemoryConfig.FlushMode#NEVER} — never flush via this middleware. The CompactionMiddleware
 *       and overflow-recovery paths still run their own flush when they fire.</li>
 *   <li>{@link MemoryConfig.FlushMode#THROTTLED} — flush at most once per
 *       {@link MemoryConfig.FlushTrigger#minGap()}.</li>
 * </ul>
 *
 * <p>Native session logging runs independently of memory extraction, so disabling memory
 * flush does not affect durable execution history.
 *
 * <p>Concurrent flushes for the same isolation key are serialised: at most one flush runs per
 * key at a time, and pending flushes of the same conversation coalesce into a single queued
 * task, because a queued task reads the conversation state only when it executes and the
 * newest task therefore subsumes the ones it replaces. Flushes of distinct conversations
 * under the same key are queued separately.
 *
 * <p>The class is thread-safe: instances hold only configuration, and the per-key queues live
 * in a static map and are synchronised internally.
 *
 * <p>Throttle and queue keys include the workspace root and isolation identity.
 */
public class MemoryFlushMiddleware implements HarnessRuntimeMiddleware {

    private static final Logger log = LoggerFactory.getLogger(MemoryFlushMiddleware.class);

    private final WorkspaceManager workspaceManager;
    private final Model model;
    private final MemoryConfig memoryConfig;
    private final MemoryConfig.FlushTrigger flushTrigger;
    private final IsolationScope isolationScope;
    private final PeriodicGate periodicGate;

    public MemoryFlushMiddleware(WorkspaceManager workspaceManager, Model model) {
        this(
                workspaceManager,
                model,
                MemoryFlushManager.DEFAULT_FLUSH_PROMPT,
                MemoryConfig.FlushTrigger.always(),
                IsolationScope.USER,
                new LocalPeriodicGate());
    }

    public MemoryFlushMiddleware(
            WorkspaceManager workspaceManager,
            Model model,
            String flushPrompt,
            MemoryConfig.FlushTrigger flushTrigger) {
        this(
                workspaceManager,
                model,
                flushPrompt,
                flushTrigger,
                IsolationScope.USER,
                new LocalPeriodicGate());
    }

    public MemoryFlushMiddleware(
            WorkspaceManager workspaceManager,
            Model model,
            String flushPrompt,
            MemoryConfig.FlushTrigger flushTrigger,
            IsolationScope isolationScope) {
        this(
                workspaceManager,
                model,
                flushPrompt,
                flushTrigger,
                isolationScope,
                new LocalPeriodicGate());
    }

    public MemoryFlushMiddleware(
            WorkspaceManager workspaceManager,
            Model model,
            String flushPrompt,
            MemoryConfig.FlushTrigger flushTrigger,
            IsolationScope isolationScope,
            PeriodicGate periodicGate) {
        this(
                workspaceManager,
                model,
                MemoryConfig.builder()
                        .flushPrompt(flushPrompt)
                        .flushTrigger(
                                flushTrigger != null
                                        ? flushTrigger
                                        : MemoryConfig.FlushTrigger.always())
                        .build(),
                isolationScope,
                periodicGate);
    }

    public MemoryFlushMiddleware(
            WorkspaceManager workspaceManager,
            Model model,
            MemoryConfig memoryConfig,
            IsolationScope isolationScope,
            PeriodicGate periodicGate) {
        this.workspaceManager = workspaceManager;
        this.model = model;
        this.memoryConfig = memoryConfig;
        this.flushTrigger = memoryConfig.flushTrigger();
        this.isolationScope = isolationScope != null ? isolationScope : IsolationScope.USER;
        this.periodicGate = periodicGate != null ? periodicGate : new LocalPeriodicGate();
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
        // Capture the completing subscription's context so the fire-and-forget flush can replay
        // it; a fresh background subscription does not inherit it, and context-scoped
        // instrumentation (token usage, tracing) on the model call would otherwise see nothing.
        return next.apply(input)
                .doOnEach(
                        signal -> {
                            if (signal.isOnComplete()) {
                                scheduleFlush(agent, rc, signal.getContextView());
                            }
                        });
    }

    private void scheduleFlush(Agent agent, RuntimeContext rc, ContextView contextView) {
        // The in-flight slot is acquired at dispatch rather than inside the task: a queued
        // flush must already be counted, or a quiescence check could observe an empty
        // in-flight set while work is still pending.
        MemoryBackgroundTasks.begin();
        String key = compositeTimerKey(rc);
        // Coalescing key of the task: the conversation whose state the task reads when it
        // executes. The agent reference distinguishes agent instances serving the same user
        // and session ids, whose flushes must not displace each other.
        ConversationKey conversationKey =
                new ConversationKey(
                        agent, blankToEmpty(rc.getUserId()), blankToEmpty(rc.getSessionId()));
        Runnable task = () -> runFlush(key, agent, rc, contextView);
        Runnable[] starter = new Runnable[1];
        Runnable[] displaced = new Runnable[1];
        FLUSH_QUEUES.compute(
                key,
                (k, queue) -> {
                    FlushQueue q = queue == null ? new FlushQueue() : queue;
                    if (q.running) {
                        // A queued task reads the conversation state only when it executes, and
                        // calls of one conversation are serialised on the agent's
                        // (userId, sessionId) serialization key, so the newest task's state
                        // includes everything the replaced tasks would have read.
                        displaced[0] = q.pending.put(conversationKey, task);
                    } else {
                        q.running = true;
                        starter[0] = task;
                    }
                    return q;
                });
        if (displaced[0] != null) {
            // The replaced task will never execute; release the in-flight slot it acquired at
            // dispatch. Done outside compute to avoid running under the map's bin lock.
            MemoryBackgroundTasks.end();
        }
        if (starter[0] != null) {
            starter[0].run();
        }
    }

    private void runFlush(String key, Agent agent, RuntimeContext rc, ContextView contextView) {
        Mono.defer(() -> doFlush(agent, rc))
                .subscribeOn(Schedulers.boundedElastic())
                .contextWrite(context -> context.putAll(contextView))
                .doFinally(
                        signal -> {
                            MemoryBackgroundTasks.end();
                            drainFlushQueue(key);
                        })
                .subscribe(null, e -> log.warn("Memory flush failed: {}", e.getMessage()));
    }

    private void drainFlushQueue(String key) {
        Runnable[] next = new Runnable[1];
        FLUSH_QUEUES.compute(
                key,
                (k, queue) -> {
                    if (queue == null) {
                        return null;
                    }
                    Iterator<Runnable> it = queue.pending.values().iterator();
                    if (it.hasNext()) {
                        next[0] = it.next();
                        it.remove();
                    }
                    if (next[0] == null) {
                        return null; // idle: evict so the map holds only active keys
                    }
                    return queue; // the dequeued task continues as the running flush
                });
        if (next[0] != null) {
            next[0].run();
        }
    }

    /**
     * Per-isolation-key pending flush tasks, keeping at most one flush in flight per memory
     * namespace. Fields are only mutated inside {@link ConcurrentHashMap#compute} (which holds
     * the per-key bin lock), so they need no additional synchronisation; entries are removed
     * as soon as a key goes idle.
     */
    private static final class FlushQueue {
        final LinkedHashMap<ConversationKey, Runnable> pending = new LinkedHashMap<>();
        boolean running;
    }

    private static final ConcurrentHashMap<String, FlushQueue> FLUSH_QUEUES =
            new ConcurrentHashMap<>();

    private Mono<Void> doFlush(Agent agent, RuntimeContext rc) {
        if (!shouldFlushNow(rc)) {
            log.debug("Memory flush skipped (trigger={})", flushTrigger);
            return Mono.empty();
        }

        Model effective = effectiveFlushModel(agent, rc);
        if (effective == null) {
            // No dedicated model and the agent cannot resolve a per-call one (non-ReActAgent
            // direct construction): nothing to extract memories with.
            return Mono.empty();
        }
        MemoryFlushManager flushManager =
                MemoryFlushManager.configured(workspaceManager, effective, memoryConfig);
        Mono<Void> flush;
        if (agent instanceof ReActAgent react && react.sessionLogEnabled()) {
            var source = react.sessionLog(rc);
            flush =
                    flushManager.flushSessionMemories(
                            rc,
                            source,
                            source.sourceId() != null
                                    ? source.sourceId()
                                    : react.sessionKey(rc).storagePath());
        } else {
            AgentState state = RuntimeContext.resolveAgentState(rc, agent);
            if (state == null) return Mono.empty();
            // Read on execution so a coalesced task includes all newly appended messages.
            flush = flushManager.flushMemories(rc, new ArrayList<>(state.getContext()));
        }
        return flush.doOnSuccess(v -> log.debug("Memory flush completed"))
                .onErrorResume(
                        e -> {
                            log.warn("Memory flush failed: {}", e.getMessage());
                            return Mono.empty();
                        });
    }

    /**
     * Resolves the model used for memory extraction: a dedicated flush model when configured,
     * else the executing {@link ReActAgent}'s per-call model (its {@link RuntimeContext} model id
     * when set, else its build-time default), so flush follows the model actually serving the
     * conversation. Package-private for unit testing.
     */
    Model effectiveFlushModel(Agent agent, RuntimeContext rc) {
        if (model != null) {
            return model;
        }
        return agent instanceof ReActAgent react ? react.getModel(rc) : null;
    }

    /**
     * Returns whether this call should trigger a flush, applying the configured trigger policy.
     * For {@link MemoryConfig.FlushMode#THROTTLED}, delegates the window claim to {@link PeriodicGate}.
     *
     * <p>The throttle window is keyed by the isolation dimension that matches the memory data
     * namespace (see {@link #timerKeyFor(RuntimeContext)}).
     *
     */
    boolean shouldFlushNow(RuntimeContext rc) {
        switch (flushTrigger.mode()) {
            case ALWAYS:
                return true;
            case NEVER:
                return false;
            case THROTTLED:
                return periodicGate.tryClaim(compositeTimerKey(rc), flushTrigger.minGap());
            default:
                return true;
        }
    }

    /**
     * Separates workspaces and isolation identities; the operation prefix keeps flush independent
     * of maintenance when sharing a gate.
     */
    private String compositeTimerKey(RuntimeContext rc) {
        var workspace = workspaceManager != null ? workspaceManager.getWorkspace() : null;
        String root = workspace != null ? workspace.toAbsolutePath().normalize().toString() : "";
        return "memory-flush:" + isolationScope.name() + ":" + root + ":" + timerKeyFor(rc);
    }

    /** The conversation a queued flush reads when it executes: agent instance, user, session. */
    private record ConversationKey(Agent agent, String userId, String sessionId) {}

    /**
     * Normalises {@code null} and blank identifiers to {@code ""} so absent ids share one slot.
     * Package-private: shared with {@link MemoryMaintenanceMiddleware#timerKeyFor}.
     */
    static String blankToEmpty(String s) {
        return (s != null && !s.isBlank()) ? s : "";
    }

    /**
     * Derives the per-call identity portion of the composite timer key from the configured
     * {@link IsolationScope} and the {@link RuntimeContext}, mirroring the memory data
     * namespace:
     * <ul>
     *   <li>{@link IsolationScope#USER} — {@code userId} (empty string for anonymous)</li>
     *   <li>{@link IsolationScope#SESSION} — {@code sessionId} (empty string when absent)</li>
     *   <li>{@link IsolationScope#AGENT} / {@link IsolationScope#GLOBAL} — constant {@code ""}
     *       so all callers share one throttle slot, serialising flushes on shared memory files</li>
     * </ul>
     */
    String timerKeyFor(RuntimeContext rc) {
        return switch (isolationScope) {
            case USER -> blankToEmpty(rc != null ? rc.getUserId() : null);
            case SESSION -> blankToEmpty(rc != null ? rc.getSessionId() : null);
            case AGENT, GLOBAL -> "";
        };
    }
}
