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

import io.agentscope.core.agent.AgentBase;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.formatter.DocumentReferenceRenderer;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.DocumentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.middleware.ModelCallInput;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.FinishReasons;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.session.SessionEvent;
import io.agentscope.core.session.SessionLog;
import io.agentscope.core.session.SessionLogException;
import io.agentscope.core.session.SessionRecorder;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.context.ContextBudgetExceededException;
import io.agentscope.harness.agent.context.ContextModelCalls;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.memory.compaction.ConversationCompactor;
import io.agentscope.harness.agent.workspace.WorkspaceConstants;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Extracts unconsumed conversation parts into {@code memory/YYYY-MM-DD.md}, using persistent
 * receipts and recoverable appends. {@code MEMORY.md} is read-only context for this path;
 * {@link MemoryConsolidator} curates it separately.
 */
public class MemoryFlushManager {

    private static final Logger log = LoggerFactory.getLogger(MemoryFlushManager.class);

    /** Default extraction prompt; override through {@link MemoryConfig.Builder#flushPrompt(String)}. */
    public static final String DEFAULT_FLUSH_PROMPT =
            """
            You are a memory extraction assistant. Analyze the conversation below and extract \
            important facts, decisions, preferences, and contextual information that should be \
            remembered for future conversations.

            Output ONLY the extracted memories as a markdown bullet list. Each item should be \
            a concise, self-contained fact. Include dates, names, and specifics when available.

            If there is nothing worth remembering, respond with exactly: NO_REPLY

            Guidelines:
            - Extract user preferences, personal information, project decisions
            - Capture important technical decisions and their rationale
            - Note any commitments, deadlines, or action items
            - Record relationship context (who works on what, team structure)
            - Ignore routine greetings, tool invocations, and ephemeral status updates

            IMPORTANT — write target and append-only rules:
            - You are writing to TODAY'S daily memory ledger (memory/YYYY-MM-DD.md), NOT to \
            MEMORY.md. The daily ledger is append-only — your output will be appended after the \
            entries already shown below.
            - MEMORY.md is the curated long-term memory and is shown ONLY as read-only context. \
            Do NOT restate facts already covered by MEMORY.md or by today's earlier entries; a \
            separate consolidation step periodically merges new daily entries into MEMORY.md.
            - Keep each bullet point independent and self-contained so entries can be searched \
            individually.\
            """;

    private final WorkspaceManager workspaceManager;
    private final Model model;
    private final String flushPrompt;
    private final ContextPolicy policy;
    private final MemoryAdmission admission;
    private final GenerateOptions flushOptions;
    private final Duration flushTimeout;

    static final int MAX_BATCH_CHARS = 24_000;
    static final int MAX_EXISTING_CHARS = 8_000;
    static final int MAX_OUTPUT_CHARS = 16_000;
    static final String SESSION_LOG_SINK = "agentscope.memory-flush.v1";
    private static final int LOG_PAGE_EVENTS = 256;
    private static final int MAX_DEFERRED_BATCHES = 64;
    // Flush blocks on model preparation, which uses the shared bounded-elastic pool.
    private static final Scheduler FLUSH_SCHEDULER =
            Schedulers.newBoundedElastic(
                    Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
                    Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
                    "agentscope-memory-flush",
                    60,
                    true);
    private static final Pattern ENTRY_START = Pattern.compile("^(?:[-*+]\\s+|#{1,6}\\s+|<!--).*");
    private static final Pattern BULLET = Pattern.compile("^[-*+]\\s+.*", Pattern.DOTALL);

    private record Part(String identity, String baseIdentity, int start, int end, String text) {}

    private record Batch(
            List<Part> parts,
            List<Msg> input,
            GenerateOptions options,
            String source,
            List<String> existing) {}

    private record PendingFlush(
            List<String> identities, String dailyPath, String id, String timestamp, String output) {
        PendingFlush {
            if (identities == null
                    || identities.isEmpty()
                    || dailyPath == null
                    || !dailyPath.matches("memory/\\d{4}-\\d{2}-\\d{2}\\.md")
                    || id == null
                    || timestamp == null
                    || output == null) {
                throw new IllegalStateException("Invalid pending memory extraction");
            }
            identities = List.copyOf(identities);
        }
    }

    private record LogRange(long after, long through) {
        LogRange {
            if (after < 0 || through <= after)
                throw new IllegalStateException("Invalid deferred log range");
        }
    }

    private record Deferred(
            List<Part> parts, int attempts, String retryAt, String error, LogRange source) {
        Deferred(List<Part> parts, int attempts, String retryAt, String error) {
            this(parts, attempts, retryAt, error, null);
        }

        Deferred {
            if (parts == null || parts.isEmpty() || attempts < 1 || error == null)
                throw new IllegalStateException("Invalid deferred extraction");
            parts = List.copyOf(parts);
            Instant.parse(retryAt);
        }
    }

    private record ExtractionState(
            int version,
            Set<String> consumed,
            PendingFlush pending,
            long through,
            Map<String, Deferred> deferred) {
        ExtractionState(int version, Set<String> consumed, PendingFlush pending) {
            this(version, consumed, pending, 0, Map.of());
        }

        ExtractionState {
            if (version != 1 || consumed == null || through < 0) {
                throw new IllegalStateException(
                        "Invalid memory extraction state; refusing to reset progress");
            }
            consumed = Set.copyOf(consumed);
            deferred = deferred == null ? Map.of() : Map.copyOf(deferred);
        }
    }

    public MemoryFlushManager(WorkspaceManager workspaceManager, Model model) {
        this(workspaceManager, model, DEFAULT_FLUSH_PROMPT);
    }

    /**
     * @param flushPrompt SYSTEM prompt for the extraction LLM call. {@code null} falls back to
     *     {@link #DEFAULT_FLUSH_PROMPT}.
     */
    public MemoryFlushManager(WorkspaceManager workspaceManager, Model model, String flushPrompt) {
        this(workspaceManager, model, MemoryConfig.builder().flushPrompt(flushPrompt).build());
    }

    /** Creates an extractor using the shared memory configuration. */
    public static MemoryFlushManager configured(
            WorkspaceManager workspaceManager, Model model, MemoryConfig config) {
        return new MemoryFlushManager(workspaceManager, model, config);
    }

    private MemoryFlushManager(
            WorkspaceManager workspaceManager, Model model, MemoryConfig config) {
        this.workspaceManager = workspaceManager;
        this.model = model;
        this.flushPrompt =
                config.flushPrompt() != null ? config.flushPrompt() : DEFAULT_FLUSH_PROMPT;
        this.policy = config.flushContextPolicy();
        this.admission = config.admission();
        this.flushOptions = config.flushOptions();
        this.flushTimeout = config.flushTimeout();
    }

    /** Extracts unconsumed message parts and recovers prepared ledger writes before new work. */
    public Mono<Void> flushMemories(RuntimeContext rc, List<Msg> messages) {
        RuntimeContext context = rc == null ? RuntimeContext.empty() : rc;
        List<Msg> snapshot = List.copyOf(messages);
        // Keep blocking IO off event loops and carry tracing into the auxiliary subscription.
        return Mono.deferContextual(
                        view -> {
                            Deadline deadline = new Deadline(System.nanoTime(), flushTimeout);
                            return Mono.fromRunnable(
                                            () -> {
                                                retryDeferred(
                                                        context,
                                                        journal(context, false),
                                                        view,
                                                        deadline,
                                                        null);
                                                flushBlocking(context, snapshot, view, deadline);
                                            })
                                    .subscribeOn(FLUSH_SCHEDULER)
                                    .timeout(flushTimeout);
                        })
                .then();
    }

    /** Consumes a stable log prefix; confirms delivery or durable isolation before advancing. */
    public Mono<Void> flushSessionMemories(RuntimeContext rc, SessionLog sessionLog) {
        return flushSessionMemories(rc, sessionLog, sessionLog.sourceId());
    }

    /** Supplies a stable full session identity for backends without {@link SessionLog#sourceId()}. */
    public Mono<Void> flushSessionMemories(
            RuntimeContext rc, SessionLog sessionLog, String sourceId) {
        if (sourceId == null || sourceId.isBlank())
            return Mono.error(
                    new IllegalArgumentException("Memory extraction requires a log sourceId"));
        RuntimeContext context = rc == null ? RuntimeContext.empty() : rc;
        return Mono.deferContextual(
                        view -> {
                            // Extraction may outlive the turn; never reuse its writer or mutable
                            // state.
                            RuntimeContext background =
                                    RuntimeContext.builder(
                                                    view.getOrDefault(
                                                            AgentBase.RUNTIME_CONTEXT_KEY, context))
                                            .build();
                            background.put(SessionRecorder.CONTEXT_KEY, null);
                            background.setAgentState(null);
                            Context detached =
                                    Context.of(view).put(AgentBase.RUNTIME_CONTEXT_KEY, background);
                            Deadline deadline = new Deadline(System.nanoTime(), flushTimeout);
                            return Mono.fromRunnable(
                                            () ->
                                                    flushSessionBlocking(
                                                            context,
                                                            sessionLog,
                                                            sourceId,
                                                            detached,
                                                            deadline))
                                    .subscribeOn(FLUSH_SCHEDULER)
                                    .timeout(flushTimeout);
                        })
                .then();
    }

    private void flushSessionBlocking(
            RuntimeContext rc,
            SessionLog sessionLog,
            String sourceId,
            ContextView context,
            Deadline deadline) {
        AtomicTextFile journal = sessionJournal(rc, sourceId);
        // Old progress lacks source identity; recover its work without adopting its cursor.
        AtomicTextFile legacy = journal(rc, false);
        AtomicTextFile unscopedLog = journal(rc, true);
        flushBlocking(rc, List.of(), context, deadline);
        flushBlocking(rc, List.of(), context, deadline, unscopedLog, 0, null, 0, null);
        retryDeferred(rc, journal, context, deadline, sessionLog, legacy, unscopedLog);
        ExtractionState state = readState(journal, Set.of());
        long exported = sessionLog.exportCursor(SESSION_LOG_SINK);
        long cursor = state.through();
        long target = sessionLog.head().seq();
        if (exported < 0 || exported > target || cursor > target)
            throw new SessionLogException("Memory extraction cursor is outside session history");
        if (exported < cursor) advanceCursor(sessionLog, exported, cursor);
        if (cursor == target) return;
        long through = cursor;
        int events = 0;
        List<Msg> messages = new ArrayList<>();
        SourceReader sources = new SourceReader(sessionLog, deadline);
        for (SessionEvent event : sessionLog.scan(cursor, target)) {
            deadline.remaining();
            if (event.seq() != through + 1)
                throw new SessionLogException("Missing memory extraction source events");
            messages.addAll(sources.read(event));
            through = event.seq();
            if (++events == LOG_PAGE_EVENTS) {
                flushBlocking(
                        rc,
                        messages,
                        context,
                        deadline,
                        journal,
                        through,
                        null,
                        cursor,
                        sessionLog);
                if (readState(journal, Set.of()).through() < through) return;
                deadline.remaining();
                advanceCursor(sessionLog, sessionLog.exportCursor(SESSION_LOG_SINK), through);
                cursor = through;
                messages.clear();
                events = 0;
            }
        }
        if (through != target)
            throw new SessionLogException("Incomplete memory extraction source prefix");
        if (through > cursor) {
            flushBlocking(
                    rc, messages, context, deadline, journal, through, null, cursor, sessionLog);
            if (readState(journal, Set.of()).through() < through) return;
            deadline.remaining();
            advanceCursor(sessionLog, sessionLog.exportCursor(SESSION_LOG_SINK), through);
        }
    }

    private void advanceCursor(SessionLog logSource, long expected, long next) {
        if (next <= expected) return;
        try {
            logSource.advanceExportCursor(SESSION_LOG_SINK, expected, next);
        } catch (UnsupportedOperationException unsupported) {
            log.debug("Session backend has no export cursor; using durable workspace progress");
        }
    }

    private static final class SourceReader {
        private final SessionLog logSource;
        private final Deadline deadline;
        private Set<String> earlier;

        SourceReader(SessionLog logSource, Deadline deadline) {
            this.logSource = logSource;
            this.deadline = deadline;
        }

        List<Msg> read(SessionEvent event) {
            List<Msg> messages = sourceMessages(event);
            boolean snapshot =
                    !messages.isEmpty()
                            && (event.type().equals("compaction/start")
                                    || event.type().equals("turn/output"))
                            && !event.data().containsKey("unrecordedMessages")
                            && !event.data().containsKey("sourceMessageRecorded");
            if (snapshot && earlier == null) {
                earlier = new LinkedHashSet<>();
                // One prefix scan per invocation, shared across legacy snapshots and pages.
                for (SessionEvent previous : logSource.scan(0, event.seq() - 1)) {
                    deadline.remaining();
                    for (Msg message : sourceMessages(previous)) earlier.add(message.getId());
                }
            }
            List<Msg> result =
                    snapshot
                            ? messages.stream()
                                    .filter(message -> !earlier.contains(message.getId()))
                                    .toList()
                            : messages;
            if (earlier != null) for (Msg message : messages) earlier.add(message.getId());
            return result;
        }
    }

    private static List<Msg> sourceMessages(SessionEvent event) {
        return switch (event.type()) {
            case "migration/baseline" ->
                    List.copyOf(
                            AgentState.fromJsonString((String) event.data().get("stateJson"))
                                    .getContext());
            case "compaction/start" -> {
                // The acknowledged snapshot also covers messages not yet checkpointed individually.
                if (!(event.data()
                                .getOrDefault(
                                        "unrecordedMessages", event.data().get("inputMessages"))
                        instanceof List<?> messages))
                    throw new SessionLogException("Missing pre-compaction memory source");
                yield messages.stream()
                        .map(message -> JsonUtils.getJsonCodec().convertValue(message, Msg.class))
                        .toList();
            }
            case "message/user", "message/assistant", "tool/result", "turn/output" -> {
                if (Boolean.TRUE.equals(event.data().get("sourceMessageRecorded"))) yield List.of();
                Object message = event.data().get("message");
                if (message == null) throw new SessionLogException("Missing memory source message");
                yield List.of(JsonUtils.getJsonCodec().convertValue(message, Msg.class));
            }
            default -> List.of();
        };
    }

    private record Deadline(long started, Duration timeout) {
        Duration remaining() {
            Duration remaining = timeout.minusNanos(System.nanoTime() - started);
            if (Thread.currentThread().isInterrupted()
                    || remaining.isNegative()
                    || remaining.isZero())
                throw new IllegalStateException(
                        "Memory flush interrupted or timed out; unfinished sources remain"
                                + " retryable");
            return remaining;
        }
    }

    private void flushBlocking(
            RuntimeContext rc, List<Msg> messages, ContextView context, Deadline deadline) {
        flushBlocking(rc, messages, context, deadline, journal(rc, false), 0, null, 0, null);
    }

    private AtomicTextFile sessionJournal(RuntimeContext rc, String sourceId) {
        return workspaceManager.atomicMemoryFile(
                rc, "memory/.flush-state/" + digest(sourceId) + ".source.json");
    }

    private AtomicTextFile journal(RuntimeContext rc, boolean session) {
        String scope =
                digest(
                        JsonUtils.getJsonCodec()
                                .toJson(
                                        List.of(
                                                rc.getUserId() == null ? "" : rc.getUserId(),
                                                rc.getSessionId() == null
                                                        ? ""
                                                        : rc.getSessionId())));
        return workspaceManager.atomicMemoryFile(
                rc, "memory/.flush-state/" + scope + (session ? ".log.json" : ".json"));
    }

    private ExtractionState readState(AtomicTextFile journal, Set<String> initial) {
        return parseState(journal.read(), initial);
    }

    private ExtractionState parseState(String snapshot, Set<String> initial) {
        ExtractionState state =
                snapshot == null
                        ? new ExtractionState(1, initial, null)
                        : JsonUtils.getJsonCodec().fromJson(snapshot, ExtractionState.class);
        if (state == null) throw new IllegalStateException("Null memory extraction state");
        return state;
    }

    private void flushBlocking(
            RuntimeContext rc,
            List<Msg> messages,
            ContextView context,
            Deadline deadline,
            AtomicTextFile journal,
            long through,
            Deferred retry,
            long after,
            SessionLog logSource) {
        String owner =
                rc.getUserId() == null || rc.getUserId().isBlank()
                        ? digest(
                                JsonUtils.getJsonCodec()
                                        .toJson(
                                                List.of(
                                                        rc.getUserId() == null
                                                                ? ""
                                                                : rc.getUserId(),
                                                        rc.getSessionId() == null
                                                                ? ""
                                                                : rc.getSessionId())))
                        : rc.getUserId();
        int conflicts = 0;
        while (true) {
            deadline.remaining();
            String before = journal.read();
            ExtractionState state =
                    parseState(
                            before,
                            before == null && through > 0
                                    ? readState(journal(rc, false), Set.of()).consumed()
                                    : Set.of());
            if (through > 0 && state.through() >= through) return;
            if (through > 0 && state.through() > after)
                throw new SessionLogException(
                        "Extraction prefix advanced concurrently; rescan required");
            ExtractionState updated;
            if (state.pending() != null) {
                deliver(rc, state.pending(), deadline);
                Set<String> consumed = new LinkedHashSet<>(state.consumed());
                consumed.addAll(state.pending().identities());
                Map<String, Deferred> deferred = new HashMap<>(state.deferred());
                deferred.values()
                        .removeIf(
                                failure ->
                                        failure.parts().stream()
                                                .allMatch(
                                                        part ->
                                                                consumed.contains(
                                                                        part.identity())));
                updated = new ExtractionState(1, consumed, null, state.through(), deferred);
            } else {
                if (retry != null
                        && !retry.equals(state.deferred().get(retry.parts().get(0).identity())))
                    return;
                if (state.deferred().values().stream()
                        .anyMatch(
                                failure ->
                                        "in_flight".equals(failure.error())
                                                && Instant.parse(failure.retryAt())
                                                        .isAfter(Instant.now()))) return;
                Set<String> ignored = new LinkedHashSet<>(state.consumed());
                if (retry == null)
                    state.deferred()
                            .values()
                            .forEach(
                                    failure ->
                                            failure.parts()
                                                    .forEach(part -> ignored.add(part.identity())));
                List<Part> parts =
                        retry == null
                                ? messageParts(messages, ignored)
                                : remainingParts(
                                        deferredParts(retry, logSource, deadline), ignored);
                if (parts.stream().allMatch(part -> ignored.contains(part.identity()))) {
                    log.debug(
                            "Memory flush has no new message parts (session={})",
                            rc.getSessionId());
                    if (through == 0 && retry == null) return;
                    Map<String, Deferred> deferred = new HashMap<>(state.deferred());
                    if (retry != null)
                        retry.parts().forEach(part -> deferred.remove(part.identity()));
                    updated =
                            new ExtractionState(
                                    1,
                                    through > 0 ? Set.of() : state.consumed(),
                                    null,
                                    Math.max(through, state.through()),
                                    deferred);
                    if (journal.compareAndSet(before, JsonUtils.getJsonCodec().toJson(updated)))
                        return;
                    if (++conflicts >= 20)
                        throw new IllegalStateException("Repeated extraction completion conflict");
                    continue;
                }
                String today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
                String dailyPath = WorkspaceConstants.MEMORY_DIR + "/" + today + ".md";
                Batch batch = null;
                try {
                    Batch prepared = prepareBatch(rc, parts, ignored, dailyPath);
                    batch = prepared;
                    String attemptKey =
                            retry == null
                                    ? batch.parts().get(0).identity()
                                    : retry.parts().get(0).identity();
                    Map<String, Deferred> attempts = new HashMap<>(state.deferred());
                    if (attempts.size() >= MAX_DEFERRED_BATCHES
                            && !attempts.containsKey(attemptKey))
                        throw new IllegalStateException(
                                "Memory retry queue full; source cursor retained");
                    int attempt = retry == null ? 1 : retry.attempts() + 1;
                    LogRange source =
                            retry != null
                                    ? retry.source()
                                    : logSource == null ? null : new LogRange(after, through);
                    attempts.put(
                            attemptKey,
                            new Deferred(
                                    retry == null
                                            ? savedParts(batch.parts(), source)
                                            : retry.parts(),
                                    attempt,
                                    Instant.now()
                                            .plus(flushTimeout)
                                            .plusSeconds(retryDelay(attempt))
                                            .toString(),
                                    "in_flight",
                                    source));
                    String started =
                            JsonUtils.getJsonCodec()
                                    .toJson(
                                            new ExtractionState(
                                                    1,
                                                    state.consumed(),
                                                    null,
                                                    state.through(),
                                                    attempts));
                    // Persist the source before a cancellable model/judge call, including timeouts.
                    if (!journal.compareAndSet(before, started)) {
                        if (++conflicts >= 20)
                            throw new IllegalStateException("Repeated extraction attempt conflict");
                        continue;
                    }
                    before = started;
                    String extracted =
                            ContextModelCalls.auxiliary(
                                            model, batch.input(), policy, batch.options())
                                    .collect(ExtractionOutput::new, ExtractionOutput::accept)
                                    .map(ExtractionOutput::validated)
                                    .contextWrite(context)
                                    .block(deadline.remaining());
                    if (extracted == null)
                        throw new IllegalStateException(
                                "Memory extractor completed without a result");
                    if (extracted.isEmpty())
                        log.info(
                                "Memory extraction outcome=no_reply (session={})",
                                rc.getSessionId());
                    if (admission != null && !extracted.isEmpty()) {
                        List<String> accepted = new ArrayList<>();
                        List<String> existing = new ArrayList<>(batch.existing());
                        int rejected = 0;
                        int ignoredCandidates = 0;
                        for (String candidate : memoryEntries(extracted)) {
                            if (candidate.isBlank()) continue;
                            if (!BULLET.matcher(candidate).matches()) {
                                ignoredCandidates++;
                                continue;
                            }
                            MemoryAdmission.Decision decision =
                                    Mono.defer(
                                                    () ->
                                                            admission.assess(
                                                                    new MemoryAdmission.Input(
                                                                            owner,
                                                                            candidate,
                                                                            prepared.source(),
                                                                            List.copyOf(existing))))
                                            .switchIfEmpty(
                                                    Mono.error(
                                                            new IllegalStateException(
                                                                    "Empty memory admission"
                                                                            + " decision")))
                                            .contextWrite(context)
                                            .subscribeOn(Schedulers.boundedElastic())
                                            .block(deadline.remaining());
                            if (decision.accepted()) {
                                accepted.add(candidate);
                                existing.add(candidate);
                            } else {
                                rejected++;
                            }
                        }
                        if (accepted.isEmpty() && rejected == 0)
                            throw new IllegalStateException(
                                    "Memory admission requires at least one bullet candidate");
                        log.info(
                                "Memory admission: accepted={}, rejected={}, ignored={}"
                                        + " (session={})",
                                accepted.size(),
                                rejected,
                                ignoredCandidates,
                                rc.getSessionId());
                        if (accepted.isEmpty())
                            log.info(
                                    "Memory extraction outcome=admission_rejected (session={})",
                                    rc.getSessionId());
                        extracted = String.join("\n", accepted);
                    }
                    PendingFlush pending =
                            new PendingFlush(
                                    batch.parts().stream().map(Part::identity).toList(),
                                    dailyPath,
                                    UUID.randomUUID().toString(),
                                    Instant.now().toString(),
                                    extracted);
                    updated =
                            new ExtractionState(
                                    1,
                                    state.consumed(),
                                    pending,
                                    state.through(),
                                    state.deferred());
                } catch (RuntimeException error) {
                    List<Part> failed =
                            batch == null
                                    ? List.of(
                                            parts.stream()
                                                    .filter(
                                                            part ->
                                                                    !ignored.contains(
                                                                            part.identity()))
                                                    .findFirst()
                                                    .orElseThrow())
                                    : batch.parts();
                    Map<String, Deferred> deferred = new HashMap<>(state.deferred());
                    if (retry != null) {
                        failed =
                                remainingParts(parts, state.consumed()).stream()
                                        .filter(part -> !state.consumed().contains(part.identity()))
                                        .toList();
                        deferred.remove(retry.parts().get(0).identity());
                    }
                    String key = failed.get(0).identity();
                    if (deferred.size() >= MAX_DEFERRED_BATCHES && !deferred.containsKey(key))
                        throw new IllegalStateException(
                                "Memory retry queue full; source cursor retained", error);
                    int attempts = retry == null ? 1 : retry.attempts() + 1;
                    long delay = retryDelay(attempts);
                    LogRange source =
                            retry != null
                                    ? retry.source()
                                    : logSource == null ? null : new LogRange(after, through);
                    deferred.put(
                            key,
                            new Deferred(
                                    savedParts(failed, source),
                                    attempts,
                                    Instant.now().plusSeconds(delay).toString(),
                                    error.getClass().getSimpleName(),
                                    source));
                    ExtractionState deferredState =
                            new ExtractionState(
                                    1, state.consumed(), null, state.through(), deferred);
                    if (!journal.compareAndSet(
                            before, JsonUtils.getJsonCodec().toJson(deferredState)))
                        throw new IllegalStateException(
                                "Failed to isolate extraction source; cursor retained", error);
                    log.warn(
                            "Memory extraction deferred (session={}, attempts={}, retrySeconds={},"
                                    + " reason={})",
                            rc.getSessionId(),
                            attempts,
                            delay,
                            error.getClass().getSimpleName());
                    if (retry == null) throw error;
                    return;
                }
            }
            deadline.remaining();
            if (!journal.compareAndSet(before, JsonUtils.getJsonCodec().toJson(updated))) {
                // Discard losing model output and follow the winner's prepared result/receipts.
                if (++conflicts >= 20)
                    throw new IllegalStateException("Repeated memory extraction state conflict");
                continue;
            }
            conflicts = 0;
        }
    }

    private static long retryDelay(int attempts) {
        return Math.min(1800, 60L << Math.min(attempts - 1, 5));
    }

    private static List<Part> savedParts(List<Part> parts, LogRange source) {
        if (source == null) return parts;
        return parts.stream()
                .map(
                        part ->
                                new Part(
                                        part.identity(),
                                        part.baseIdentity(),
                                        part.start(),
                                        part.end(),
                                        null))
                .toList();
    }

    private List<Part> deferredParts(Deferred deferred, SessionLog source, Deadline deadline) {
        if (deferred.source() == null) return deferred.parts();
        if (source == null)
            throw new SessionLogException("Deferred extraction requires its source log");
        Map<String, Part> originals = new HashMap<>();
        SourceReader reader = new SourceReader(source, deadline);
        long through = deferred.source().after();
        for (SessionEvent event : source.scan(through, deferred.source().through())) {
            deadline.remaining();
            if (event.seq() != through + 1)
                throw new SessionLogException("Missing deferred extraction source");
            for (Part part : messageParts(reader.read(event), Set.of()))
                originals.putIfAbsent(part.baseIdentity(), part);
            through = event.seq();
        }
        if (through != deferred.source().through())
            throw new SessionLogException("Incomplete deferred extraction source");
        return deferred.parts().stream()
                .map(
                        saved -> {
                            Part original = originals.get(saved.baseIdentity());
                            if (original == null
                                    || saved.start() < original.start()
                                    || saved.end() > original.end())
                                throw new SessionLogException(
                                        "Deferred message range no longer exists");
                            return new Part(
                                    saved.identity(),
                                    saved.baseIdentity(),
                                    saved.start(),
                                    saved.end(),
                                    original.text()
                                            .substring(
                                                    saved.start() - original.start(),
                                                    saved.end() - original.start()));
                        })
                .toList();
    }

    private void retryDeferred(
            RuntimeContext rc,
            AtomicTextFile journal,
            ContextView context,
            Deadline deadline,
            SessionLog logSource,
            AtomicTextFile... legacyJournals) {
        AtomicTextFile selected = journal;
        Deferred due = nextDeferred(journal);
        for (AtomicTextFile legacy : legacyJournals) {
            Deferred candidate = nextDeferred(legacy);
            if (candidate != null
                    && (due == null
                            || Instant.parse(candidate.retryAt())
                                    .isBefore(Instant.parse(due.retryAt())))) {
                selected = legacy;
                due = candidate;
            }
        }
        if (due != null)
            flushBlocking(
                    rc,
                    List.of(),
                    context,
                    deadline,
                    selected,
                    0,
                    due,
                    0,
                    selected == journal ? logSource : null);
    }

    private Deferred nextDeferred(AtomicTextFile journal) {
        Instant now = Instant.now();
        return readState(journal, Set.of()).deferred().values().stream()
                .filter(failure -> !Instant.parse(failure.retryAt()).isAfter(now))
                .min(java.util.Comparator.comparing(failure -> Instant.parse(failure.retryAt())))
                .orElse(null);
    }

    /** Makes isolated sources eligible after an operator repairs model or admission configuration. */
    public Mono<Void> retryDeferredMemories(RuntimeContext rc, SessionLog sessionLog) {
        return retryDeferredMemories(rc, sessionLog, sessionLog.sourceId());
    }

    public Mono<Void> retryDeferredMemories(
            RuntimeContext rc, SessionLog sessionLog, String sourceId) {
        if (sourceId == null || sourceId.isBlank())
            return Mono.error(
                    new IllegalArgumentException("Memory extraction requires a log sourceId"));
        RuntimeContext context = rc == null ? RuntimeContext.empty() : rc;
        return makeDeferredEligible(journal(context, false))
                .then(makeDeferredEligible(journal(context, true)))
                .then(makeDeferredEligible(sessionJournal(context, sourceId)))
                .then(flushSessionMemories(context, sessionLog, sourceId));
    }

    /** Retries isolated sources in legacy context mode after configuration repair. */
    public Mono<Void> retryDeferredMemories(RuntimeContext rc, List<Msg> messages) {
        RuntimeContext context = rc == null ? RuntimeContext.empty() : rc;
        return makeDeferredEligible(journal(context, false)).then(flushMemories(context, messages));
    }

    private Mono<Void> makeDeferredEligible(AtomicTextFile journal) {
        return Mono.fromRunnable(
                        () -> {
                            String before = journal.read();
                            ExtractionState state = parseState(before, Set.of());
                            if (state.deferred().isEmpty()) return;
                            Map<String, Deferred> failures = new HashMap<>();
                            state.deferred()
                                    .forEach(
                                            (key, failure) ->
                                                    failures.put(
                                                            key,
                                                            new Deferred(
                                                                    failure.parts(),
                                                                    failure.attempts(),
                                                                    Instant.EPOCH.toString(),
                                                                    failure.error(),
                                                                    failure.source())));
                            ExtractionState next =
                                    new ExtractionState(
                                            1,
                                            state.consumed(),
                                            state.pending(),
                                            state.through(),
                                            failures);
                            if (!journal.compareAndSet(
                                    before, JsonUtils.getJsonCodec().toJson(next)))
                                throw new IllegalStateException(
                                        "Memory retry state changed; retry after stopping"
                                                + " executions");
                        })
                .subscribeOn(FLUSH_SCHEDULER)
                .then();
    }

    private List<Part> messageParts(List<Msg> messages, Set<String> consumed) {
        List<Part> parts = new ArrayList<>();
        Map<String, List<int[]>> ranges = receiptRanges(consumed);
        for (Msg message : messages) {
            if (message.getRole() == null
                    || message.getRole() == MsgRole.SYSTEM
                    || isInternalContextMessage(message)) continue;
            String text = renderMessage(message);
            if (text == null || text.isBlank()) continue;
            String identity =
                    digest(
                            message.getId() == null || message.getId().isBlank()
                                    ? text
                                    : message.getId());
            int part = 0;
            for (int offset = 0; offset < text.length(); ) {
                int end = Math.min(text.length(), offset + MAX_BATCH_CHARS - 1);
                if (end < text.length() && Character.isHighSurrogate(text.charAt(end - 1))) end--;
                String base = identity + ":" + part++;
                if (consumed.contains(base)) parts.add(part(base, offset, end, offset, end, text));
                else
                    parts.addAll(
                            receiptedParts(
                                    base,
                                    offset,
                                    end,
                                    text,
                                    ranges.getOrDefault(base, List.of()),
                                    0));
                offset = end;
            }
        }
        return parts;
    }

    private List<Part> remainingParts(List<Part> saved, Set<String> consumed) {
        Map<String, List<int[]>> ranges = receiptRanges(consumed);
        List<Part> parts = new ArrayList<>();
        for (Part original : saved) {
            if (consumed.contains(original.identity())) {
                parts.add(original);
                continue;
            }
            for (Part part :
                    receiptedParts(
                            original.baseIdentity(),
                            original.start(),
                            original.end(),
                            original.text(),
                            ranges.getOrDefault(original.baseIdentity(), List.of()),
                            original.start()))
                parts.add(
                        part.start() == original.start() && part.end() == original.end()
                                ? original
                                : part);
        }
        return parts;
    }

    private Map<String, List<int[]>> receiptRanges(Set<String> consumed) {
        Map<String, List<int[]>> ranges = new HashMap<>();
        for (String identity : consumed) {
            int split = identity.indexOf(":range:");
            if (split < 0) continue;
            String[] bounds = identity.substring(split + 7).split(":");
            if (bounds.length != 2) continue;
            ranges.computeIfAbsent(identity.substring(0, split), key -> new ArrayList<>())
                    .add(new int[] {Integer.parseInt(bounds[0]), Integer.parseInt(bounds[1])});
        }
        ranges.values()
                .forEach(list -> list.sort(java.util.Comparator.comparingInt(range -> range[0])));
        return ranges;
    }

    private static List<Part> receiptedParts(
            String base, int start, int end, String source, List<int[]> ranges, int sourceStart) {
        List<Part> parts = new ArrayList<>();
        int cursor = start;
        for (int[] range : ranges) {
            if (range[0] >= end) break;
            if (range[1] <= start) continue;
            if (range[0] > cursor)
                parts.add(
                        part(
                                base,
                                cursor,
                                Math.min(range[0], end),
                                start,
                                end,
                                source,
                                sourceStart));
            if (range[1] > cursor)
                parts.add(
                        part(
                                base,
                                Math.max(cursor, range[0]),
                                Math.min(range[1], end),
                                start,
                                end,
                                source,
                                sourceStart));
            cursor = Math.max(cursor, range[1]);
            if (cursor >= end) return parts;
        }
        if (cursor < end) parts.add(part(base, cursor, end, start, end, source, sourceStart));
        return parts;
    }

    private static Part part(
            String base, int from, int to, int segmentStart, int segmentEnd, String source) {
        return part(base, from, to, segmentStart, segmentEnd, source, 0);
    }

    private static Part part(
            String base,
            int from,
            int to,
            int segmentStart,
            int segmentEnd,
            String source,
            int sourceStart) {
        String identity =
                from == segmentStart && to == segmentEnd
                        ? base
                        : base + ":range:" + from + ":" + to;
        return new Part(
                identity, base, from, to, source.substring(from - sourceStart, to - sourceStart));
    }

    private Batch prepareBatch(
            RuntimeContext rc, List<Part> parts, Set<String> consumed, String dailyPath) {
        List<Part> available = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>(consumed);
        for (Part part : parts) if (seen.add(part.identity())) available.add(part);
        List<Part> batch = new ArrayList<>();
        GenerateOptions options = ContextModelCalls.outputOptions(model, policy, flushOptions);
        Part firstPart = available.get(0);
        String source = firstPart.text();
        List<Msg> input = extractionInput(source, "", "", "");
        long limit =
                policy.budget(new ModelCallInput(input, List.of(), options, model)).inputTokens();
        int fittingLength =
                cost(input, options) <= limit
                        ? source.length()
                        : fittingLength(source, options, limit);
        if (fittingLength == 0)
            throw new ContextBudgetExceededException(cost(input, options), limit);
        Part first = prefix(firstPart, fittingLength);
        batch.add(first);
        source = first.text();

        String previous = "";
        int firstIndex = parts.indexOf(firstPart);
        if (firstIndex > 0
                && consumed.contains(parts.get(firstIndex - 1).identity())
                && parts.get(firstIndex - 1).text().length() <= MAX_EXISTING_CHARS) {
            String candidate = parts.get(firstIndex - 1).text();
            if (cost(extractionInput(source, candidate, "", ""), options) <= limit)
                previous = candidate;
        }
        String memory = readExistingContent(rc, WorkspaceConstants.MEMORY_MD);
        String daily = readExistingContent(rc, dailyPath);
        String selectedMemory = "";
        List<String> memoryEntries = memoryEntries(memory);
        Map<Integer, String> selectedEntries = new TreeMap<>();
        for (int i = 0; i < memoryEntries.size(); i++) {
            int index = i % 2 == 0 ? i / 2 : memoryEntries.size() - 1 - i / 2;
            String entry = memoryEntries.get(index);
            if (entry.isBlank() || entry.startsWith("<!--")) continue;
            Map<Integer, String> next = new TreeMap<>(selectedEntries);
            next.put(index, entry);
            String candidate = String.join("\n", next.values());
            if (candidate.length() > MAX_EXISTING_CHARS
                    || cost(extractionInput(source, previous, candidate, ""), options) > limit)
                continue;
            selectedEntries = next;
            selectedMemory = candidate;
        }
        String selectedDaily = "";
        List<String> entries = memoryEntries(daily);
        for (int i = entries.size() - 1; i >= 0; i--) {
            String entry = entries.get(i);
            if (entry.isBlank() || entry.startsWith("<!--")) continue;
            String candidate = selectedDaily.isEmpty() ? entry : entry + "\n" + selectedDaily;
            if (candidate.length() > MAX_EXISTING_CHARS
                    || cost(extractionInput(source, previous, selectedMemory, candidate), options)
                            > limit) break;
            selectedDaily = candidate;
        }
        for (int i = 1; fittingLength == firstPart.text().length() && i < available.size(); i++) {
            Part next = available.get(i);
            String candidate = source + "\n" + next.text();
            if (candidate.length() > MAX_BATCH_CHARS
                    || cost(
                                    extractionInput(
                                            candidate, previous, selectedMemory, selectedDaily),
                                    options)
                            > limit) break;
            batch.add(next);
            source = candidate;
        }
        input = extractionInput(source, previous, selectedMemory, selectedDaily);
        List<String> existing =
                memoryEntries(selectedMemory + "\n" + selectedDaily).stream()
                        .filter(entry -> !entry.isBlank())
                        .toList();
        log.debug(
                "Memory extraction batch: parts={}, estimatedTokens={}, limit={}, windowKnown={}",
                batch.size(),
                cost(input, options),
                limit,
                model.getContextWindowSize() > 0);
        return new Batch(List.copyOf(batch), input, options, source, existing);
    }

    private int fittingLength(String source, GenerateOptions options, long limit) {
        int low = 0;
        int high = source.codePointCount(0, source.length());
        while (low < high) {
            int middle = low + (high - low + 1) / 2;
            int end = source.offsetByCodePoints(0, middle);
            long estimate = cost(extractionInput(source.substring(0, end), "", "", ""), options);
            if (estimate <= limit) low = middle;
            else high = middle - 1;
        }
        return source.offsetByCodePoints(0, low);
    }

    private static Part prefix(Part part, int length) {
        int end = part.start() + length;
        String identity =
                end == part.end()
                        ? part.identity()
                        : part.baseIdentity() + ":range:" + part.start() + ":" + end;
        return new Part(
                identity, part.baseIdentity(), part.start(), end, part.text().substring(0, length));
    }

    private long cost(List<Msg> input, GenerateOptions options) {
        return policy.estimator()
                .estimate(new ModelCallInput(input, List.of(), options, model))
                .tokens();
    }

    private List<Msg> extractionInput(
            String conversationText, String previous, String existingMemory, String existingDaily) {
        StringBuilder userPrompt = new StringBuilder();
        if (!previous.isBlank())
            userPrompt
                    .append(
                            "Read-only preceding conversation (context only; do not extract from"
                                    + " it):\n")
                    .append(previous)
                    .append("\n\n");
        if (!existingMemory.isBlank()) {
            userPrompt
                    .append("MEMORY.md (read-only curated long-term memory — do NOT restate):\n")
                    .append(existingMemory)
                    .append("\n\n");
        }
        if (!existingDaily.isBlank()) {
            userPrompt
                    .append("Today's daily ledger so far (your output will be appended after):\n")
                    .append(existingDaily)
                    .append("\n\n");
        }
        userPrompt
                .append(
                        "Extract NEW memories from this conversation window (skip anything"
                                + " already covered above):\n\n")
                .append(conversationText);

        List<Msg> flushInput = new ArrayList<>();
        flushInput.add(
                Msg.builder()
                        .role(MsgRole.SYSTEM)
                        .name("system")
                        .content(TextBlock.builder().text(flushPrompt).build())
                        .build());
        flushInput.add(
                Msg.builder()
                        .role(MsgRole.USER)
                        .content(TextBlock.builder().text(userPrompt.toString()).build())
                        .build());

        return List.copyOf(flushInput);
    }

    private void deliver(RuntimeContext rc, PendingFlush pending, Deadline deadline) {
        if (pending.output().isEmpty()) return; // NO_REPLY or all candidates rejected.
        AtomicTextFile daily = workspaceManager.atomicMemoryFile(rc, pending.dailyPath());
        String marker = "<!-- agentscope-memory-flush:" + pending.id() + " -->";
        String memory = readExistingContent(rc, WorkspaceConstants.MEMORY_MD);
        for (int attempt = 0; attempt < 20; attempt++) {
            String before = daily.read();
            if (before != null && before.contains(marker)) return;
            Set<String> known = new LinkedHashSet<>();
            memoryEntries(memory + "\n" + (before == null ? "" : before)).stream()
                    .map(MemoryFlushManager::normalizedBullet)
                    .filter(s -> !s.isBlank())
                    .forEach(known::add);
            String content =
                    memoryEntries(pending.output()).stream()
                            .filter(
                                    line ->
                                            !BULLET.matcher(line).matches()
                                                    || known.add(normalizedBullet(line)))
                            .collect(Collectors.joining("\n"))
                            .strip();
            if (content.isEmpty()) {
                log.info(
                        "Memory extraction outcome=duplicate (session={}, batch={})",
                        rc.getSessionId(),
                        pending.id());
                return;
            }
            String entry =
                    "\n"
                            + marker
                            + "\n## Memory Flush — "
                            + pending.timestamp()
                            + "\n"
                            + content
                            + "\n";
            deadline.remaining();
            if (daily.compareAndSet(before, (before == null ? "" : before) + entry)) return;
        }
        throw new IllegalStateException("Repeated memory ledger append conflict");
    }

    private static String normalizedBullet(String line) {
        return BULLET.matcher(line).matches()
                ? line.strip().substring(1).strip().replaceAll("\\s+", " ")
                : "";
    }

    private static List<String> memoryEntries(String text) {
        List<String> entries = new ArrayList<>();
        StringBuilder entry = new StringBuilder();
        for (String line : text.split("\\R", -1)) {
            if (ENTRY_START.matcher(line).matches() && !entry.isEmpty()) {
                entries.add(entry.toString().strip());
                entry.setLength(0);
            }
            entry.append(line).append('\n');
        }
        if (!entry.isEmpty()) entries.add(entry.toString().strip());
        return entries;
    }

    private static String digest(String text) {
        try {
            return HexFormat.of()
                    .formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class ExtractionOutput {
        private final StringBuilder text = new StringBuilder();
        private boolean invalid;

        void accept(ChatResponse response) {
            String reason = response.getFinishReason();
            if (FinishReasons.indicatesOutputLimit(
                            reason,
                            response.getMetadata() == null ? Map.of() : response.getMetadata())
                    || "incomplete".equalsIgnoreCase(reason)
                    || "content_filter".equalsIgnoreCase(reason)) invalid = true;
            if (response.getContent() != null)
                for (ContentBlock block : response.getContent()) {
                    if (block instanceof ToolUseBlock) invalid = true;
                    if (block instanceof TextBlock tb && tb.getText() != null) {
                        if (text.length() + tb.getText().length() > MAX_OUTPUT_CHARS)
                            invalid = true;
                        else text.append(tb.getText());
                    }
                }
        }

        String validated() {
            String output = text.toString().strip();
            if (invalid || output.isBlank())
                throw new IllegalStateException(
                        "Incomplete or invalid memory extraction output; progress unchanged");
            return output.equals("NO_REPLY") ? "" : output;
        }
    }

    private String readExistingContent(RuntimeContext rc, String relativePath) {
        String content = workspaceManager.atomicMemoryFile(rc, relativePath).read();
        return content == null ? "" : content;
    }

    private static final String SESSION_CONTEXT_TAG = "<session_context>";

    private static boolean isInternalContextMessage(Msg msg) {
        return isSessionContextMessage(msg) || isCompactionSummaryMessage(msg);
    }

    private static boolean isCompactionSummaryMessage(Msg msg) {
        return msg != null && ConversationCompactor.SUMMARY_MSG_NAME.equals(msg.getName());
    }

    private static boolean isSessionContextMessage(Msg msg) {
        if (msg.getRole() != MsgRole.USER) {
            return false;
        }
        String text = msg.getTextContent();
        return text != null && text.contains(SESSION_CONTEXT_TAG);
    }

    private String renderMessage(Msg msg) {
        String body = renderContentBlocks(msg);
        if (body == null) {
            return null;
        }
        return "[" + msg.getRole().name() + "]: " + body;
    }

    /**
     * Renders all content blocks of a message into a single text string.
     * Returns null if no renderable content is found.
     */
    private String renderContentBlocks(Msg msg) {
        List<ContentBlock> blocks = msg.getContent();
        if (blocks == null || blocks.isEmpty()) {
            return null;
        }

        List<String> parts = new ArrayList<>();
        for (ContentBlock block : blocks) {
            if (block instanceof TextBlock tb) {
                String text = tb.getText();
                if (text != null && !text.isBlank()) {
                    parts.add(text);
                }
            } else if (block instanceof ToolUseBlock tu) {
                parts.add(renderToolUse(tu));
            } else if (block instanceof ToolResultBlock tr) {
                parts.add(renderToolResult(tr));
            } else if (block instanceof DocumentBlock doc) {
                parts.add(DocumentReferenceRenderer.render(doc));
            }
        }

        if (parts.isEmpty()) {
            return null;
        }
        return String.join("\n", parts);
    }

    private static String renderToolUse(ToolUseBlock tu) {
        StringBuilder sb = new StringBuilder();
        sb.append("[tool_call: ").append(tu.getName());
        if (tu.getInput() != null && !tu.getInput().isEmpty()) {
            try {
                String inputJson = JsonUtils.getJsonCodec().toJson(tu.getInput());
                if (inputJson.length() > 500) {
                    inputJson = inputJson.substring(0, 500) + "...";
                }
                sb.append("(").append(inputJson).append(")");
            } catch (Exception e) {
                sb.append("(...)");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private static String renderToolResult(ToolResultBlock tr) {
        StringBuilder sb = new StringBuilder();
        sb.append("[tool_result");
        if (tr.getName() != null) {
            sb.append(": ").append(tr.getName());
        }
        sb.append("] ");

        List<ContentBlock> outputs = tr.getOutput();
        if (outputs != null) {
            for (ContentBlock out : outputs) {
                if (out instanceof TextBlock tb) {
                    String text = tb.getText();
                    if (text != null) {
                        if (text.length() > 1000) {
                            sb.append(text, 0, 1000).append("...(truncated)");
                        } else {
                            sb.append(text);
                        }
                    }
                } else if (out instanceof DocumentBlock doc) {
                    if (sb.length() > 0 && !Character.isWhitespace(sb.charAt(sb.length() - 1))) {
                        sb.append(' ');
                    }
                    sb.append(DocumentReferenceRenderer.render(doc));
                }
            }
        }
        return sb.toString();
    }
}
