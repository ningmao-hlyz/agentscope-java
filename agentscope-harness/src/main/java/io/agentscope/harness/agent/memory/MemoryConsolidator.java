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

import io.agentscope.core.agent.RuntimeContext;
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
import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.context.ContextBudgetExceededException;
import io.agentscope.harness.agent.context.ContextModelCalls;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.filesystem.model.FileInfo;
import io.agentscope.harness.agent.filesystem.remote.store.BaseStore;
import io.agentscope.harness.agent.filesystem.remote.store.StoreItem;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Merges one budgeted ledger prefix per run, persisting file offsets for restart recovery.
 * Storage follows {@link WorkspaceManager}'s memory routing; the legacy watermark is retained.
 *
 * <p>The budget-only auxiliary call commits complete, non-empty, text-only output within the
 * body target. CAS against the input snapshot protects concurrent saves; conflicts and storage
 * errors leave progress unchanged for retry. Memory and progress are separate writes, so a
 * crash after the memory write can replay a batch.
 *
 * <p>{@code maxMemoryTokens} sets the prompt target and a four-characters-per-token body limit.
 * The provider's output limit also covers reasoning tokens and remains independently configured;
 * length-truncated output is rejected.
 */
public class MemoryConsolidator {

    private static final Logger log = LoggerFactory.getLogger(MemoryConsolidator.class);

    /** Hidden state file inside {@code memory/} tracking the last consolidation Instant. */
    public static final String STATE_FILE = ".consolidation_state";

    /**
     * Default prompt for the consolidation step. Exposed publicly so callers can extend
     * (e.g. append project-specific guidelines) when constructing
     * {@link io.agentscope.harness.agent.memory.MemoryConfig}. The prompt contains exactly
     * two {@code %d} placeholders (max-tokens, max-chars) that are filled in at call time.
     */
    public static final String DEFAULT_CONSOLIDATION_PROMPT =
            """
            You are a memory consolidation assistant. You own the curated long-term memory \
            file MEMORY.md. Your job is to merge new daily ledger entries into MEMORY.md while \
            keeping it concise, deduplicated, and high-signal.

            You are given two inputs:
            1. The current MEMORY.md content (the existing curated long-term memory).
            2. New daily ledger entries that have been appended since the last consolidation.

            Rules:
            - MEMORY.md is the single source of truth for cross-day, cross-session knowledge. \
            Keep it stable and authoritative.
            - Daily ledger entries are stream-of-consciousness flush logs — they may be noisy, \
            redundant with MEMORY.md, or redundant with each other. Promote only what is \
            durable and reusable.
            - Deduplicate: if a new entry restates something MEMORY.md already covers, skip it.
            - Merge related facts: combine entries about the same topic into cohesive paragraphs \
            with clear section headers.
            - Update or remove stale information when new entries supersede it.
            - Keep total output within %d tokens (approximately %d characters); prioritize \
            recent and frequently-referenced information when trimming.

            Output the COMPLETE new MEMORY.md content (not just a diff). Use markdown.\
            """;

    private static final List<String> WATERMARK_NAMESPACE = List.of("memory", "consolidation");
    private static final String WATERMARK_KEY = "watermark";
    private static final int MAX_CAS_RETRIES = 5;
    static final String PROGRESS_REL_PATH = "memory/.consolidation-progress.json";
    private static final int MAX_BATCH_CHARS = 24_000;

    private record FileProgress(int offset, String prefixHash) {
        FileProgress {
            if (offset < 0 || prefixHash == null)
                throw new IllegalStateException("Invalid consolidation offset");
        }
    }

    private record Progress(int version, String legacyWatermark, Map<String, FileProgress> files) {
        Progress {
            if (version != 1 || files == null)
                throw new IllegalStateException("Invalid consolidation progress");
            Instant.parse(legacyWatermark);
            files = Map.copyOf(files);
        }
    }

    private record Batch(List<Msg> input, Progress progress) {}

    private final WorkspaceManager workspaceManager;
    private final Model model;
    private final String consolidationPrompt;
    private final int maxMemoryTokens;
    private final BaseStore baseStore;
    private final ContextPolicy policy;
    private final GenerateOptions options;

    public MemoryConsolidator(WorkspaceManager workspaceManager, Model model) {
        this(
                workspaceManager,
                model,
                DEFAULT_CONSOLIDATION_PROMPT,
                MemoryConfig.DEFAULT_CONSOLIDATION_MAX_TOKENS,
                null);
    }

    public MemoryConsolidator(WorkspaceManager workspaceManager, Model model, int maxMemoryTokens) {
        this(workspaceManager, model, DEFAULT_CONSOLIDATION_PROMPT, maxMemoryTokens, null);
    }

    /**
     * @param consolidationPrompt prompt template for the consolidation LLM call. Must contain
     *     exactly two {@code %d} placeholders (max-tokens, max-chars). {@code null} falls back
     *     to {@link #DEFAULT_CONSOLIDATION_PROMPT}.
     */
    public MemoryConsolidator(
            WorkspaceManager workspaceManager,
            Model model,
            String consolidationPrompt,
            int maxMemoryTokens) {
        this(workspaceManager, model, consolidationPrompt, maxMemoryTokens, null);
    }

    /**
     * @param baseStore optional legacy watermark mirror; source progress stays in routed files
     */
    public MemoryConsolidator(
            WorkspaceManager workspaceManager,
            Model model,
            String consolidationPrompt,
            int maxMemoryTokens,
            BaseStore baseStore) {
        this(
                workspaceManager,
                model,
                consolidationPrompt,
                maxMemoryTokens,
                baseStore,
                ContextPolicy.defaults());
    }

    public MemoryConsolidator(
            WorkspaceManager workspaceManager,
            Model model,
            String consolidationPrompt,
            int maxMemoryTokens,
            BaseStore baseStore,
            ContextPolicy policy) {
        this.workspaceManager = workspaceManager;
        this.model = model;
        this.consolidationPrompt =
                consolidationPrompt != null ? consolidationPrompt : DEFAULT_CONSOLIDATION_PROMPT;
        this.maxMemoryTokens = maxMemoryTokens;
        this.baseStore = baseStore;
        this.policy = Objects.requireNonNull(policy);
        this.options = model == null ? null : ContextModelCalls.outputOptions(model, policy, null);
    }

    /**
     * Merges one batch, advancing file offsets only after the memory CAS succeeds.
     */
    public Mono<Void> consolidate(RuntimeContext rc) {
        return Mono.defer(() -> consolidateBatch(rc))
                .then()
                .subscribeOn(Schedulers.boundedElastic());
    }

    /** Processes at most four successful batches per maintenance run. */
    public Mono<Void> catchUp(RuntimeContext rc) {
        return catchUp(rc, 4).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Void> catchUp(RuntimeContext rc, int remaining) {
        return Mono.defer(
                () -> {
                    AtomicTextFile progress =
                            workspaceManager.atomicMemoryFile(rc, PROGRESS_REL_PATH);
                    String before = progress.read();
                    return consolidate(rc)
                            .then(
                                    Mono.defer(
                                            () ->
                                                    remaining > 1
                                                                    && !Objects.equals(
                                                                            before, progress.read())
                                                            ? catchUp(rc, remaining - 1)
                                                            : Mono.empty()));
                });
    }

    /** Checks persisted coverage before maintenance archives a ledger snapshot. */
    public boolean isConsolidated(RuntimeContext rc, FileInfo file) {
        return consolidatedSnapshot(rc, file) != null;
    }

    /** Returns a fully consumed snapshot for conditional archival, or null without proof. */
    public String consolidatedSnapshot(RuntimeContext rc, FileInfo file) {
        try {
            String snapshot = workspaceManager.atomicMemoryFile(rc, PROGRESS_REL_PATH).read();
            if (snapshot == null) return null;
            Progress progress = JsonUtils.getJsonCodec().fromJson(snapshot, Progress.class);
            FileProgress position = progress.files().get(toRelative(file.path()));
            if (position == null) return null;
            String content = workspaceManager.atomicMemoryFile(rc, toRelative(file.path())).read();
            return content != null
                            && position.offset() == content.length()
                            && hashPrefix(content, content.length()).equals(position.prefixHash())
                    ? content
                    : null;
        } catch (RuntimeException e) {
            log.warn(
                    "Cannot confirm consolidation coverage for {}: {}",
                    file.path(),
                    e.getMessage());
            return null;
        }
    }

    private Mono<Boolean> consolidateBatch(RuntimeContext rc) {
        Instant runStart = Instant.now();

        AtomicTextFile memoryFile;
        AtomicTextFile progressFile;
        String progressSnapshot;
        Progress progress;
        String existingMemory;
        try {
            // Read progress before memory so a newer cursor never pairs with an older snapshot.
            progressFile = workspaceManager.atomicMemoryFile(rc, PROGRESS_REL_PATH);
            String snapshot = progressFile.read();
            Progress previous =
                    snapshot == null
                            ? new Progress(1, readWatermarkFromFile(rc).toString(), Map.of())
                            : JsonUtils.getJsonCodec().fromJson(snapshot, Progress.class);
            if (previous == null) throw new IllegalStateException("Null consolidation progress");
            progress = pruneArchivedProgress(rc, previous);
            progressSnapshot =
                    progress.equals(previous)
                            ? snapshot
                            : JsonUtils.getJsonCodec().toJson(progress);
            if (!progress.equals(previous)
                    && !progressFile.compareAndSet(snapshot, progressSnapshot)) {
                log.debug("Consolidation progress changed during archive cleanup; retrying later");
                return Mono.empty();
            }
            if (!progress.equals(previous)) {
                log.debug(
                        "Reclaimed {} archived ledger offsets",
                        previous.files().size() - progress.files().size());
            }
            memoryFile = workspaceManager.atomicMemoryFile(rc, "MEMORY.md");
            existingMemory = memoryFile.read();
        } catch (RuntimeException e) {
            log.warn("Failed to prepare consolidation state: {}", e.getMessage());
            return Mono.empty();
        }
        long maxChars = (long) maxMemoryTokens * 4;
        String systemPrompt = String.format(consolidationPrompt, maxMemoryTokens, maxChars);
        Batch batch;
        try {
            batch =
                    prepareBatch(
                            rc,
                            progress,
                            existingMemory == null ? "" : existingMemory,
                            systemPrompt);
        } catch (ContextBudgetExceededException e) {
            return Mono.error(e);
        } catch (RuntimeException e) {
            log.warn("Failed to read consolidation ledgers: {}", e.getMessage());
            return Mono.empty();
        }
        if (batch == null) return Mono.empty();

        return ContextModelCalls.auxiliary(model, batch.input(), policy, options)
                .reduce(
                        new ConsolidationOutput(),
                        (acc, chatResponse) -> {
                            acc.accept(chatResponse);
                            return acc;
                        })
                .publishOn(Schedulers.boundedElastic())
                .flatMap(
                        acc -> {
                            String consolidated = acc.text().strip();
                            if (acc.incomplete()) {
                                log.warn(
                                        "Consolidation output incomplete (finish reason {}) —"
                                                + " keeping existing MEMORY.md and progress",
                                        acc.finishReason());
                                return Mono.empty();
                            }
                            if (acc.sawToolBlock()) {
                                log.warn(
                                        "Consolidation output contained tool-call blocks —"
                                                + " keeping existing MEMORY.md and progress");
                                return Mono.empty();
                            }
                            if (consolidated.isBlank()) {
                                log.warn("Consolidation produced empty output, skipping");
                                return Mono.empty();
                            }
                            if (consolidated.length() > maxChars) {
                                log.warn(
                                        "Consolidation output of {} chars exceeds the {}-char body"
                                                + " target — keeping existing MEMORY.md and"
                                                + " progress",
                                        consolidated.length(),
                                        maxChars);
                                return Mono.empty();
                            }
                            boolean committed;
                            try {
                                committed = memoryFile.compareAndSet(existingMemory, consolidated);
                            } catch (RuntimeException e) {
                                log.warn(
                                        "Failed to write consolidated MEMORY.md: {}",
                                        e.getMessage());
                                return Mono.empty();
                            }
                            if (!committed) {
                                log.warn(
                                        "MEMORY.md changed during consolidation; keeping"
                                                + " progress for retry");
                                return Mono.empty();
                            }
                            try {
                                if (!progressFile.compareAndSet(
                                        progressSnapshot,
                                        JsonUtils.getJsonCodec().toJson(batch.progress()))) {
                                    log.warn(
                                            "Consolidation progress changed; leaving it for retry");
                                    return Mono.empty();
                                }
                            } catch (RuntimeException e) {
                                log.warn(
                                        "Failed to persist consolidation progress; batch may"
                                                + " replay: {}",
                                        e.getMessage());
                                return Mono.empty();
                            }
                            writeWatermark(rc, runStart);
                            log.info(
                                    "MEMORY.md consolidated ({} chars), watermark advanced to {}",
                                    consolidated.length(),
                                    runStart);
                            return Mono.just(true);
                        });
    }

    /** Accumulates one consolidation stream: body text plus the terminal finish signal. */
    private static final class ConsolidationOutput {

        private final StringBuilder text = new StringBuilder();
        private String finishReason;
        private final Map<String, Object> metadata = new HashMap<>();
        private boolean sawToolBlock;
        private boolean incomplete;

        void accept(ChatResponse chunk) {
            if (chunk.getFinishReason() != null) {
                finishReason = chunk.getFinishReason();
            }
            if (chunk.getMetadata() != null && !chunk.getMetadata().isEmpty()) {
                metadata.putAll(chunk.getMetadata());
            }
            if (FinishReasons.indicatesOutputLimit(finishReason, metadata)
                    || "incomplete".equalsIgnoreCase(finishReason)
                    || "content_filter".equalsIgnoreCase(finishReason)) incomplete = true;
            if (chunk.getContent() != null) {
                for (var block : chunk.getContent()) {
                    if (block instanceof TextBlock tb && tb.getText() != null) {
                        text.append(tb.getText());
                    } else if (block instanceof ToolUseBlock || block instanceof ToolResultBlock) {
                        sawToolBlock = true;
                    }
                }
            }
        }

        String text() {
            return text.toString();
        }

        String finishReason() {
            return finishReason;
        }

        boolean incomplete() {
            return incomplete;
        }

        boolean sawToolBlock() {
            return sawToolBlock;
        }
    }

    /** Reclaims completed archived offsets without changing the legacy cutoff. */
    private Progress pruneArchivedProgress(RuntimeContext rc, Progress progress) {
        Map<String, FileProgress> files = new HashMap<>(progress.files());
        for (var entry : progress.files().entrySet()) {
            String path = entry.getKey();
            if (!path.matches("memory/\\d{4}-\\d{2}-\\d{2}\\.md")) continue;
            String archive =
                    workspaceManager
                            .atomicMemoryFile(rc, "memory/archive/" + fileName(path))
                            .read();
            FileProgress position = entry.getValue();
            if (archive != null
                    && position.offset() == archive.length()
                    && hashPrefix(archive, archive.length()).equals(position.prefixHash())
                    && workspaceManager.atomicMemoryFile(rc, path).read() == null) {
                files.remove(path);
            }
        }
        return new Progress(1, progress.legacyWatermark(), files);
    }

    /** Selects unconsumed file ranges within the complete request budget. */
    private Batch prepareBatch(RuntimeContext rc, Progress progress, String memory, String prompt) {
        Instant legacyWatermark = Instant.parse(progress.legacyWatermark());
        List<FileInfo> eligible =
                workspaceManager.listMemoryFileInfos(rc).stream()
                        .filter(
                                fi ->
                                        !fi.isDirectory()
                                                && toRelative(fi.path())
                                                        .matches("memory/[^/]+\\.md"))
                        .filter(
                                fi ->
                                        progress.files().containsKey(toRelative(fi.path()))
                                                || isModifiedAfter(fi, legacyWatermark))
                        .sorted(Comparator.comparing(fi -> fileName(fi.path())))
                        .toList();
        Map<String, FileProgress> files = new HashMap<>(progress.files());
        StringBuilder selected = new StringBuilder();
        for (FileInfo fi : eligible) {
            String rel = toRelative(fi.path());
            String content = workspaceManager.atomicMemoryFile(rc, rel).read();
            if (content == null || content.isBlank()) continue;
            FileProgress previous = files.get(rel);
            int start = 0;
            if (previous != null) {
                if (previous.offset() <= content.length()
                        && hashPrefix(content, previous.offset()).equals(previous.prefixHash())) {
                    start = previous.offset();
                } else {
                    log.warn(
                            "Consolidation ledger {} changed before its offset; replaying it", rel);
                }
            }
            if (start == content.length()) continue;
            String prefix = selected + "### " + fileName(fi.path()) + "\n";
            int end = Math.min(content.length(), start + MAX_BATCH_CHARS - prefix.length() - 2);
            if (end <= start) break;
            if (end < content.length() && Character.isHighSurrogate(content.charAt(end - 1))) end--;
            end = fittingEnd(prompt, memory, prefix, content, start, end);
            if (end == start) {
                if (!selected.isEmpty()) break;
                List<Msg> input =
                        consolidationInput(
                                prompt,
                                memory,
                                prefix
                                        + content.substring(
                                                start, content.offsetByCodePoints(start, 1))
                                        + "\n\n");
                throw new ContextBudgetExceededException(cost(input), limit(input));
            }
            // Keep lines intact when possible; an oversized single line resumes by code point.
            if (end < content.length()) {
                int lineEnd = content.lastIndexOf('\n', end - 1) + 1;
                if (lineEnd > start) end = lineEnd;
            }
            selected.append("### ")
                    .append(fileName(fi.path()))
                    .append('\n')
                    .append(content, start, end)
                    .append("\n\n");
            files.put(rel, new FileProgress(end, hashPrefix(content, end)));
            if (end < content.length()) break;
        }
        if (selected.isEmpty()) return null;
        List<Msg> input = consolidationInput(prompt, memory, selected.toString());
        log.debug(
                "Consolidation batch: ledgerChars={}, estimatedTokens={}, limit={}",
                selected.length(),
                cost(input),
                limit(input));
        return new Batch(input, new Progress(1, progress.legacyWatermark(), files));
    }

    private int fittingEnd(
            String prompt, String memory, String prefix, String source, int start, int end) {
        List<Msg> full =
                consolidationInput(prompt, memory, prefix + source.substring(start, end) + "\n\n");
        if (cost(full) <= limit(full)) return end;
        int low = 0;
        int high = source.codePointCount(start, end);
        while (low < high) {
            int middle = low + (high - low + 1) / 2;
            int next = source.offsetByCodePoints(start, middle);
            List<Msg> input =
                    consolidationInput(
                            prompt, memory, prefix + source.substring(start, next) + "\n\n");
            if (cost(input) <= limit(input)) low = middle;
            else high = middle - 1;
        }
        return source.offsetByCodePoints(start, low);
    }

    private List<Msg> consolidationInput(String prompt, String memory, String entries) {
        return List.of(
                Msg.builder().role(MsgRole.SYSTEM).name("system").textContent(prompt).build(),
                Msg.builder()
                        .role(MsgRole.USER)
                        .textContent(
                                "Current MEMORY.md:\n"
                                        + (memory.isBlank() ? "(empty)" : memory)
                                        + "\n\nNew daily ledger entries to merge:\n"
                                        + entries)
                        .build());
    }

    private long cost(List<Msg> input) {
        return policy.estimator()
                .estimate(new ModelCallInput(input, List.of(), options, model))
                .tokens();
    }

    private long limit(List<Msg> input) {
        return policy.budget(new ModelCallInput(input, List.of(), options, model)).inputTokens();
    }

    private static String hashPrefix(String content, int end) {
        try {
            return HexFormat.of()
                    .formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(
                                            content.substring(0, end)
                                                    .getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean isModifiedAfter(FileInfo fi, Instant watermark) {
        String modifiedAt = fi.modifiedAt();
        if (modifiedAt == null || modifiedAt.isBlank()) {
            return true; // be safe — include on unknown mtime
        }
        try {
            return Instant.parse(modifiedAt).isAfter(watermark);
        } catch (Exception e) {
            return true; // be safe on parse error
        }
    }

    /** Extracts the file name (last path segment) from a path string. */
    private static String fileName(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        String stripped = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        int idx = stripped.lastIndexOf('/');
        return idx >= 0 ? stripped.substring(idx + 1) : stripped;
    }

    /**
     * Converts an absolute filesystem path (e.g. {@code /memory/2025-01-01.md}) to a
     * workspace-relative path ({@code memory/2025-01-01.md}) for use with
     * {@link WorkspaceManager#readMemoryFileUtf8}.
     */
    private static String toRelative(String path) {
        if (path == null) {
            return "";
        }
        return path.startsWith("/") ? path.substring(1) : path;
    }

    static final String STATE_REL_PATH = "memory/" + STATE_FILE;

    /** Reads the last consolidation Instant, or {@link Instant#EPOCH} if none recorded. */
    Instant readWatermark(RuntimeContext rc) {
        BaseStore store = this.baseStore;
        if (store != null) {
            try {
                StoreItem item = store.get(WATERMARK_NAMESPACE, WATERMARK_KEY);
                if (item != null && item.value() != null) {
                    Object ts = item.value().get("ts");
                    if (ts instanceof Number n) {
                        return Instant.ofEpochMilli(n.longValue());
                    }
                }
            } catch (RuntimeException e) {
                log.warn(
                        "Failed to read consolidation watermark from store: {} — falling back to"
                                + " file",
                        e.getMessage());
            }
        }
        return readWatermarkFromFile(rc);
    }

    private Instant readWatermarkFromFile(RuntimeContext rc) {
        try {
            String value = workspaceManager.atomicMemoryFile(rc, STATE_REL_PATH).read();
            if (value == null || value.isBlank()) {
                return Instant.EPOCH;
            }
            return Instant.parse(value.strip());
        } catch (Exception e) {
            log.warn(
                    "Failed to read consolidation watermark at {}: {} — treating as EPOCH",
                    STATE_REL_PATH,
                    e.getMessage());
            return Instant.EPOCH;
        }
    }

    private void writeWatermark(RuntimeContext rc, Instant ts) {
        if (baseStore != null) {
            writeWatermarkToStore(ts);
        }
        try {
            AtomicTextFile watermark = workspaceManager.atomicMemoryFile(rc, STATE_REL_PATH);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                String before = watermark.read();
                if (before != null && !Instant.parse(before.strip()).isBefore(ts)) return;
                if (watermark.compareAndSet(before, ts.toString())) return;
            }
            log.warn("Repeated consolidation watermark conflict; progress remains authoritative");
        } catch (Exception e) {
            log.warn(
                    "Failed to write consolidation watermark at {}: {}",
                    STATE_REL_PATH,
                    e.getMessage());
        }
    }

    private void writeWatermarkToStore(Instant ts) {
        Map<String, Object> value = new HashMap<>();
        value.put("ts", ts.toEpochMilli());
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            StoreItem item;
            try {
                item = baseStore.get(WATERMARK_NAMESPACE, WATERMARK_KEY);
            } catch (RuntimeException e) {
                log.warn("Failed to read consolidation watermark for CAS: {}", e.getMessage());
                return;
            }
            long expectedVersion = item != null ? item.version() : 0L;
            try {
                if (baseStore.putIfVersion(
                        WATERMARK_NAMESPACE, WATERMARK_KEY, value, expectedVersion)) {
                    return;
                }
            } catch (RuntimeException e) {
                log.warn("Failed to write consolidation watermark to store: {}", e.getMessage());
                return;
            }
        }
        log.warn(
                "Failed to advance consolidation watermark in store after {} CAS retries",
                MAX_CAS_RETRIES);
    }
}
