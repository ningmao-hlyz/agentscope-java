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

import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ModelRegistry;
import io.agentscope.harness.agent.context.ContextPolicy;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import java.time.Duration;
import java.util.Objects;

/**
 * Unified configuration for the long-term memory pipeline (flush + consolidation +
 * maintenance). Pair with {@link CompactionConfig} for the in-context summarization pipeline.
 *
 * <p>Three independent LLM operations:
 * <ul>
 *   <li>Flush extracts facts into daily ledgers; see {@link #flushPrompt()} and {@link #flushTrigger()}.</li>
 *   <li>Consolidation merges ledgers into MEMORY.md; see {@link #consolidationPrompt()} and {@link #consolidationMinGap()}.</li>
 *   <li>Compaction summarizes conversation history; see {@link CompactionConfig}.</li>
 * </ul>
 * Flush budgeting and optional admission apply to all extraction triggers.
 *
 * <p>Flush and consolidation use independent throttle windows. A throttled policy allows the
 * first eligible call immediately; its minimum gap applies only between subsequent runs and is
 * not an initial delay. {@link FlushTrigger#never()} disables only the per-call flush.
 */
public final class MemoryConfig {

    /** Default {@code consolidationMaxTokens}. */
    public static final int DEFAULT_CONSOLIDATION_MAX_TOKENS = 4_000;

    /** Default {@code consolidationMinGap} — matches {@code MemoryMaintenanceMiddleware}. */
    public static final Duration DEFAULT_CONSOLIDATION_MIN_GAP = Duration.ofMinutes(30);

    /** Default retention before a daily ledger is archived. */
    public static final int DEFAULT_DAILY_FILE_RETENTION_DAYS = 90;

    /** Total deadline for one flush, including extraction, admission and all source batches. */
    public static final Duration DEFAULT_FLUSH_TIMEOUT = Duration.ofMinutes(2);

    /** Strategy for the per-call flush hook. See {@link FlushTrigger}. */
    public enum FlushMode {
        /** Flush after every agent call. */
        ALWAYS,
        /** Disable per-call memory extraction (native session logging still runs). */
        NEVER,
        /** Flush at most once per {@link FlushTrigger#minGap()}. */
        THROTTLED
    }

    /**
     * Trigger policy for {@link io.agentscope.harness.agent.middleware.MemoryFlushMiddleware}.
     *
     * <p>A throttled trigger allows the first eligible call immediately. Its minimum gap is
     * measured between subsequent per-call flushes, independently of consolidation maintenance.
     * {@link #never()} disables only this per-call flush path.
     *
     * <p>{@code throttled(Duration.ZERO)} normalises to {@link #always()} so callers do not
     * need a special branch for the degenerate case.
     */
    public static final class FlushTrigger {

        private static final FlushTrigger ALWAYS_INSTANCE =
                new FlushTrigger(FlushMode.ALWAYS, Duration.ZERO);
        private static final FlushTrigger NEVER_INSTANCE =
                new FlushTrigger(FlushMode.NEVER, Duration.ZERO);

        private final FlushMode mode;
        private final Duration minGap;

        private FlushTrigger(FlushMode mode, Duration minGap) {
            this.mode = mode;
            this.minGap = minGap;
        }

        public static FlushTrigger always() {
            return ALWAYS_INSTANCE;
        }

        public static FlushTrigger never() {
            return NEVER_INSTANCE;
        }

        public static FlushTrigger throttled(Duration minGap) {
            if (minGap == null) {
                throw new IllegalArgumentException("minGap must not be null");
            }
            if (minGap.isNegative()) {
                throw new IllegalArgumentException("minGap must not be negative");
            }
            if (minGap.isZero()) {
                return ALWAYS_INSTANCE;
            }
            return new FlushTrigger(FlushMode.THROTTLED, minGap);
        }

        public FlushMode mode() {
            return mode;
        }

        public Duration minGap() {
            return minGap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FlushTrigger other)) return false;
            return mode == other.mode && Objects.equals(minGap, other.minGap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, minGap);
        }

        @Override
        public String toString() {
            return mode == FlushMode.THROTTLED
                    ? "FlushTrigger{THROTTLED, minGap=" + minGap + "}"
                    : "FlushTrigger{" + mode + "}";
        }
    }

    private final Model model;
    private final String flushPrompt;
    private final String consolidationPrompt;
    private final int consolidationMaxTokens;
    private final Duration consolidationMinGap;
    private final int dailyFileRetentionDays;
    private final FlushTrigger flushTrigger;
    private final ContextPolicy flushContextPolicy;
    private final ContextPolicy consolidationContextPolicy;
    private final MemoryAdmission admission;
    private final GenerateOptions flushOptions;
    private final Duration flushTimeout;

    private MemoryConfig(Builder b) {
        this.model = b.model;
        this.flushPrompt = b.flushPrompt;
        this.consolidationPrompt = b.consolidationPrompt;
        this.consolidationMaxTokens = b.consolidationMaxTokens;
        this.consolidationMinGap = b.consolidationMinGap;
        this.dailyFileRetentionDays = b.dailyFileRetentionDays;
        this.flushTrigger = b.flushTrigger;
        this.flushContextPolicy = b.flushContextPolicy;
        this.consolidationContextPolicy = b.consolidationContextPolicy;
        this.admission = b.admission;
        this.flushOptions = b.flushOptions;
        this.flushTimeout = b.flushTimeout;
    }

    /**
     * Optional model override for memory operations (flush + consolidation).
     * {@code null} means use the agent's primary model.
     */
    public Model model() {
        return model;
    }

    /**
     * Override for the flush prompt. {@code null} means use
     * {@link MemoryFlushManager#DEFAULT_FLUSH_PROMPT}.
     */
    public String flushPrompt() {
        return flushPrompt;
    }

    /**
     * Override for the consolidation prompt. {@code null} means use
     * {@link MemoryConsolidator#DEFAULT_CONSOLIDATION_PROMPT}.
     *
     * <p>Custom prompts must contain exactly two {@code %d} placeholders (max-tokens and
     * max-chars, in that order). The Builder enforces this at construction time.
     */
    public String consolidationPrompt() {
        return consolidationPrompt;
    }

    public int consolidationMaxTokens() {
        return consolidationMaxTokens;
    }

    /**
     * Minimum gap between consolidation/maintenance runs. The first eligible call runs
     * immediately; this duration is not an initial delay and is independent of
     * {@link #flushTrigger()}.
     */
    public Duration consolidationMinGap() {
        return consolidationMinGap;
    }

    public int dailyFileRetentionDays() {
        return dailyFileRetentionDays;
    }

    public FlushTrigger flushTrigger() {
        return flushTrigger;
    }

    /** Whole-request extraction budget, independent of compaction. */
    public ContextPolicy flushContextPolicy() {
        return flushContextPolicy;
    }

    /** Whole-request consolidation budget, independent of extraction and compaction. */
    public ContextPolicy consolidationContextPolicy() {
        return consolidationContextPolicy;
    }

    /** Optional candidate gate; {@code null} disables admission. */
    public MemoryAdmission admission() {
        return admission;
    }

    /** Extraction generation overrides, or {@code null} for defaults. */
    public GenerateOptions flushOptions() {
        return flushOptions;
    }

    public Duration flushTimeout() {
        return flushTimeout;
    }

    /** Returns the default configuration, with semantic admission disabled. */
    public static MemoryConfig defaults() {
        return new Builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Model model = null;
        private String flushPrompt = null;
        private String consolidationPrompt = null;
        private int consolidationMaxTokens = DEFAULT_CONSOLIDATION_MAX_TOKENS;
        private Duration consolidationMinGap = DEFAULT_CONSOLIDATION_MIN_GAP;
        private int dailyFileRetentionDays = DEFAULT_DAILY_FILE_RETENTION_DAYS;
        private FlushTrigger flushTrigger = FlushTrigger.always();
        private ContextPolicy flushContextPolicy = ContextPolicy.defaults();
        private ContextPolicy consolidationContextPolicy = ContextPolicy.defaults();
        private MemoryAdmission admission;
        private GenerateOptions flushOptions;
        private Duration flushTimeout = DEFAULT_FLUSH_TIMEOUT;

        /**
         * Sets a dedicated model for memory operations (flush + consolidation),
         * allowing a lighter/cheaper model than the agent's primary reasoning model.
         * When not set, the agent's primary model is used.
         */
        public Builder model(Model model) {
            this.model = model;
            return this;
        }

        /**
         * Sets a dedicated model for memory operations by model id string
         * (e.g. {@code "openai:gpt-4.1-mini"}). Legacy eager semantics: resolved once at build
         * time, not per call.
         *
         * @see ModelRegistry#resolve(String)
         */
        public Builder model(String modelId) {
            this.model = ModelRegistry.resolve(modelId);
            return this;
        }

        /**
         * Overrides the prompt used by {@link MemoryFlushManager#flushMemories}.
         * {@code null} restores the default. The text is passed verbatim as a SYSTEM
         * message — it does not need any placeholders.
         */
        public Builder flushPrompt(String flushPrompt) {
            this.flushPrompt = flushPrompt;
            return this;
        }

        /**
         * Overrides the prompt used by {@link MemoryConsolidator#consolidate}. The prompt
         * is rendered via {@link String#format} with two {@code int} arguments
         * (max-tokens, max-chars), so a custom prompt MUST contain exactly two {@code %d}
         * placeholders. {@code null} restores the default.
         *
         * @throws IllegalArgumentException if the prompt does not contain exactly two
         *     {@code %d} placeholders
         */
        public Builder consolidationPrompt(String consolidationPrompt) {
            if (consolidationPrompt != null) {
                int count = countOccurrences(consolidationPrompt, "%d");
                if (count != 2) {
                    throw new IllegalArgumentException(
                            "consolidationPrompt must contain exactly two %d placeholders "
                                    + "(max-tokens, max-chars), found "
                                    + count);
                }
            }
            this.consolidationPrompt = consolidationPrompt;
            return this;
        }

        /** Consolidation token target; four times this value caps MEMORY.md writes in characters. */
        public Builder consolidationMaxTokens(int consolidationMaxTokens) {
            if (consolidationMaxTokens <= 0) {
                throw new IllegalArgumentException(
                        "consolidationMaxTokens must be positive, got " + consolidationMaxTokens);
            }
            this.consolidationMaxTokens = consolidationMaxTokens;
            return this;
        }

        /**
         * Minimum gap between consolidation/maintenance runs. The first eligible call runs
         * immediately; this duration is not an initial delay and is independent of
         * {@link MemoryConfig#flushTrigger()}. Must not be null.
         */
        public Builder consolidationMinGap(Duration consolidationMinGap) {
            if (consolidationMinGap == null) {
                throw new IllegalArgumentException("consolidationMinGap must not be null");
            }
            if (consolidationMinGap.isNegative()) {
                throw new IllegalArgumentException("consolidationMinGap must not be negative");
            }
            this.consolidationMinGap = consolidationMinGap;
            return this;
        }

        /** Days before a daily ledger is moved to {@code memory/archive/}. */
        public Builder dailyFileRetentionDays(int dailyFileRetentionDays) {
            if (dailyFileRetentionDays <= 0) {
                throw new IllegalArgumentException(
                        "dailyFileRetentionDays must be positive, got " + dailyFileRetentionDays);
            }
            this.dailyFileRetentionDays = dailyFileRetentionDays;
            return this;
        }

        /** Trigger policy for the per-call flush hook. Must not be null. */
        public Builder flushTrigger(FlushTrigger flushTrigger) {
            if (flushTrigger == null) {
                throw new IllegalArgumentException("flushTrigger must not be null");
            }
            this.flushTrigger = flushTrigger;
            return this;
        }

        /** Selection and final validation share this policy; its output reserve caps generation. */
        public Builder flushContextPolicy(ContextPolicy policy) {
            this.flushContextPolicy = Objects.requireNonNull(policy);
            return this;
        }

        /** Selection and final validation share this consolidation policy. */
        public Builder consolidationContextPolicy(ContextPolicy policy) {
            this.consolidationContextPolicy = Objects.requireNonNull(policy);
            return this;
        }

        /** Null disables semantic admission. The extension or application supplies the judge. */
        public Builder admission(MemoryAdmission admission) {
            this.admission = admission;
            return this;
        }

        /** Total flush deadline; completed batches survive a timeout. Must be positive. */
        public Builder flushTimeout(Duration timeout) {
            if (timeout == null || timeout.isZero() || timeout.isNegative())
                throw new IllegalArgumentException("flushTimeout must be positive");
            this.flushTimeout = timeout;
            return this;
        }

        /** Per-request generation options. Use the output parameter supported by the adapter. */
        public Builder flushOptions(GenerateOptions options) {
            if (options != null) {
                if (options.getModelName() != null)
                    throw new IllegalArgumentException(
                            "Select the extraction model with MemoryConfig.model");
                Integer tokens = options.getMaxTokens();
                Integer completion = options.getMaxCompletionTokens();
                if (tokens != null && completion != null)
                    throw new IllegalArgumentException("Set only one flush output token limit");
                if ((tokens != null && tokens <= 0) || (completion != null && completion <= 0))
                    throw new IllegalArgumentException("Flush output token limit must be positive");
            }
            this.flushOptions = options;
            return this;
        }

        public MemoryConfig build() {
            return new MemoryConfig(this);
        }

        private static int countOccurrences(String haystack, String needle) {
            int count = 0;
            int idx = 0;
            while ((idx = haystack.indexOf(needle, idx)) != -1) {
                count++;
                idx += needle.length();
            }
            return count;
        }
    }
}
