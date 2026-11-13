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
package io.agentscope.harness.agent.tool;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.memory.MemoryConfig;
import io.agentscope.harness.agent.workspace.WorkspaceConstants;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Appends user memories to {@code MEMORY.md} and today's daily ledger.
 *
 * <p>Appends within the consolidation body limit. Semantic deduplication and trimming remain
 * the responsibility of {@link io.agentscope.harness.agent.memory.MemoryConsolidator}.
 */
public class MemorySaveTool {

    private final WorkspaceManager workspaceManager;
    private final long maxMemoryChars;

    public MemorySaveTool(WorkspaceManager workspaceManager) {
        this(workspaceManager, MemoryConfig.DEFAULT_CONSOLIDATION_MAX_TOKENS);
    }

    public MemorySaveTool(WorkspaceManager workspaceManager, int maxMemoryTokens) {
        if (maxMemoryTokens <= 0) {
            throw new IllegalArgumentException("maxMemoryTokens must be positive");
        }
        this.workspaceManager = workspaceManager;
        this.maxMemoryChars = (long) maxMemoryTokens * 4;
    }

    @Tool(
            name = "memory_save",
            description =
                    "Persist one or more facts to long-term memory. Use whenever the user asks you"
                        + " to remember something, or when you observe important preferences,"
                        + " decisions, or context worth keeping across conversations. Do NOT use"
                        + " write_file or edit_file on MEMORY.md — always use this tool instead.")
    public ToolResultBlock memorySave(
            RuntimeContext runtimeContext,
            @ToolParam(
                            name = "content",
                            description =
                                    "Markdown bullet list of facts to remember. Each bullet should"
                                            + " be a concise, self-contained fact. Example:\\n"
                                            + "- User prefers dark mode\\n"
                                            + "- Project deadline is 2026-07-01")
                    String content) {
        if (content == null || content.isBlank()) {
            return ToolResultBlock.error("content is required");
        }

        RuntimeContext rc = runtimeContext != null ? runtimeContext : RuntimeContext.empty();

        String section = "\n" + content.strip() + "\n";

        AtomicTextFile memory = workspaceManager.atomicMemoryFile(rc, WorkspaceConstants.MEMORY_MD);
        boolean saved = false;
        for (int attempt = 0; attempt < 20; attempt++) {
            String before = memory.read();
            long size = (before == null ? 0L : before.length()) + section.length();
            if (size > maxMemoryChars) {
                return ToolResultBlock.error(
                        "MEMORY.md would exceed its "
                                + maxMemoryChars
                                + "-character limit by "
                                + (size - maxMemoryChars)
                                + ". Nothing was saved. Save less content or retry after existing"
                                + " memory is reduced; maintenance without new ledger entries"
                                + " does not free space.");
            }
            if (memory.compareAndSet(before, (before == null ? "" : before) + section)) {
                saved = true;
                break;
            }
        }
        if (!saved) {
            return ToolResultBlock.error(
                    "MEMORY.md changed repeatedly; nothing was saved. Retry later.");
        }

        String today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        String dailyPath = WorkspaceConstants.MEMORY_DIR + "/" + today + ".md";
        String dailyEntry =
                String.format(
                        "\n## Memory Save — %s\n%s\n", Instant.now().toString(), content.strip());
        workspaceManager.appendMemoryFileUtf8(rc, dailyPath, dailyEntry);

        long count = content.strip().lines().filter(l -> l.stripLeading().startsWith("-")).count();
        if (count == 0) {
            count = 1;
        }
        return ToolResultBlock.success(
                "Saved " + count + " memor" + (count == 1 ? "y" : "ies") + " to MEMORY.md");
    }
}
