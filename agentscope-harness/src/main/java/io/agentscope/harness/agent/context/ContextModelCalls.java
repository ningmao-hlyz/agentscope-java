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
package io.agentscope.harness.agent.context;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.middleware.ModelCallInput;
import io.agentscope.core.middleware.ModelRequestPreparer;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import java.util.List;
import java.util.UUID;
import reactor.core.publisher.Flux;

/** Budget-only auxiliary call boundary: never recursively compacts or injects task state. */
public final class ContextModelCalls {
    private ContextModelCalls() {}

    /** Resolves finite auxiliary output limits without copying provider credentials. */
    public static GenerateOptions outputOptions(
            Model model, ContextPolicy policy, GenerateOptions overrides) {
        GenerateOptions configured = model.getDefaultGenerateOptions();
        Integer tokens = configured == null ? null : configured.getMaxTokens();
        Integer completion = configured == null ? null : configured.getMaxCompletionTokens();
        if (tokens != null && tokens <= 0) tokens = null;
        if (completion != null && completion <= 0) completion = null;
        if (tokens == null && completion == null)
            tokens =
                    policy.budget(new ModelCallInput(List.of(), List.of(), null, model))
                            .outputTokens();
        GenerateOptions defaults =
                GenerateOptions.builder().maxTokens(tokens).maxCompletionTokens(completion).build();
        boolean explicit =
                overrides != null
                        && (overrides.getMaxTokens() != null
                                || overrides.getMaxCompletionTokens() != null);
        if (!explicit
                && model.getContextWindowSize() > 0
                && Math.max(tokens == null ? 0 : tokens, completion == null ? 0 : completion)
                        > model.getContextWindowSize() / 2) {
            int capped = Math.max(1, model.getContextWindowSize() / 2);
            defaults =
                    completion != null
                            ? GenerateOptions.builder().maxCompletionTokens(capped).build()
                            : GenerateOptions.builder().maxTokens(capped).build();
        }
        GenerateOptions resolved = GenerateOptions.mergeOptions(overrides, defaults);
        if (policy.budget(new ModelCallInput(List.of(), List.of(), resolved, model)).inputTokens()
                == 0)
            throw new IllegalArgumentException(
                    "Auxiliary output and safety reservations leave no input budget");
        return resolved;
    }

    public static Flux<ChatResponse> auxiliary(Model model, List<Msg> messages) {
        return auxiliary(model, messages, ContextPolicy.defaults(), null);
    }

    public static Flux<ChatResponse> auxiliary(
            Model model, List<Msg> messages, ContextPolicy policy, GenerateOptions options) {
        return Flux.deferContextual(
                subscriber ->
                        new HarnessContextBuilder(policy, null, null)
                                .prepare(
                                        null,
                                        RuntimeContext.empty(),
                                        new ModelCallInput(messages, List.of(), options, model),
                                        UUID.randomUUID().toString(),
                                        ModelRequestPreparer.Purpose.SUMMARY)
                                .flatMapMany(
                                        input ->
                                                io.agentscope.core.session.SessionModels.wrap(
                                                        input.model(),
                                                        subscriber.getOrDefault(
                                                                io.agentscope.core.agent.AgentBase
                                                                        .RUNTIME_CONTEXT_KEY,
                                                                RuntimeContext.empty()),
                                                        "SUMMARY")
                                                        .stream(
                                                                input.messages(),
                                                                input.tools(),
                                                                input.options())));
    }
}
