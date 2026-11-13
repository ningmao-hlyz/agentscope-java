---
title: "检查长期记忆的价值与事实冲突"
---

`JevMemoryGate` 评审候选事实的长期价值及事实冲突。`assess()` 返回判断结果，由业务处理写入；`admission()` 接入 Harness 的可选 `MemoryAdmission`，由 Flush 处理写入和重试。

## 示例场景：记住偏好，但不要悄悄覆盖旧事实

用户明确要求“以后回答简洁一点”，应用准备把它存为长期偏好。如果候选与已有偏好相反，需要核查来源是否明确支持替代旧信息。

以下沿用 `assess()` 的原有政策：判断持久事实，拒绝缺乏明确替代依据的冲突；方法本身不写入记忆，owner 检查在请求模型前完成。

## 1. 构造带归属和来源的候选

```java
import io.agentscope.extensions.judge.jev.application.JevMemoryGate;
import java.util.List;

String owner = "user-42";
var candidate = new JevMemoryGate.Fact(
    owner, "偏好简洁的回答", "本轮用户明确要求");
var existingFacts = List.<JevMemoryGate.Fact>of();
```

`owner` 应来自认证后的用户身份；`source` 应描述应用实际掌握的来源证据。填写一个来源字符串不等于完成来源验证，不能把其他用户的记忆混进这次评审。

## 2. 在真正写入前评审

```java
var gate = new JevMemoryGate(judge);
var pending = gate.assess(owner, candidate, existingFacts);
var observed = pending.doOnNext(result -> {
    System.out.println("准入状态：" + result.status());
    System.out.println("逐项结果：" + result.findings());
});
```

`judge` 初始化见[应用组件配置](/v2/zh/jev/guides/application-api#初始化)。应用应把这个 Mono 组合到自己的写入流程，先观察结果再决定是否接管。只有 PASS 表示两项评审都通过，仍须经过宿主权限、来源和幂等检查。

| 状态 | 在偏好记忆中的处理 |
|---|---|
| PASS | 可以进入应用的写入流程 |
| FAIL | 保留当前记忆，不自动覆盖；核查不符合的条件 |
| INCONCLUSIVE | 证据不足，继续澄清或暂不写入 |
| ERROR | 本次没有有效判断，不能按通过处理 |

## 3. 接入 Harness Flush

```java
HarnessAgent.builder()
    .model(model)
    .workspace(workspace)
    .memory(MemoryConfig.builder()
        .admission(gate.admission())
        .build())
    .build();
```

默认关闭，配置后应用于各 Flush 入口：PASS 写入，FAIL 消费但不写，ERROR／INCONCLUSIVE 不消费本批。恢复直接使用已准备输出；准备前失败则下次重抽、重判。切换政策不重放已消费来源。

Harness 适配器明确接纳有持久价值的事实、偏好、规则和决定；与旧事实冲突时，来源须明确支持对同一对象、同一范围的纠正或已生效变更。更晚出现的陈述本身、引用或未确认提议不构成变更依据。通过的纠正追加到日流水账，后续 consolidation 才更新 MEMORY，期间新旧事实可能共存。

来源是本批新增对话，旧事实限于预算内条目及本批先前通过的候选。跨会话更新可能让快照过期；准入不保证全局无冲突，不拦截 `memory_save` 或 consolidation，也不减少抽取调用。预算和恢复协议见[记忆](/v2/zh/docs/harness/memory#增量抽取与恢复)。

## 运行与验证

按[案例构建步骤](/v2/zh/jev/guides/agent-integration-example#构建一次运行多个案例)设置 `JEV_CP` 后执行 `assess()` 示例：

```bash
java -cp "$JEV_CP" io.agentscope.examples.jev.JevApplicationScenarios memory
```

用例演示无冲突时 PASS、预设高冲突时 FAIL，两个分支实际写入数都为 0。概率由离线夹具提供，不是模型效果测量。[JevApplicationScenarios 完整源码](/examples/jev/source/JevApplicationScenarios.java.txt)

## 边界与错误

候选与现有事实必须全部属于显式 owner，否则调用模型前抛出参数错误。内置条件 `durable` 期望 true，`conflict` 期望 false，期望概率 ≥0.8 通过、≤0.2 失败，中间区间不确定；这些默认阈值未经项目真实回放校准。替换准入逻辑时实现 `MemoryAdmission`；自定义 JEV 问题时使用 [JevJudge](/v2/zh/jev/guides/judge-api)。

单独构造 Gate 不会启用拦截，须配置 `MemoryConfig.admission` 或显式调用。准入失败不删除旧记忆，上下文压缩的结论也不构成长期记忆写入许可。超时和取消沿用 Judge 契约。
