# 进程内批量作业调度服务（Spring Boot，纯内存）

一个不依赖数据库或外部中间件的进程内批量作业调度器：用内存结构模拟队列、租户配额与作业执行。
作业从提交到给出最终结论的全过程都是确定、可解释、可测试的。

## 能力概览

- 作业提交 / 查询 / 取消（Java API 与 REST API）。
- 幂等提交：同一 `idempotencyKey`（含并发重复提交）在全生命周期内只产生一个作业；重复提交返回已有作业（响应体 `reused=true`，HTTP `200`），不会新建、不会重复执行。
- 可区分的拒绝原因（`RejectReason`，REST 响应体 `reason` 字段）：
  - `INVALID_PARAMETER`（HTTP 400）：必填字段为空、`timeoutMillis<=0`、`maxAttempts<=1` 非法等；
  - `HANDLER_NOT_FOUND`（HTTP 409）：提交了未注册的 `jobType`；
  - `QUEUE_FULL`（HTTP 409）：全局活动作业数已达 `scheduler.global-queue-capacity`；
  - `TENANT_QUOTA_EXCEEDED`（HTTP 409）：该租户的提交配额已满；
  - `JOB_NOT_FOUND`（HTTP 404）、`ILLEGAL_STATE_TRANSITION`（HTTP 409，消息携带当前状态）。
- 被拒绝的提交在任何配额计数器上都不留下痕迹（参数与 handler 校验先于一切资源申请；容量/配额申请在同一临界区内，申请失败不建作业）。

## 配额模型（两级，按租户）

1. 提交配额（queued permits）：限制一个租户处于 QUEUED（含重试等待）+ RUNNING 的作业总数。提交成功时获取，作业到达任意终态时由“唯一的终态结算者”精确归还一次。
2. 并发配额（running permits）：限制一个租户同一时刻真正 RUNNING 的作业数。调度派发时获取；一次尝试结束（成功 / 失败 / 超时 / 取消 / 进入退避）即归还。

另外有全局约束：`scheduler.global-queue-capacity`（活动作业总数）和 `scheduler.worker-threads`（全局并发，worker 槽位用 `Semaphore` 控制）。

全部作业结束后，每个租户的两个已用计数都必须回到 0——测试中对这一点有断言；配额计数器做了“重复释放会抛错”的保护，泄漏或重复扣减会立刻暴露。

## 调度与公平策略

单 dispatcher 线程按 `scheduler.tick-millis` 周期性扫描 QUEUED 队列：

1. 过滤退避未到期（`notBefore`）的作业；
2. 计算**有效优先级** = 声明优先级按等待时长做老化：每等待一个 `scheduler.fairness-quantum-millis`，有效优先级提升一级（HIGH 为最高级）；
3. 按「有效优先级 → 全局提交序号 FIFO」排序；
4. 依次派发，前提是：全局 worker 有空槽、且该租户有空闲并发配额；否则跳过该租户继续考察后续作业。

防饿死保证：即使持续不断地提交 HIGH 作业，一个 LOW 作业等待的时间是有上界的——
`最多约 (LOW 与 HIGH 的级差) × fairness-quantum-millis` 后，它的有效优先级即追平 HIGH，随后按 FIFO 必然被派发。
`SchedulingAndQuotaTest#lowPriorityCannotBeStarvedByContinuousHighPriority` 在持续 HIGH 压力下验证了这一点。

## 状态流转与执行控制

合法迁移（`JobStatus#canTransitTo`）：

```
QUEUED  -> RUNNING | CANCELLED
RUNNING -> SUCCEEDED | FAILED | TIMED_OUT | CANCELLED | QUEUED(退避重试)
SUCCEEDED / FAILED / CANCELLED / TIMED_OUT 为终态，不可再迁移
```

- 非法迁移（如对终态作业取消、对运行中作业重复结算）被明确拒绝，原因 `ILLEGAL_STATE_TRANSITION`，错误消息携带当前状态。
- 每次尝试有独立的超时任务（`ScheduledExecutorService`）：超时后把状态置为 `TIMED_OUT`（若还能重试则回到 QUEUED 退避），并 `interrupt` 工作线程。
- 取消：QUEUED 作业立即 CANCELLED 且从未执行（`attemptCount=0`）；RUNNING 作业被中断并以 CANCELLED 结束。
- 单次尝试的结算通过“尝试结果认领”（CAS）保证唯一：handler 正常返回、抛异常、超时、取消四条路径只有一个赢家给出结论，因此同一作业不可能重复执行或出现互相矛盾的最终结论；worker 槽位、租户配额同样一次性归还。
- 单个作业抛异常/超时只影响该尝试：dispatcher 循环整体兜底捕获，worker 永远不会让异常逃逸，其他作业和调度器不受影响。

## 重试规则

- 失败与超时都消耗一次尝试；`maxAttempts` 为总尝试次数（1 = 不重试）。
- 退避为指数退避：`backoff(attempt) = min(retry-max-backoff-millis, retry-backoff-millis * 2^(attempt-1))`。
- 退避期间作业为 QUEUED 且并发配额已归还（不占用租户并发），但仍占用一个提交配额，直到终态。
- 尝试数达到上限：异常路径终态 `FAILED`，超时路径终态 `TIMED_OUT`。

## 配置项（`src/main/resources/application.properties`）

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `scheduler.global-queue-capacity` | 1024 | 全局 QUEUED+RUNNING 作业上限 |
| `scheduler.worker-threads` | 8 | worker 池大小，即全局并发上限 |
| `scheduler.tick-millis` | 20 | dispatcher 扫描周期（ms） |
| `scheduler.default-tenant-queued-quota` | 64 | 默认每租户提交配额 |
| `scheduler.default-tenant-running-quota` | 2 | 默认每租户并发配额 |
| `scheduler.tenant-queued-quota.<租户>` | - | 覆盖某租户提交配额 |
| `scheduler.tenant-running-quota.<租户>` | - | 覆盖某租户并发配额 |
| `scheduler.default-timeout-millis` | 5000 | 单次尝试默认超时，可按提交覆盖 |
| `scheduler.default-max-attempts` | 3 | 默认最大尝试次数，可按提交覆盖 |
| `scheduler.retry-backoff-millis` | 200 | 指数退避基数 |
| `scheduler.retry-max-backoff-millis` | 5000 | 退避上限 |
| `scheduler.fairness-quantum-millis` | 200 | 老化量子，防饿死灵敏度 |

内置示例 handler：`echo`（原样返回）、`sleep`（payload 为睡眠毫秒数，用于验证超时/取消）、`flaky`（恒失败，用于验证重试耗尽与故障隔离）。
自定义逻辑只需实现 `JobHandler`（`jobType()` + `execute(payload)`）并注册为 Spring Bean。

## REST API

- `POST /api/jobs`：提交。新建返回 `201`；幂等命中返回 `200` 且 `reused=true`。
- `GET /api/jobs/{jobId}`：查询完整状态（含 `attemptCount`、`result`、`errorMessage`、时间戳）。
- `POST /api/jobs/{jobId}/cancel`：取消；终态作业返回 `409 ILLEGAL_STATE_TRANSITION`。

提交请求体示例：

```json
{
  "idempotencyKey": "biz-order-20261008-001",
  "tenantId": "tenant-a",
  "jobType": "echo",
  "priority": "HIGH",
  "timeoutMillis": 3000,
  "maxAttempts": 3,
  "payload": "hello"
}
```

## 本地验证

```bash
mvn test                 # 运行全部 18 个用例（单元 + Spring HTTP 集成）
mvn spring-boot:run      # 启动服务（默认随机端口）
```

测试日志中每个业务用例都以 `[CASE:...]` 标记开始与 `pass` 结束，可直接看出覆盖了哪些业务：

- `SubmissionAndIdempotencyTest`：并发重复提交、非法参数、队列已满、租户配额不足；
- `SchedulingAndQuotaTest`：租户并发占满不突破且配额归零、优先级顺序、持续 HIGH 下 LOW 不被饿死；
- `ExecutionControlTest`：执行超时、取消排队/运行中作业、失败重试成功、重试耗尽、超时后重试、非法状态迁移、单作业故障隔离；
- `HttpApiIntegrationTest`：HTTP 端到端提交/查询/幂等、400/404/409 拒绝原因可区分。

作业生命周期与调度日志关键词：`[submit]`、`[dispatch]`、`[attempt-failed]`、`[retry]`、`[timeout]`、`[succeeded]`、`[terminal-failed]`、`[cancel]`。
