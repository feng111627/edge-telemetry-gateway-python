# Edge Telemetry Gateway

面向边缘设备的遥测接入与状态监控网关（FastAPI）。多来源、多格式的设备上报被统一
归一为内部结构，逐设备维护最新值与有界历史，支持在线状态判定、时间戳时效判定、
下游失败后台自动补偿与接入质量汇总。

## 运行

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

应用启动时自动开启后台补偿线程（守护线程），关闭时优雅停止。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest/{source}` | 接入单条（JSON 对象）或一批（JSON 数组）上报 |
| GET | `/devices` | 列出所有已知设备 |
| GET | `/devices/{device_id}` | 查询设备最新值、last_seen 与历史 |
| GET | `/devices/{device_id}/status` | 查询在线状态（normal/lagging/offline） |
| GET | `/metrics` | 按设备的接入质量汇总 |
| GET | `/retry` | 查看重试队列（pending/dead 及 dead 明细） |
| POST | `/retry/run` | 手动处理当前到期的重试条目 |
| POST | `/retry/redeliver` | 手动重新投递 dead 中的一条（`?item_id=`）或全部 |

## 数据归一规则

不同来源的字段名不同，统一映射为内部结构 `TelemetryRecord`
（`device_id, metric, value, timestamp, seq, source`）：

| 内部字段 | modbus | mqtt | http_raw |
| --- | --- | --- | --- |
| device_id | `dev` | `device_id` | `id` |
| metric | `reg` | `metric` | `name` |
| value | `val` | `value` | `reading` |
| timestamp | `ts`（Unix 秒） | `timestamp`（Unix 秒） | `time`（ISO8601） |
| seq | `seq` | `seq` | `n` |

`timestamp` 统一接受 Unix 秒（数字）或 ISO8601 字符串。校验规则：

- 上报必须是 JSON 对象，否则拒绝原因 `not_an_object`；
- 缺少任一必需字段，拒绝原因 `missing_field`（detail 中列出缺失字段）；
- `device_id`/`metric` 必须为非空字符串，`value` 必须为数字（布尔不算），
  `seq` 必须为整数，否则拒绝原因 `bad_type`；
- `timestamp` 无法解析时拒绝原因 `bad_timestamp`；
- 未注册的来源拒绝原因 `unknown_source`。

所有被拒绝的上报都会在响应中逐条给出 `reason` 与 `detail`，不会被静默丢弃。

## 幂等、乱序与容量淘汰

- **幂等**：以 `(device_id, metric, seq)` 去重。同一设备重复上报同一条数据返回
  `duplicate`，状态不变、不重复计数、不重复转发下游（并发重复上报也只计一次）。
- **乱序**：时间戳早于当前最新值的记录返回 `stale`，仍按时间序插入历史，但
  不覆盖最新值；比历史窗口内最旧记录还旧的数据同样返回 `stale`，并在容量
  淘汰中被立即挤出，结论确定。
- **容量**：每台设备历史默认上限 100 条（`TelemetryStore(history_limit=...)`），
  超出后按时间从旧到新淘汰。
- **隔离与并发**：状态按设备独立维护，内部以锁串行化写入，多设备并发上报
  互不串扰。

## 时间戳时效判定

归一化成功（时间戳可解析）的记录，在写入前按“当前时间”做确定性分类。
阈值在 `app/api.py` 顶部配置：

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `MAX_AGE` | 86400（24 小时） | 时间戳比当前早超过该秒数判为 `too_old` |
| `FUTURE_SKEW` | 300（5 分钟） | 时间戳比当前晚超过该秒数判为 `future`（容忍时钟偏差） |

判定规则（窗口边界含等于）：

- **正常 `accepted` / 乱序 `stale`**：时间戳落在 `[now-MAX_AGE, now+FUTURE_SKEW]`。
- **太旧 `too_old`**：`timestamp < now - MAX_AGE`。
  - 不更新设备 `latest`，不刷新 `last_seen`——不会因为补传旧数据把设备判成在线；
  - 仍按时间序插入历史，留下确定的历史痕迹（受容量淘汰约束）；
  - 不触发下游转发（`forwarded` 为 `null`），旧数据不代表当前状态；
  - 首次返回 `too_old` 并纳入去重；同一条数据再次上报返回 `duplicate`，
    历史不重复计数。
- **未来 `future`**：`timestamp > now + FUTURE_SKEW`。
  - 与 `too_old` 以及时间戳无法解析的 `bad_timestamp` 严格区分；
  - 不留任何痕迹（不建设备状态、不入历史、不更新最新值/last_seen、不转发），
    因此同一条未来数据无论上报多少次，结论始终是 `future`，也不会带偏设备状态。
- **时间戳解析失败**：仍走归一化拒绝 `bad_timestamp`，响应 `accepted=false`、
  带 `reason`；时效判定只处理“能解析”的时间戳。

响应中通过 `outcome` 一眼区分 `accepted` / `stale` / `duplicate` /
`too_old` / `future`；归一化被拒则没有 `outcome`，只有 `reason`。
`accepted` 仅对真正进入当前状态的 `accepted`/`stale` 为 `true`；
`duplicate`/`too_old`/`future` 均为 `false`，具体含义以 `outcome` 为准。

存储层 `TelemetryStore(max_age=None, future_skew=None)` 默认关闭时效判定，
与历史行为完全一致；网关在 `app/api.py` 显式开启。

## 在线判定阈值

按设备最后一次**有效**上报时间 `last_seen` 距当前时间的时长判定（阈值见
`app/api.py` 的 `LAGGING_AFTER` / `OFFLINE_AFTER`）：

| 距现在时长 | 状态 |
| --- | --- |
| ≤ 60 秒 | `normal`（正常） |
| 60 秒 < 时长 ≤ 300 秒 | `lagging`（迟滞） |
| > 300 秒或从未上报 | `offline`（离线） |

设备时钟异常导致 `last_seen` 在未来时按 `normal` 处理，避免误判。
太旧数据不刷新 `last_seen`，未来数据不产生 `last_seen`。

## 下游失败：自动补偿与最终放弃

下游转发失败的数据进入 `RetryQueue`，由后台 `RetryWorker` 自动补偿，
无需人工触发：

- **退避**：第 n 次失败后等待 `min(max_delay, base_delay * 2^(n-1))` 秒，
  间隔逐步拉长但有上限（默认 `base_delay=1s`、`max_delay=300s`），不会无限拖后；
- **最终放弃**：失败次数达到 `max_attempts`（默认 5）后停止自动重试，
  移入单独的 dead 集合；
- **互不阻塞**：重试按条独立调度，只取走“到期且未在投递中”的条目，
  单条一直失败或下游抛异常都只影响它自己，不拖累其他数据，也不卡住新数据接入；
- **容错**：补偿循环本身捕获异常，不会因单轮错误退出线程。

配置项：`RETRY_BASE_DELAY` / `RETRY_MAX_DELAY` / `RETRY_MAX_ATTEMPTS` /
`WORKER_INTERVAL`（均在 `app/api.py`）。

### dead 集合查看与手动重新投递

- `GET /retry` 返回 `pending`、`dead` 数量及 `dead_items` 明细
  （`id, attempts, device_id, metric, seq, timestamp, source, value`）；
- `POST /retry/redeliver`（不带参数）重新投递全部；
  `POST /retry/redeliver?item_id=<id>` 只重投一条；不存在的 id 安全无操作。
- 重投是**原子移出 dead 后立即重新入队**（尝试次数清零、立即到期）并尝试一轮：
  成功则出队，仍失败则按退避重新等待、再次达到上限后回到 dead。
  同一条 dead 数据不会被重复计数，也不会被重复送给下游
  （投递中的条目通过 inflight 标记去重）；重复调用不会产生额外副本。
- `POST /retry/run` 语义保持不变：手动处理一轮当前到期的条目。

## 接入质量汇总

`GET /metrics` 按设备返回接入质量读数，`pending`/`dead` 在查询时从重试队列
实时并入：

| 字段 | 统计口径 |
| --- | --- |
| `accepted` | 正常收下的条数，含乱序 `stale`（数据本身有效） |
| `duplicate` | 重复上报条数（含已留痕的太旧数据再次上报） |
| `too_old` | 首次判为太旧的条数 |
| `future` | 未来时间戳条数（重复上报每次都计入） |
| `rejected` | 归一化阶段被拒条数（各可区分原因统一计入此项） |
| `pending` | 该设备当前等待自动补偿的条数（实时） |
| `dead` | 该设备已最终放弃的条数（实时） |

说明：

- 归一化被拒时若能从上报中识别出非空 `device_id`，计入对应设备；
  识别不出来（如不是对象、缺设备字段）的拒绝不计入任何设备；
- 计数通过锁串行化，多设备高并发上报读数依然准确；
  `pending`/`dead` 来自队列瞬时快照，与接入计数口径独立、不会互相漂移。

## 与既有行为的兼容范围

本轮全部为增量能力，以下行为语义保持不变：

- 多来源字段归一、可区分的拒绝原因（`not_an_object` / `missing_field` /
  `bad_type` / `bad_timestamp` / `unknown_source`）；
- `(device_id, metric, seq)` 幂等去重与“并发只计一次”；
- 乱序 `stale` 结论、历史容量从旧到新淘汰、设备间隔离；
- 在线状态三档阈值与“未来 last_seen 按 normal”规则；
- `GET /retry` 的 `pending`/`dead` 字段、`POST /retry/run` 的处理语义；
  `RetryQueue.dead` 仍是 record 列表；
- `TelemetryStore` / `RetryQueue` 的默认构造行为不变
  （时效判定默认关闭；退避新增 `max_delay` 参数但有默认值）。

## 验证方式

```bash
uv run pytest
```

测试输出按业务模块打印「业务覆盖」标题及每条用例的通过/失败结果，覆盖：
归一化与拒绝原因、幂等/乱序/容量淘汰/设备隔离/并发接入、在线状态阈值、
时间戳时效判定（太旧/未来/边界/重复一致性）、退避重试与退避上限、
后台自动补偿与最终放弃、dead 集合手动重新投递（防重复）、
并发下质量汇总计数准确性，以及端到端的接入、查询、质量视图与重投接口，
并包含一个走应用 lifespan 的后台补偿集成用例。
