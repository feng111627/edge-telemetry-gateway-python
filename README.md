# Edge Telemetry Gateway

面向边缘设备的遥测接入与状态监控网关（FastAPI）。多来源、多格式的设备上报被统一
归一为内部结构，逐设备维护最新值与有界历史，支持在线状态判定与下游失败重试。

## 运行

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest/{source}` | 接入单条（JSON 对象）或一批（JSON 数组）上报 |
| GET | `/devices` | 列出所有已知设备 |
| GET | `/devices/{device_id}` | 查询设备最新值、last_seen 与历史 |
| GET | `/devices/{device_id}/status` | 查询在线状态（normal/lagging/offline） |
| GET | `/retry` | 查看重试队列（pending/dead） |
| POST | `/retry/run` | 手动处理当前到期的重试条目（与后台补偿同一套逻辑） |
| GET | `/retry/dead` | 查看最终放弃的数据集合 |
| POST | `/retry/dead/redrive` | 重新投递 dead 中的全部，或指定 `{"index": n}` 单条 |
| GET | `/metrics/summary` | 按设备汇总接入质量（各类计数与补偿积压） |

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
  `duplicate`，状态不变、不重复计数（并发重复上报也只计一次）。
- **乱序**：时间戳早于当前最新值的记录返回 `stale`，仍按时间序插入历史，但
  不覆盖最新值；比历史窗口内最旧记录还旧的数据同样返回 `stale`，并在容量
  淘汰中被立即挤出，结论确定。
- **容量**：每台设备历史默认上限 100 条（`TelemetryStore(history_limit=...)`），
  超出后按时间从旧到新淘汰。
- **隔离与并发**：状态按设备独立维护，内部以锁串行化写入，多设备并发上报
  互不串扰。

## 在线判定阈值

按设备最后一次上报时间 `last_seen` 距当前时间的时长判定（阈值见
`app/api.py` 的 `LAGGING_AFTER` / `OFFLINE_AFTER`）：

| 距现在时长 | 状态 |
| --- | --- |
| ≤ 60 秒 | `normal`（正常） |
| 60 秒 < 时长 ≤ 300 秒 | `lagging`（迟滞） |
| > 300 秒或从未上报 | `offline`（离线） |

设备时钟异常导致 `last_seen` 在未来时按 `normal` 处理，避免误判。

## 下游失败暂存与重试

下游转发失败的数据进入 `RetryQueue` 暂存，由后台自动补偿（见下节）：

- 第 n 次失败后等待 `min(base_delay * 2^(n-1), max_delay)` 秒（指数退避，
  间隔逐步拉长但有上限）；
- 超过 `max_attempts`（默认 5）次失败则放弃，移入 dead 集合；
- 重试按条独立调度，单条数据反复失败不会阻塞后续数据的接入与重试。

## 时效判定（太旧 / 未来时间戳）

接入时在归一化之后、状态变更之前做时效判定，结论确定且可重复：

| 情况 | 接入结果 `outcome` | 对设备状态的影响 |
| --- | --- | --- |
| `timestamp < now - max_past_age` | `too_old` | 无任何影响 |
| `timestamp > now + max_future_skew` | `future` | 无任何影响 |
| 时间戳无法解析 | 归一化阶段拒绝，`reason=bad_timestamp` | 无任何影响 |

- 太旧与未来时间戳是两种**独立**的结论，彼此区分，也与「格式解析不了」
  （`bad_timestamp`）区分；接入响应里的 `outcome` 一眼可辨。
- 这两种数据**不创建设备、不更新最新值、不写历史、不刷新 last_seen**
  （不会把设备误判成在线），也**不转发下游**；同一条数据重复上报结论一致
  （始终是 `too_old`/`future`，不会变成 `duplicate`）。
- 已被正常收下过的记录重复上报仍判 `duplicate`（幂等优先于时效）。
- 可配置项（`app/api.py`）：`MAX_PAST_AGE`（默认 24 小时）、
  `MAX_FUTURE_SKEW`（默认 300 秒）；存储层对应
  `TelemetryStore(max_past_age=..., max_future_skew=...)`，默认 `None`
  即不做时效判定，保持接入任意时间戳的旧行为。

## 后台自动补偿与最终放弃

下游转发失败的数据进入 `RetryQueue` 后，由后台线程 `RetryWorker`
（服务启动时自动运行，见 `main.py` lifespan）持续补偿，无需人工触发：

- 第 n 次失败后等待 `min(base_delay * 2^(n-1), max_delay)` 秒：间隔指数
  拉长但有**上限**（`RETRY_MAX_DELAY`，默认 60 秒），不会无限往后拖。
- 超过 `RETRY_MAX_ATTEMPTS`（默认 5）次失败则停止自动重试，移入 **dead**
  集合，可通过 `GET /retry/dead` 查看。
- `POST /retry/dead/redrive` 把 dead 中的数据重新放回队列等待投递：
  默认全部，`{"index": n}` 指定单条。重新投递是**移动**而非复制——
  同一数据不会重复送给下游，也不影响接入侧计数；投递成功即出队，
  再次失败重新走退避。
- 后台补偿与接入互不影响：转发在队列锁之外逐条进行，单条数据一直失败
  只会拉长自己的退避，不会卡住新数据接入，也不拖累其它数据的补偿。
- `POST /retry/run` 手动触发与后台线程共用同一套处理逻辑，语义一致。

## 接入质量汇总

`GET /metrics/summary` 按设备给出计数（`app/metrics.py`）：

| 字段 | 口径 |
| --- | --- |
| `accepted` | 正常收下并成为当前最新值 |
| `stale` | 乱序收下（写入历史但不更新最新值） |
| `duplicate` | 重复上报被去重 |
| `too_old` / `future` | 时效判定未通过（未改动任何状态） |
| `rejected` | 归一化阶段被拒绝（含各类 `reason`） |
| `pending_retry` | 当前在重试队列中等待补偿的条数（实时读数） |
| `dead` | 当前已最终放弃、等待人工处理的条数（实时读数） |

无法从上报中解析出 `device_id` 的拒绝计入 `<unknown>` 桶。
`pending_retry`/`dead` 是读取时从重试队列现算的当前值，其余为累计计数；
所有计数在锁内完成，多设备并发上报时读数准确。

## 与既有行为的兼容范围

本轮全部为增量能力，既有语义保持不变：

- 多来源归一与可区分拒绝原因（`not_an_object`/`missing_field`/`bad_type`/
  `bad_timestamp`/`unknown_source`）不变；
- 重复上报按 `(device_id, metric, seq)` 去重、返回 `duplicate` 不变；
- 乱序数据返回 `stale`、写历史但不覆盖最新值的结论不变；
- 历史容量淘汰（从旧到新）与设备间隔离、并发串行化不变；
- 在线状态判定（normal/lagging/offline 阈值与未来时钟容忍）不变；
- 存储层默认不做时效判定（`max_past_age`/`max_future_skew` 为 `None`），
  未配置时接入行为与之前完全一致；重试队列默认退避上限 300 秒，
  不影响既有退避序列。

## 验证方式

```bash
uv run pytest
```

测试输出会按业务模块打印「业务覆盖」标题及每条用例的通过/失败结果，覆盖：
归一化与拒绝原因、幂等/乱序/容量淘汰/设备隔离/并发接入、时效判定
（太旧/未来时间戳）、在线状态阈值、退避重试与上限封顶、后台自动补偿与
最终放弃后的重新投递、接入质量汇总（含并发计数准确性），以及端到端的
接入、查询与重试接口。

本地手动验证后台补偿与汇总：

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8000
# 另开终端：
curl -X POST localhost:8000/ingest/mqtt -H 'Content-Type: application/json' \
  -d '{"device_id":"d1","metric":"t","value":1.0,"timestamp":'"$(date +%s)"',"seq":1}'
curl localhost:8000/metrics/summary   # 查看按设备的接入质量汇总
curl localhost:8000/retry             # 查看补偿积压与最终放弃数量
```
