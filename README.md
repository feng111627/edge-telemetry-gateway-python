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
| POST | `/retry/run` | 处理当前到期的重试条目 |

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

下游转发失败的数据进入 `RetryQueue` 暂存：

- 第 n 次失败后等待 `base_delay * 2^(n-1)` 秒（指数退避，间隔逐步拉长）；
- 超过 `max_attempts`（默认 5）次失败则放弃，移入 dead 列表；
- 重试按条独立调度，单条数据反复失败不会阻塞后续数据的接入与重试。

## 验证方式

```bash
uv run pytest
```

测试输出会按业务模块打印「业务覆盖」标题及每条用例的通过/失败结果，覆盖：
归一化与拒绝原因、幂等/乱序/容量淘汰/设备隔离/并发接入、在线状态阈值、
退避重试，以及端到端的接入、查询与重试接口。
