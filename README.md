# Edge Telemetry Gateway（边缘设备遥测接入网关）

基于 FastAPI 的边缘设备遥测接入与状态监控服务。不同来源、不同字段格式的设备
上报在网关内被归一为统一结构，完成**留痕（最新值 + 有界历史）、幂等去重、
在线判定、下游失败暂存重试**，并通过 HTTP 接口可靠查询。

## 1. 架构与处理链路

```
HTTP 上报
  └─> 归一化 normalizer（结构/类型/时间戳校验，失败返回可区分原因）
        └─> 设备存储 store（每设备锁 + 幂等去重 + 最新值 + 有界历史）
              └─> 下游处理器 processor（可插拔；失败入退避重试队列）
查询侧：状态 / 历史 / 在线情况 / 重试统计
```

代码分层：

| 模块 | 职责 |
| --- | --- |
| `app/config.py` | 全部阈值与容量配置（`Settings`） |
| `app/errors.py` | 拒绝原因枚举 `RejectReason` 与 `IngestError` |
| `app/models.py` | 统一内部模型 `NormalizedRecord` / `DeviceState` / `IngestResult` |
| `app/normalizer.py` | 多格式检测与归一化、时间戳校验、幂等键生成 |
| `app/store.py` | 每设备并发隔离、有界有序历史、去重指纹、在线判定 |
| `app/retry_queue.py` | 指数退避重试堆、死信队列、投递统计 |
| `app/service.py` | 编排整条链路、批量接入、后台重试循环 |
| `app/api.py` | HTTP 路由 |

## 2. 数据归一规则

网关自动识别并支持三种来源格式，最终统一为内部结构
`NormalizedRecord(device_id, ts, received_at, measurements, source_format, message_id, dedup_key)`：

### 2.1 `sensor_v1`（温湿度传感器；时间戳为毫秒 epoch）

```json
{
  "device_id": "s-1",
  "ts_ms": 1728000000000,
  "temperature": 23.5,
  "humidity": 60.1,
  "msg_id": "abc-123"
}
```

- `temperature` / `humidity` 至少提供一个；指标按名称排序后统一存放。
- `ts_ms` 除以 1000 换算为秒级 epoch。

### 2.2 `gateway_v2`（网关；时间戳为 ISO-8601 字符串）

```json
{
  "device": {"id": "g-1"},
  "timestamp": "2024-10-04T10:00:00Z",
  "metrics": {"cpu": 0.42, "mem": 0.8},
  "seq": 123
}
```

- 设备 ID 取自 `device.id`；`timestamp` 支持 `Z` 后缀，无时区时按 UTC 处理。
- `seq` 作为该来源的消息标识参与幂等。

### 2.3 `generic`（通用格式；时间戳为秒 epoch）

```json
{
  "device_id": "x-1",
  "ts": 1728000000,
  "measurements": [{"metric": "voltage", "value": 220.1}],
  "message_id": "可选"
}
```

- `measurements` 必须是非空数组，元素含非空字符串 `metric` 与数值 `value`。

### 2.4 统一规则

- 指标统一为 `Measurement(metric: str, value: float)`，布尔值不接受为数值。
- 指标按 `(metric, value)` 排序，保证同一内容的字节/字段顺序差异产生相同指纹。
- `received_at` 为网关接收时刻；设备时间与接收时间分别保存，互不覆盖。

### 2.5 拒绝原因（不会静默丢数据）

归一化失败时返回 `accepted=false` 与下列可机读原因之一：

| `reason` | 含义 |
| --- | --- |
| `malformed_json` | 请求体不是合法 JSON（HTTP 400） |
| `not_an_object` | 上报内容不是 JSON 对象 |
| `missing_device_id` | 设备 ID 缺失或为空 |
| `unknown_format` | 缺少任何已知格式的特征字段 |
| `missing_field` | 必填字段缺失（时间戳、指标等） |
| `bad_type` | 字段类型错误（指标非数值、metrics 非对象等） |
| `bad_timestamp` | 时间戳无法解析或非有限数值 |
| `future_timestamp` | 时间戳领先当前时间超过 `max_clock_skew_seconds` |
| `expired_timestamp` | 时间戳早于当前时间超过 `expire_after_seconds` |

### 2.6 幂等（重复上报结果稳定、不重复计数）

- 有消息标识时（`msg_id` / `seq` / `message_id`）：同一设备同一标识即视为同一条，
  即使指标值不同也直接幂等命中，**保留首次内容**，不计数、不重投下游。
- 无消息标识时：幂等键为
  `sha256(device_id | ts | 排序后的指标内容)` 的内容指纹。
- 每设备幂等指纹有界保留（`dedup_window`，默认 4096 条，FIFO 淘汰）。

## 3. 最新值、历史与乱序/过期处理

- 每个设备维护一个 `latest` 最新值和一段按时间升序排列的历史。
- **容量上限**：每设备 `history_capacity`（默认 100），超出后**最旧的记录先淘汰**。
- **并发隔离**：每设备一把 `asyncio.Lock`，同设备写入串行、跨设备互不阻塞，
  多设备并发上报不会串扰或错位。
- **乱序（晚到数据）的确定结论**：
  仍在时间戳有效窗口内、但早于当前最新值的数据会按时间序插入历史，
  **但不刷新最新值**；因此历史始终有序，最新值始终是时间戳最大的一条。
- **过期 / 超前时间戳**：在归一化层按 `expire_after_seconds` /
  `max_clock_skew_seconds` 直接拒绝（见 2.5），不进入存储。

## 4. 在线判定阈值

按设备**最后接收时间** `received_at` 与当前时间的差值判定（与设备时钟无关）：

| 状态 | 条件（默认阈值，可在 `app/config.py` 调整） |
| --- | --- |
| `online` 正常 | 距今 < `stale_after_seconds`（60 秒） |
| `stale` 迟滞 | 60 秒 ≤ 距今 < `offline_after_seconds`（300 秒） |
| `offline` 离线 | 距今 ≥ 300 秒 |
| `unknown` 未知 | 从未收到过该设备数据 |

## 5. 下游失败暂存与重试

- 下游处理器通过 `TelemetryService.set_processor()` 注册；处理失败**不影响接入结论**，
  数据被暂存到重试队列。
- 重试间隔指数退避：
  `delay = min(retry_base_delay * retry_backoff ** (attempts-1), retry_max_delay)`
  ，默认 1s → 2s → 4s … 上限 60s。
- 队列按“下次可见时间”排序，每轮只处理到期条目；**某条数据反复失败进入退避后，
  不会阻塞其后到期的数据**。
- 重试达到 `retry_max_attempts`（默认 5）后进入**死信队列**，仍可通过
  `/retry/stats` 观察数量，不再参与重试。
- 应用通过 lifespan 启动后台 pump（间隔 0.1s）自动补偿；也可用
  `POST /retry/pump` 手动触发一轮（测试与排障用）。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ingest` | 接入单条上报 |
| POST | `/ingest/batch` | 批量接入（JSON 数组，逐条独立判定） |
| GET | `/devices/{id}/status` | 最新状态与在线情况 |
| GET | `/devices/{id}/history?limit=N` | 按时间升序的历史（最近 N 条） |
| POST | `/retry/pump` | 手动触发一轮重试 |
| GET | `/retry/stats` | 暂存/死信/投递统计 |

快速试用：

```bash
uv run uvicorn main:app --reload
curl -s localhost:8000/devices/s-1/status
```

## 7. 验证方式

```bash
uv sync                       # 安装依赖（含 pytest）
uv run pytest                 # 全部 46 个用例
uv run pytest -m store        # 只跑“设备存储”业务
uv run pytest -s              # 同时查看每条用例的开始/通过日志
```

测试按业务分为五组（`normalizer` / `store` / `retry_queue` / `service` / `api`），
跑完会在终端输出**业务覆盖汇总**：逐组列出用例数、通过/失败数以及每个用例的
名称与结果，可直接核对覆盖的业务范围。

覆盖的边界场景包括：

- 三种来源格式的正常接入、毫秒/ISO/秒时间戳统一换算；
- 畸形 JSON、非对象、缺字段、类型错误、布尔冒充数值、无法识别格式；
- 未来时间戳、过期时间戳、晚到乱序数据；
- 同消息 ID / 同内容重复上报的幂等与“不重复计数、不重复投递”；
- 历史容量淘汰最旧记录、晚到数据插入有序历史但不刷新最新值；
- 多设备高并发接入互不串扰、同设备高并发重复只计一次；
- 下游失败暂存、退避间隔逐次拉长、死信、单条失败不阻塞后续、后台自动补偿；
- 在线状态 online → stale → offline 的随时间迁移。
