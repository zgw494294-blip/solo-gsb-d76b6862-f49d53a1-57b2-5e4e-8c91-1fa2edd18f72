# 多副本离线写入核对 API

接收 2～8 个副本离线产生的 1～120 条 put/delete 事件（可乱序），校验
向量时钟因果关系，并按向量时钟偏序解析出每个 key 的有效版本：

- 唯一极大事件是 put → 返回其值
- 唯一极大事件是 delete → 标记为已删除
- 存在多个极大事件（互不可比，即使值相同）→ 报告冲突

此外，`/api/catchup` 接口在同一批事件之上，为目标副本生成**可直接重放的
增量补传计划**：传入目标对各来源已连续接收的最高 seq（`known_clock`），
缺失事件按依赖分轮返回。

`/api/resolve-conflict` 接口把**已确认的冲突处理结果**写成一条新的、
可重放的事件：在写入副本上追加 put/delete，其时钟并入所有冲突版本及该副本
此前见过的历史；返回新事件与追加后该 key 的裁决。

上述三个接口完全无状态，**请求数据不落库**。

另外还提供一组**持久化历史**接口（`/api/histories`）：多个调用方先凭一份
合法批次创建历史并获得 ID，之后只按 ID 读取、汇入新的离线事件、或按 ID 提交
冲突处理参数（不必再传整个批次）。汇入按 `(replica_id, seq)` 合并：内容完全
相同的事件忽略，同标识异内容返回 **409**；冲突提交时会重新核对当前极大版本，
过期返回 **409**。历史保存在本地 SQLite（Docker 下位于命名卷），容器重启后
仍可读取。

## 启动

需要 Docker 与 Docker Compose v2（`docker compose` 子命令）。

```bash
docker compose up --build -d
```

停止：

```bash
docker compose down
```

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| 健康检查 | <http://localhost:8000/health> |
| 裁决接口 | `POST http://localhost:8000/api/resolve` |
| 冲突处理事件接口 | `POST http://localhost:8000/api/resolve-conflict` |
| 增量补传接口 | `POST http://localhost:8000/api/catchup` |
| 创建持久历史 | `POST http://localhost:8000/api/histories` |
| 读取历史（批次+裁决） | `GET http://localhost:8000/api/histories/{id}` |
| 汇入离线事件 | `POST http://localhost:8000/api/histories/{id}/events` |
| 提交冲突处理 | `POST http://localhost:8000/api/histories/{id}/resolve-conflict` |
| Swagger UI | <http://localhost:8000/docs> |
| OpenAPI 文档 | <http://localhost:8000/openapi.json> |

容器内监听 `0.0.0.0:8000`，映射到宿主机 **8000** 端口。

## 请求格式

```json
{
  "replica_ids": ["r1", "r2"],
  "events": [
    {
      "replica_id": "r1",
      "seq": 1,
      "key": "name",
      "put": "alice",
      "clock": {"r1": 1, "r2": 0}
    },
    {
      "replica_id": "r2",
      "seq": 1,
      "key": "name",
      "delete": "delete",
      "clock": {"r1": 0, "r2": 1}
    }
  ]
}
```

约束：

- `replica_ids`：2～8 个非空且唯一的副本 ID。
- 每个事件属于其中某个副本；`seq` 从 1 开始，同一副本内不重复且必须连续
  （事件在请求中可乱序）。
- `put`（字符串，允许空串）与 `delete`（值必须是字符串 `"delete"`）二选一。
- `clock` 必须覆盖**全部**副本且无多余分量，分量为非负整数；
  本副本分量必须等于本事件 `seq`。
- 时钟任何分量都不得指向不存在的事件（0 表示尚不知道该副本的事件）。
- 本副本上一事件（`seq - 1`）以及其他副本分量所指的最新事件，其时钟必须
  逐分量 `<=` 当前时钟且至少一个分量严格更小。
- 任何违例整批返回 **HTTP 422**，错误详情中带 `event_index`（请求 `events`
  数组下标）指出具体事件。

## 可执行 HTTP 示例

### 1. 冲突：两个副本并发改同一个 key（值相同也冲突）

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

响应（结果按 key 排序；冲突事件按 `replica_id`、`seq` 排序）：

```json
{
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 2. 同步后再写入：put 生效、旧 delete 被遮蔽

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "session", "delete": "delete",
       "clock": {"r1": 2, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice2",
       "clock": {"r1": 3, "r2": 1}}
    ]
  }'
```

响应：

```json
{
  "results": [
    {
      "key": "name",
      "state": "put",
      "value": "alice2",
      "event": {"replica_id": "r1", "seq": 3, "op": "put", "value": "alice2",
                "clock": {"r1": 3, "r2": 1}}
    },
    {
      "key": "session",
      "state": "deleted",
      "event": {"replica_id": "r1", "seq": 2, "op": "delete", "value": null,
                "clock": {"r1": 2, "r2": 0}}
    }
  ]
}
```

### 3. 非法批次：时钟本副本分量不等于 seq → 422

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "v",
       "clock": {"r1": 2, "r2": 0}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body 例如：

```json
{
  "detail": [
    {
      "code": "clock_self_component",
      "message": "clock['r1'] = 2 must equal seq = 1",
      "event_index": 0
    }
  ]
}
```

## 冲突处理事件接口 `POST /api/resolve-conflict`

把确认过的冲突处理结果写成一条**可重放事件**。请求沿用裁决接口的
`replica_ids`、`events`（先按原规则整批校验），并额外携带：

- `key`：要消解的冲突 key（非空字符串）；
- `replica_id`：写入副本，必须属于 `replica_ids`；
- `versions`：当时看到的冲突版本列表，每项含 `replica_id`、`seq`；
  列表须无重复且**恰好等于**该 key 当前全部极大事件，且至少两项
  （列表顺序任意）；
- 处理结果二选一：`put`（字符串，允许空串）或 `delete`（值必须为
  字符串 `"delete"`）。

接口行为：

- 新事件的 `seq` 接续写入副本现有末尾（该副本此前无事件时为 `1`）；
- 新事件时钟逐分量取所有冲突版本时钟与写入副本此前历史的最大值，
  再把写入副本自身分量推进到新 `seq`——因此新事件严格支配全部冲突版本；
- 响应包含完整新事件 `event`（与事件请求线格式一致：`put` 或
  `delete` 二选一），以及追加后的该 key 裁决 `result`
  （形状与 `/api/resolve` 的单条结果一致）；
- 即使各冲突版本的值相同，冲突也必须经此接口显式消解；其他 key 的
  裁决不受影响；
- 新事件并入 `events` 后可直接通过原批次校验，并能由 `/api/catchup`
  按依赖安排补传。

状态码：

- **200**：冲突已按确认结果消解；
- **409**：`versions` 已过期/不完整（`conflict_versions_mismatch`），
  或该 key 当前已不冲突/批次中不存在（`key_not_conflicted`）；
  返回 409 时不生成事件；
- **422**：原批次校验失败，或写入副本不在 `replica_ids`
  （`unknown_replica_id`）、版本列表重复
  （`duplicate_conflict_version`）、原批次已满 120 条（`batch_full`）
  等结构性错误。

### 4. 消解并发冲突：追加支配两分支的 put

`r1/1` 与 `r2/1` 并发写入 `name`（两分支互不可比），确认采用 `r1` 写入
的 `"alice2"`；版本列表可以乱序：

```bash
curl -sS -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "alice2"
  }'
```

响应（新事件为 `r1/2`，时钟并入两个冲突分支）：

```json
{
  "event": {
    "replica_id": "r1",
    "seq": 2,
    "key": "name",
    "put": "alice2",
    "clock": {"r1": 2, "r2": 1}
  },
  "result": {
    "key": "name",
    "state": "put",
    "value": "alice2",
    "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "alice2",
              "clock": {"r1": 2, "r2": 1}}
  }
}
```

把返回的 `event` 追加到原批次后再请求 `/api/resolve`，`name` 即变为
普通 `put`；请求 `/api/catchup` 时它会排在所依赖的冲突版本之后补传。

### 5. 冲突已被新写入遮蔽（版本列表过期）→ 409

批次中 `r1/2` 因果上支配 `r2/1`，`name` 已不再冲突。仍按旧冲突提交时
返回 `409 Conflict`，且不生成事件：

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice",
       "clock": {"r1": 2, "r2": 1}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r1", "seq": 2},
      {"replica_id": "r2", "seq": 1}
    ],
    "put": "alice2"
  }'
```

```json
{
  "detail": [
    {
      "code": "key_not_conflicted",
      "message": "key 'name' is no longer in conflict"
    }
  ]
}
```

若 key 仍冲突但列表缺少某个极大版本、列入了非极大版本，或指向不存在的
`(replica_id, seq)`，返回 `409` 且错误码为 `conflict_versions_mismatch`。

## 增量补传接口 `POST /api/catchup`

请求沿用裁决接口的 `replica_ids`、`events`（按同样规则整批校验），额外携带
`known_clock`：目标对**每个**来源已连续接收的最高 seq。

```json
{
  "replica_ids": ["r1", "r2"],
  "known_clock": {"r1": 1, "r2": 0},
  "events": [ ... ]
}
```

`known_clock` 规则（违例返回 **422**）：

- 必须**恰好覆盖**所有 `replica_ids`，无缺失、无多余键；
- 每个分量为非负整数；
- 分量不得超过该来源在本批次中的最高 seq（批次中没有事件的来源只能填 `0`）；
- 若某个**已接收事件**（自身 `seq <= known_clock[本副本]`）的时钟任一分量
  超过对应的 `known_clock`，返回 `clock_exceeds_known_clock` 并附该事件的
  `event_index`。

缺失事件（`seq > known_clock[本副本]`）按**轮次**返回：

- 每一轮纳入当时所有依赖已满足的事件——其本副本上一事件（`seq - 1`）以及
  其他副本时钟分量所指事件，均已在目标端或在更早轮次中传输；
- 轮内事件按 `replica_id`、`seq` 排序；
- 事件保留原操作（`op`）、`value`（delete 时为 `null`）、`key` 和 `clock`；
- `progress` 为补传完成后目标对各来源的进度（即批次中各来源最高 seq）；
- 全部事件均已接收时 `rounds` 为空数组；
- 并发写入即使值相同也分别补传，不做去重或冲突合并。

### 6. 分轮补传：跨副本依赖逐轮解锁

目标已接收 `r1/1`（时钟 `{r1:1, r2:0}`），缺失 `r2/1`（依赖 `r1/1`）与
`r1/2`（依赖 `r2/1`），因此分为两轮：

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

响应（第 1 轮先补 `r2/1`，第 2 轮再补依赖它的 `r1/2`）：

```json
{
  "rounds": [
    [
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 1}}
    ],
    [
      {"replica_id": "r1", "seq": 2, "key": "name", "op": "put",
       "value": "alice2", "clock": {"r1": 2, "r2": 1}}
    ]
  ],
  "progress": {"r1": 2, "r2": 1}
}
```

### 7. 并发同值写入也分别补传

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 0, "r2": 0},
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

两个事件互不依赖，进入同一轮（按 `replica_id` 排序），即使值相同也都补传：

```json
{
  "rounds": [
    [
      {"replica_id": "r1", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 0, "r2": 1}}
    ]
  ],
  "progress": {"r1": 1, "r2": 1}
}
```

### 8. 全部已接收：轮次为空

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 1},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}}
    ]
  }'
```

```json
{
  "rounds": [],
  "progress": {"r1": 1, "r2": 1}
}
```

### 9. 非法请求：已接收事件的时钟超出 known_clock → 422

目标声称只连续收到 `r1/1` 且不知道 `r2` 的任何事件，但该事件时钟里
`r2 = 1`，与目标认知矛盾：

```bash
curl -sS -i -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body：

```json
{
  "detail": [
    {
      "code": "clock_exceeds_known_clock",
      "message": "event already received (replica 'r1', seq 1 <= known_clock 1) but its clock {'r1': 1, 'r2': 1} exceeds known_clock; components beyond target knowledge: {'r2': 1}",
      "event_index": 0
    }
  ]
}
```

其余 `known_clock` 违例的错误码：`known_clock_coverage`（缺失/多余分量）、
`known_clock_ahead_of_batch`（分量超过批次最高 seq）。

## 持久化历史接口 `/api/histories`

让多个调用方在**同一份持久历史**上汇入离线事件、提交冲突处理。数据库为本地
SQLite 文件，路径由环境变量 `HISTORY_DB_PATH` 决定；`docker compose` 启动时
固定为容器内 `/data/history.db` 并挂载命名卷 `history-data`，因此
`docker compose restart` / 容器重建后历史仍可读取。原三个无状态接口与
`docker compose up --build -d` 启动方式保持不变。

接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/histories` | 用合法的 `replica_ids` + `events` 创建历史，返回 `{"id": ...}`（201） |
| `GET` | `/api/histories/{id}` | 读取完整原格式批次（事件按 `replica_id`、`seq` 排序）及原裁决，二者来自同一已提交状态 |
| `POST` | `/api/histories/{id}/events` | 按 ID 汇入一批事件（请求体只有 `events`） |
| `POST` | `/api/histories/{id}/resolve-conflict` | 按 ID 提交原冲突处理参数（`key`、`replica_id`、`versions`、`put`/`delete`），不再传整个批次 |

汇入规则：

- 按 `(replica_id, seq)` 合并：同标识且内容完全相同的事件忽略（可重复发送）；
  同标识但 `key`/操作/值/`clock` 任一不同，返回 **409**
  （`event_content_mismatch`，错误中带汇入批次的 `event_index`）；
- 合并后的整份历史重新执行原校验（因果、连续性、clock 覆盖等），失败
  **422**；事件总数上限仍为 **120**（超出为 `batch_full`，422）；
- 每次写入**全部生效或全部不生效**（单事务），失败时历史保持原样。

冲突提交规则：

- 服务端在写锁内重新读取当前历史，`versions` 必须**恰好等于**该 key 当前全部
  极大事件；过期/不完整/key 已不冲突 → **409**（与无状态接口同一套错误码），
  且不生成事件；
- 新事件 `seq` 接续写入副本现有末尾（此前无事件时为 1），时钟并入全部冲突
  版本与写入副本历史，随后随整份历史一起原子写入；
- 并发提交同一冲突时只有一个能成功，其余拿到 409，不会重复占用序号。

不存在的 ID 在三个按 ID 访问的接口上都返回 **404**（`history_not_found`）。

### 10. 创建历史并获得 ID

```bash
curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

```json
{ "id": "f82cdf528c99461c9a6becef9edddd1f" }
```

创建请求的约束与 `/api/resolve` 完全一致；非法批次返回 **422**，不会生成
历史。

### 11. 按 ID 读取：完整原格式批次 + 原裁决（同一已提交状态）

```bash
curl -sS http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f
```

事件在响应中按 `replica_id`、`seq` 排序（与创建时的提交顺序无关）：

```json
{
  "id": "f82cdf528c99461c9a6becef9edddd1f",
  "replica_ids": ["r1", "r2"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0}},
    {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
     "clock": {"r1": 0, "r2": 1}}
  ],
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "bob",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 12. 汇入离线事件：重复忽略、新事件合并

请求体只需要 `events`（不再传 `replica_ids` 或整个批次）。下面把已有的
`r1/1`（内容相同，忽略）与新事件 `r2/2` 一起汇入：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 2, "key": "other", "put": "x",
       "clock": {"r1": 0, "r2": 2}}
    ]
  }'
```

响应与 `GET` 形状一致（`id`、`replica_ids`、排序后的完整 `events`、
`results`）。同标识异内容返回 **409**，且整批（含批次内其他合法事件）都不
写入：

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "event with replica_id 'r1', seq 1 already exists in this history with different content; ...",
      "event_index": 0
    }
  ]
}
```

合并后违反因果/连续性等原校验，或超出 120 条上限时返回 **422**。

### 13. 按 ID 提交冲突处理：核对当前 versions、接续序号

请求体就是原冲突处理参数（与 `/api/resolve-conflict` 相比少了
`replica_ids`、`events`）：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "chosen"
  }'
```

```json
{
  "id": "f82cdf528c99461c9a6becef9edddd1f",
  "event": {
    "replica_id": "r1",
    "seq": 2,
    "key": "name",
    "put": "chosen",
    "clock": {"r1": 2, "r2": 1}
  },
  "result": {
    "key": "name",
    "state": "put",
    "value": "chosen",
    "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "chosen",
              "clock": {"r1": 2, "r2": 1}}
  }
}
```

生成的事件已持久化到历史中（`GET` 即可看到，事件按 `replica_id`、`seq`
排序）。再用旧的 `versions` 提交会返回 **409**（`key_not_conflicted` 或
`conflict_versions_mismatch`），不生成事件、不占用 `r2/2` 序号。

### 14. 重启容器后历史仍在

数据在命名卷 `history-data` 中，重启/重建容器（不改卷）后按原 ID 仍可读取：

```bash
docker compose restart
curl -sS http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f
```

如需连数据一起清除：`docker compose down -v`。

## 本地开发（不使用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

本地运行时持久化历史默认写入当前目录的 `history.db`（可用环境变量
`HISTORY_DB_PATH` 指定其他路径，例如 `HISTORY_DB_PATH=/tmp/history.db`）；
三个无状态接口不依赖数据库。
