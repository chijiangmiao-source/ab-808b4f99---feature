# 辐射实验舱维护事务锁管理服务（rad-lock-mgr）

调用方以**稳定追踪标识（session_id + event_id）**创建会话，按序提交至多
**4 个事务**、**48 个事件**（`begin` / 共享 `S` 或独占 `X` 申请 / `upgrade` /
`release` / `commit`），逐步观察持锁集合、等待队列、被撤销事务与裁决序号。

仅依赖 Python 3.11 标准库。

## 锁与裁决语义

- **兼容性**：S 与 S 兼容；X 与任何锁不兼容（含其他 S 持有者）。
- **严格 FIFO 等待队列**：任何资源只要存在等待者，后来的请求一律入队尾；
  即使后到请求与当前持锁兼容，也**不得越过队首**。队首可授予时才推进，
  连续可授予的队首（一批 S）连续授予。
- **升级**：持 S 者请求 X 视为升级；存在其他 S 持有者或队首有等待者时
  进入等待（等待期间保留自己的 S）。
- **死锁裁决**：等待图出现环（SCC）时，**撤销环上 begin 开始序号最大的事务**，
  原子清除其全部等待与锁，标记 aborted，再推进队列；可能暴露新环时继续裁决。
- **拒绝且不改状态**（不占用裁决序号）：标识相同而内容不同（409）、非法释放、
  重复 begin、对未 begin / 已撤销 / 已提交事务的操作、非法 action/mode、
  超出事务/事件上限。
- **幂等**：重复提交内容相同的稳定事件标识，返回**首次裁决**（`replayed=true`）。
- **等待历程**：请求或升级首次进入等待时建档一条不可变历程，包含触发事件、
  资源、目标模式、当时队列位置与直接阻塞者（不相容持锁事务 + 全部在先等待项）；
  队列推进获授或死锁裁决撤销其事务时，仅为该历程**补齐一次**终局
  （`granted` / `aborted`）与对应裁决序号；重放与被拒事件不新增、不改写历程。
  历程随状态原子落盘，重启后已结束与仍在等待的历程均与当前锁状态一致。
- **崩溃恢复**：普通事件仅在完整裁决后落盘；撤销事件先把「事件前」状态
  （带 pending 载荷）原子落盘并 fsync，再执行撤销，最后写入完整裁决。
  在撤销持久化阶段中断后重启，磁盘状态只可能是**该事件之前**或**完整裁决之后**，
  默认前滚（`RECOVERY_MODE=rollforward`）；设为 `rollback` 则回到事件前，
  调用方可凭同一稳定事件标识重新提交继续推进。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/sessions/{sid}/events` | 提交事件，返回裁决（新事件 201，重放 200） |
| GET | `/sessions/{sid}` | 当前持锁、等待队列、撤销事务、裁决序号 |
| GET | `/sessions/{sid}/events/{eid}` | 查询某稳定事件标识的首次裁决 |
| GET | `/sessions/{sid}/transactions/{tid}/wait-history` | 查询某事务的完整等待历程（按裁决序号稳定排序） |

事件体：

```json
{"event_id": "e1", "tid": "T1", "action": "request",
 "resource": "CH-A", "mode": "S"}
```

裁决中包含 `seq`、`enqueued`、`newly_granted`、`aborted` 以及完整 `state`。

等待历程查询返回：

```json
{"ok": true, "session_id": "s1", "tid": "T2",
 "history": [{"event_id": "u1", "tid": "T2", "resource": "CH-A",
              "mode": "X", "upgrade": true, "position": 1, "enq_seq": 5,
              "blocked_by": {"holders": ["T1"], "waiters": []},
              "outcome": "granted", "outcome_seq": 6}]}
```

`outcome` 为 `null` 表示仍在等待；否则为 `granted` / `aborted` 并附 `outcome_seq`。
不存在（含跨会话）的事务返回 404 `transaction_not_found`，尚未产生等待的事务
返回 404 `wait_history_not_found`，不存在的会话返回 404 `session_not_found`。

## 运行

宿主机端口可配置（默认 8080）：

```bash
docker compose up app            # http://localhost:8080/health
HOST_PORT=9090 docker compose up app
```

## 一键验收

verify 容器执行：代码测试（unittest，含真实子进程的崩溃前滚/回滚恢复）、
字节码构建检查、对 app 服务的 API/HTTP 冒烟（复现锁升级死锁撤销、
FIFO 队列推进、幂等与拒绝语义、撤销持久化阶段崩溃后的自动重启恢复），
以及等待历程验收（升级等待后获授、死锁撤销终局、重启后历程查询与拒绝语义），
最终以退出码报告结果：

```bash
docker compose build
docker compose up --abort-on-container-exit --exit-code-from verify verify
```

验收脚本会通过受控崩溃注入（`ALLOW_CRASH_INJECTION=true`，事件体加
`"crash": true`）在撤销检查点落盘后令进程退出，app 容器自动重启并前滚恢复。

## 本地测试（无需 Docker）

```bash
python3 -m unittest discover -s tests -v
```
