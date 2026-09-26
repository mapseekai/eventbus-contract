---
title: 投递与完成
weight: 30
---

订阅的 `AckMode` 决定消息在 handler 前、后或由 handler 本身何时完成。选择时先问：业务失败后，
这条消息是否必须仍有机会重试？

| 模式 | 完成时机 | 适用场景 |
|---|---|---|
| `Manual` | Handler calls `ack`, `nack`, or `retry`. | Business code controls success. |
| `AutoOnReceive` | Before handler execution. | Handler-failure loss is acceptable. |
| `AutoOnHandlerSuccess` | After handler returns `Ok`. | Common at-least-once handling. |

## 手动完成

待确认（pending）指后端已经投递、但尚未收到 ACK 的消息；重新认领（reclaim）是把超过空闲阈值的
待确认消息转交给消费者再次处理。死信主题（`dead_letter_topic`）是无法继续处理的消息的隔离目的地；重试预算则是
`max_retry` 允许的额外尝试次数。

`DeliveryControl` 的 `ack`、`nack` 和 `retry` 都接收 `self: Box<Self>`。调用一个控制方法就消费这个
handle，编译器阻止同一投递随后再走另一个完成分支。

- `ack` 确认当前投递。
- `nack` 不进入重试循环；配置了 `dead_letter_topic` 时，先发布到死信主题，发布成功后才确认原消息。没有配置 dead_letter_topic 时，nack 会确认并丢弃原消息。
- `retry` 在未耗尽预算时将消息重新发布，并写入重试 header；达到 `max_retry` 后改为发布到死信主题。两种路径都只会在发布成功后确认原消息。

重试发布前会等待 `retry_backoff`。等待期间原消息保持 pending，并占用 in-flight 名额；
因此慢速重试会降低可用并发，优雅关闭也会等待这段退避完成。
启用重试时，`retry_backoff` 必须小于 `claim_idle_timeout`。实际配置还应让同组所有消费者的
idle 超时大于“处理耗时 + 退避 + 重试发布及 ACK 耗时”，否则仍在执行的消息可能被再次认领；
当前实现不会自动续期 pending 消息的租约。

Redis 后端将重试 header 与 PEL 投递次数合并计算 `attempt`，并在 Redis 中保存该消费组的
首次接收时间，供重启后的消费者继续使用；ACK 同时清理这份元数据。

如果死信发布或重试发布失败，控制方法会在确认原消息之前返回错误；原消息仍处于 pending，之后可以被
reclaim 并再次投递。

`max_retry` 表示**初次投递之外**允许的重试次数：`max_retry = 0` 时，第一次失败就已耗尽预算。若
重试耗尽而没有 `dead_letter_topic`，当前 `StreamBus` 会返回配置验证错误，而不是伪装成已经死信。

## 订阅生命周期

`close()` 可以被超时取消，之后仍可再次等待 `close()` 或调用 `abort()` 强制终止。
`is_running()` 在实际消费任务退出后才返回 false。强制终止不代替业务事务回滚或幂等处理。

## 自动模式的边界

`AutoOnReceive` 在执行 handler 之前确认消息，因此 handler panic 或返回错误后的丢失是该模式的明确
代价。`AutoOnHandlerSuccess` 只会在 `handle` 返回 `Ok` 后自动确认；handler 已主动完成 delivery 时，
自动完成代理会避免第二次完成。

自动确认改善控制流，但不能替代业务幂等：处理结果已写入数据库、随后确认失败时，消息仍可能再次
投递。下一页说明如何把这个现实纳入保证和事务设计。
