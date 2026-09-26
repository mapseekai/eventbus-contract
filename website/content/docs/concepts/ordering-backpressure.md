---
title: 顺序、消费分配与背压
weight: 50
---

顺序、并发和资源上限是同一组取舍：提高同时处理的数量通常提高吞吐，也扩大了重排和重复的处理面。

## 顺序与消费分配

`OrderingMode::Key` 表示按消息 key 维持顺序的意图；当订阅语义要求 ordered key 时，配置只能搭配
`OrderingMode::Key`。它不意味着跨 key 的全局顺序，也不能免除重试和多消费者带来的设计考量。

当前 `StreamBus` 尚未实现按 key 的串行调度，会在订阅时明确拒绝 `OrderingMode::Key`，即使
`max_in_flight = 1` 也一样；单个订阅的串行处理不能保证同组多个消费者之间的顺序。

`ConsumerBalanceMode` 描述同一组消费者如何获得消息：

| Mode | 含义 |
|---|---|
| `Competing` | 同一 consumer group 内的消费者竞争一条消息，借由更多实例或本地并发提高吞吐。 |
| `FanOut` | 每个订阅者各自获得消息，适合独立的下游处理。 |

后端能力决定最终行为。当前 `StreamBus` 实现 `Competing` 的 consumer-group 语义，并会在订阅时拒绝
`FanOut`；需要 fan-out 时应选择支持它的后端或创建相互独立的消费组。

## 背压的硬约束

`BackpressurePolicy` 必须满足：`max_pending_acks >= max_in_flight > 0`。此外，
`max_batch_size` 也必须大于 0，且不能超过 `max_in_flight` 或 `max_pending_acks`。

`SubscriptionConfig` 可以在顶层设置 `max_in_flight` / `max_pending_acks`，也可以嵌套一个
`backpressure` policy。两处同时提供非零值时必须一致；否则验证失败，避免同一个订阅有两套互相矛盾
的容量定义。未显式设置时，配置会规范化为 `max_in_flight = 1`，并令 `max_pending_acks` 为其两倍。

更大的 `max_in_flight` 可增加吞吐，却会让同一 key 或相关业务操作更容易并行交错。先按顺序约束选择
key 与消费组，再用小而可观测的 in-flight 上限开始调优；不要把更多并发误认为更强的投递保证。
