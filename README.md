# About

gmq 一个简单消息队列

[![Go](https://github.com/giant-stone/gmq/actions/workflows/go.yml/badge.svg)](https://github.com/giant-stone/gmq/actions/workflows/go.yml)
[![CodeQL](https://github.com/giant-stone/gmq/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/giant-stone/gmq/actions/workflows/codeql-analysis.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/giant-stone/gmq)](https://goreportcard.com/report/github.com/giant-stone/gmq)
[![GoDoc](https://godoc.org/github.com/giant-stone/gmq?status.svg)](https://godoc.org/github.com/giant-stone/gmq)


特性

- [ ] 处理消息失败默认自动存档
- [ ] 测试覆盖核心逻辑
- [ ] 支持类似 cron 定时任务
- [ ] 支持暂停队列消费
- [ ] 网页端队列管理工具,自带简易验证
- [x] 命令行队列管理工具
- [x] 自定义消费间隔

参考了 [hibiken/asynq](https://github.com/hibiken/asynq) 实现，差异

- 默认支持自定义消费间隔
  - 所有的消息队列都希望消费节点尽可能快，在某些场景下，我们希望实现自定义间隔消费消息——注意：不是重试，不是预定将来某个准确时刻!
- 消息自动保证入队某个时间段内唯一
  - asynq 需要同时指定生成 TaskId、Unique、Retention 三个参数

## Message State

消息状态

    pending 待消费  消息已入队等待空闲 worker (工作协程)消费
    waiting 等待中  worker 从 pending 消费消息但由于消费间隔限制，等待中
    processing 处理中  worker 正处理消息中
    failed 处理失败  消息处理失败
    archived 已存档  默认处理失败的消息自动存储，并且 3 天后自动删除

消息生命周期状态变化

    enqueue -> pending -> [waiting] -> processing -> success -> archived
                                                \                ^
                                                  \             /
                                                   \           /
                                                    +-> failed

存储中的消息总数 total = pending + waiting + processing + failed
