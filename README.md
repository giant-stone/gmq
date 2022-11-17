# About

gmq 一个支持自定义消费速率的简单消息队列。

[![Go](https://github.com/giant-stone/gmq/actions/workflows/go.yml/badge.svg)](https://github.com/giant-stone/gmq/actions/workflows/go.yml)
[![CodeQL](https://github.com/giant-stone/gmq/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/giant-stone/gmq/actions/workflows/codeql-analysis.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/giant-stone/gmq)](https://goreportcard.com/report/github.com/giant-stone/gmq)
[![GoDoc](https://godoc.org/github.com/giant-stone/gmq?status.svg)](https://godoc.org/github.com/giant-stone/gmq)

特性

- [x] 处理消息失败默认自动存档
- [x] 支持暂停队列消费
- [ ] 网页端队列管理工具,自带简易验证
- [x] 支持类似 cron 定时任务
- [x] 命令行队列管理工具
- [x] 自定义消费速率
- [x] 自定义时限内消息去重
- [x] 中间件
- [x] 测试覆盖核心逻辑

功能和接口设计都参考了 [hibiken/asynq](https://github.com/hibiken/asynq) 实现，差异：

- 默认支持自定义消费速率（自定义从队列取消息的时间间隔）
  - 所有的消息队列都希望消费节点尽可能快，在某些场景下，我们希望实现自定义间隔消费消息——注意：不是重试，不是预定将来某个准确时刻!
- 消息自动保证入队某个时间段内唯一，`client.Enqueue(msg, gmq.UniqueIn(time.Hour*12))` 即可——无论消费结果成功还是失败，约束在 UniqueIn 内都有效
  - asynq 需要同时指定生成 TaskId、Unique、Retention 三个参数，但默认消费成功后 Unique 限制自动删除

和其他消息队列差异：

- 性能和可靠性不是首要考虑，**易用性、实用性、满足真正多样化场景需求放在第一优先考虑**

## Quickstart

安装 Go stable 版本（当前应为 1.17 或以上）后，创建项目目录，通过以下命令安装 gmq 库

```sh
go get -u github.com/giant-stone/gmq
```

实现一个最简单的消息队列生产-消费 demo：

```go
package main

import (
	"context"
	"flag"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gutil"

	"github.com/giant-stone/gmq/gmq"
)

func main() {
	const defaultDsn = "redis://127.0.0.1:6379/0"
	var dsn string
	flag.StringVar(&dsn, "d", defaultDsn, "data source name of redis")
	flag.Parse()

	// 初始化 glogging 打印日志，[glogging](https://github.com/giant-stone/go#custom-logging) 将集成格式化、自动切割、日志分级，满足大部分服务 99% 以上场景
	glogging.Init([]string{"stdout"}, "debug")

	broker, err := gmq.NewBrokerRedis(dsn)
	gutil.ExitOnErr(err)

	ctx := context.Background()
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared})
	mux := gmq.NewMux()

	// 用一个子协程模拟实现消息队列生产者
	cli, err := gmq.NewClient(dsn)
	gutil.ExitOnErr(err)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				{
					// 如果不指定队列名，gmq 默认使用 gmq.DefaultQueueName
					cli.Enqueue(ctx, &gmq.Msg{Payload: []byte(`{"data":"hello world"}`)})
					time.Sleep(time.Millisecond * 500)
				}
			}
		}
	}()

	// 设置消息消费者，mux 类似于 web 框架中常用的多路复用路由处理，
	// 消费消息以队列名为 pattern，handler 为 gmq.HandlerFunc 类型函数
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		glogging.Sugared.Debugf("consume id=%s queue=%s payload=%s", msg.GetId(), msg.GetQueue(), string(msg.GetPayload()))
		return nil
	}))

	if err := srv.Run(mux); err != nil {
		glogging.Sugared.Fatal("srv.Run ", err)
	}
	glogging.Sugared.Debug("gmq server started")
	select {}
}

```

完整代码见 examples/pushmsg/main.go .

如果不加队列配置，默认全速消费，每个队列会生成和机器 CPU (`runtime.NumCPU()`)数量一致 workers

    gmq - queue foo - worker a
                    - worker b
                    - ...
        - queue bar - worker aa
                    - worker bb
                    - ...

在某些场合下，我们希望只有一个 worker 并且按一定间隔从队列中消费消息，可以通过队列配置控制：

```go
	// ...
	func workIntervalFunc() time.Duration {
		return time.Second * time.Duration(3)
	}

	// ...

	slowQueueName := "seckill"
	srv := gmq.NewServer(ctx, broker, &gmq.Config{Logger: glogging.Sugared,
		QueueCfgs: map[string]*gmq.QueueCfg{
			// 队列名 - 队列配置
			slowQueueName: gmq.NewQueueCfg(
				gmq.OptQueueWorkerNum(1),                    // 配置限制队列只有一个 worker
				gmq.OptWorkerWorkInterval(workIntervalFunc), // 配置限制队列消费间隔为每 3 秒从队列取一条消息
			),
		},
	})

	// ...
```

完整代码见 examples/queuecfg/main.go .

消费效果输出

```
2022-07-05T10:56:00.779+0800    DEBUG   workInterval/main.go:65 consume id=a8f39ae3-de80-40a2-bdf4-a6cbbcf7cf59 queue=default payload={"data":"hello world"}
2022-07-05T10:56:01.794+0800    DEBUG   workInterval/main.go:65 consume id=077d0595-9e8c-4211-b450-86fd4215fe40 queue=default payload={"data":"hello world"}
2022-07-05T10:56:01.797+0800    DEBUG   workInterval/main.go:65 consume id=4c361217-59bc-4058-88f2-6aff96f51373 queue=default payload={"data":"hello world"}
2022-07-05T10:56:02.701+0800    DEBUG   workInterval/main.go:70 consume id=116b8e73-9a8c-4832-b14a-9efba742159f queue=seckill payload={"fulluri":"worldgold.xxoo/123"}
2022-07-05T10:56:02.813+0800    DEBUG   workInterval/main.go:65 consume id=6cc3331a-f054-4155-8b04-d57922ecb4c9 queue=default payload={"data":"hello world"}
2022-07-05T10:56:02.815+0800    DEBUG   workInterval/main.go:65 consume id=0cb698e1-00ed-4353-9e00-a5102fa1f0cc queue=default payload={"data":"hello world"}
2022-07-05T10:56:03.828+0800    DEBUG   workInterval/main.go:65 consume id=4e1db153-3177-4ffc-82f1-8f31c0a4dddf queue=default payload={"data":"hello world"}
2022-07-05T10:56:03.833+0800    DEBUG   workInterval/main.go:65 consume id=d6cd0836-29df-448b-943a-0082d7bfe7fe queue=default payload={"data":"hello world"}
2022-07-05T10:56:04.842+0800    DEBUG   workInterval/main.go:65 consume id=69569019-8448-47ef-9277-83ef85b4ef70 queue=default payload={"data":"hello world"}
2022-07-05T10:56:04.844+0800    DEBUG   workInterval/main.go:65 consume id=2bd5db36-c13c-49de-abf7-26e2aad681df queue=default payload={"data":"hello world"}
2022-07-05T10:56:05.712+0800    DEBUG   workInterval/main.go:70 consume id=9c1909f0-3f83-438e-8c98-c0dd7d7d1150 queue=seckill payload={"fulluri":"worldgold.xxoo/123"}
2022-07-05T10:56:05.852+0800    DEBUG   workInterval/main.go:65 consume id=9f5d7b9f-fbf1-48cf-8ccc-e626f198e72d queue=default payload={"data":"hello world"}
2022-07-05T10:56:05.857+0800    DEBUG   workInterval/main.go:65 consume id=ccd2f9ae-4b74-4b92-9321-d65872ae7b36 queue=default payload={"data":"hello world"}
// ...
```

default 队列无限制 worker 数量，每 500 毫秒 生产 1 条，消费无间隔，所以每秒消费 2 条 ~= 500 毫秒/条；
seckill 队列限制 worker 一个，每 500 毫秒 生产 1 条，消费间隔 3 秒一条，所以每秒消费 0.33/条 ~= 3 秒/条，和期望一致。

## Advanced Topics

更多功能见 [giant-stone/gmq/wiki](https://github.com/giant-stone/gmq/wiki)

## Run tests

run tests on Windows

```
set GMQ_RDS="redis://localhost:6379/14?dial_timeout=1s&read_timeout=1s&max_retries=1"
go test -v ./...
```

run tests on macOS/Linux

```
export GMQ_RDS="redis://localhost:6379/14?dial_timeout=1s&read_timeout=1s&max_retries=1"
go test -v ./...
```
