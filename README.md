# queue-nsq

基于 [NSQ](https://nsq.io) 与官方 Go 客户端 [go-nsq](https://github.com/nsqio/go-nsq) 实现的消息队列适配器，为 [glue](https://github.com/zhiyunliu/glue) 框架提供标准的生产者/消费者接口。实现结构参考 [queue-nats](https://github.com/zhiyunliu/queue-nats)。

## 功能特性

| 特性 | 说明 |
|---|:---|
| 发布/订阅 | ✅ Topic = 队列名，Channel = 消费者组 |
| 竞争消费 | ✅ 同 Channel 多实例竞争消费 |
| 消息持久化 | ✅ 由 nsqd 持久化 |
| Ack / Nack | ✅ Finish / Requeue |
| 自动重试 | ✅ 可配置 MaxAttempts |
| 死信队列 | ✅ 超过重试次数写入配置的死信 Topic |
| 延迟投递 | ✅ 使用 NSQ 原生 DeferredPublish |
| 批量发送 | ✅ 顺序 Publish 多条 |

## 安装

```bash
go get github.com/zhiyunliu/queue-nsq@latest
```

## 包结构

```
queue-nsq/
├── nsq/           NSQ 生产者/消费者实现（queue.IMQP / queue.IMQC）
└── nsq.go         根包注册 nsq 适配器
```

引入即注册适配器：

```go
import _ "github.com/zhiyunliu/queue-nsq"
```

## 配置说明

配置文件（如 `config.json`）示例：

```json
{
    "queues": {
        "default": {
            "proto": "nsq",
            "addr": "nsq://default",
            "delay_interval": 2,
            "group_name": "my-consumer-group",
            "deadletter_queue": "my.deadletter"
        }
    },
    "nsq": {
        "default": {
            "nsqd_addrs": ["127.0.0.1:4150"],
            "lookupd_addrs": ["127.0.0.1:4161"]
        }
    },
    "servers": {
        "mqcserver": {
            "config": {"addr": "queues://default", "status": "start"},
            "tasks": [
                {"queue": "order.created", "service": "/order/created"},
                {"queue": "order.paid", "service": "/order/paid"}
            ]
        }
    }
}
```

### 配置项说明

#### `nsq` 服务器连接配置

| 字段 | 类型 | 说明 |
|---|---|---|
| `nsqd_addrs` | `[]string` | nsqd TCP 地址列表，生产者使用第一个；例：`127.0.0.1:4150` |
| `lookupd_addrs` | `[]string` | nsqlookupd HTTP 地址列表，消费者发现 nsqd 用；例：`127.0.0.1:4161`。若为空则使用 `nsqd_addrs` 直连 nsqd |

#### `queues` 队列配置

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `proto` | `string` | — | 固定为 `nsq` |
| `addr` | `string` | — | 格式：`nsq://<配置名>`，对应上面 `nsq` 下的配置名 |
| `delay_interval` | `int` | `2` | 保留（NSQ 延迟由 DeferredPublish 实现） |
| `deadletter_queue` | `string` | `""` | 死信 Topic 名，为空则禁用死信 |
| `group_name` | `string` | `AppName` | NSQ Channel 名，即消费者组 |

#### `servers.mqcserver.tasks`

| 字段 | 说明 |
|---|---|
| `queue` | 订阅的 NSQ Topic（队列名） |
| `service` | 路由到 MQC 处理器的服务路径 |

## 使用示例

### 应用入口（main.go）

```go
package main

import (
    "github.com/zhiyunliu/glue"
    _ "github.com/zhiyunliu/queue-nsq"
)

func main() {
    glue.NewApp().Start()
}
```

### 发送与延迟发送

```go
q := glue.Queue("default")

// 普通发送（Topic = demo.order）
err := q.Send(ctx, "demo.order", map[string]interface{}{"id": "1", "amount": 99})

// 延迟 30 秒投递（NSQ DeferredPublish）
err = q.DelaySend(ctx, "demo.delay", msg, 30)

// 批量发送
err = q.BatchSend(ctx, "demo.order", msg1, msg2, msg3)
```

### 消费端（MQC）

在 `servers.mqcserver.tasks` 中配置 `queue` 与 `service`，在代码中注册对应路径的 Handler；返回 `nil` 表示 Ack（Finish），返回 `error` 表示 Nack（Requeue），超过最大重试后若配置了 `deadletter_queue` 会写入死信 Topic。

## 运行示例

```bash
# 启动 NSQ（Docker）
docker run -d --name nsqd -p 4150:4150 -p 4151:4151 nsqio/nsq:latest
docker run -d --name nsqlookupd -p 4160:4160 -p 4161:4161 nsqio/nsq:latest nsqlookupd

# 运行示例
cd examples/nsq-basic
go run .
```

## 依赖

| 包 | 用途 |
|---|---|
| `github.com/nsqio/go-nsq` | NSQ 官方 Go 客户端 |
| `github.com/zhiyunliu/glue` | 框架与 queue 接口 |
| `github.com/zhiyunliu/golibs` | 工具（xnet 等） |
| `github.com/orcaman/concurrent-map/v2` | 并发 Map |

## 许可证

MIT License，见 [LICENSE](./LICENSE)。
