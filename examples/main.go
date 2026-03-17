// Package main 演示使用 queue-nsq 包（基于 NSQ 的消息队列）。
//
// 运行方式（需先启动 NSQ）：
//
//	cd examples/nsq-basic
//	go run .
//
// NSQ 快速启动（Docker）：
//
//	docker run -d --name nsqd -p 4150:4150 -p 4151:4151 nsqio/nsq:latest
//	docker run -d --name nsqlookupd -p 4160:4160 -p 4161:4161 nsqio/nsq:latest nsqlookupd
//
// 配置说明见 config.json。
package main

import (
	"github.com/zhiyunliu/glue"
	_ "github.com/zhiyunliu/glue/contrib/metrics/prometheus"
	_ "github.com/zhiyunliu/queue-nsq" // 注册 NSQ 生产者/消费者实现
)

func main() {
	glue.NewApp(opts...).Start()
}
