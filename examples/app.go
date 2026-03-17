package main

import (
	"context"
	"fmt"
	"time"

	"github.com/zhiyunliu/glue"
	gluectx "github.com/zhiyunliu/glue/context"
	"github.com/zhiyunliu/glue/global"
	"github.com/zhiyunliu/glue/server/mqc"
	"github.com/zhiyunliu/glue/transport"
)

var opts []glue.Option

func init() {
	global.AppName = "nsq-basic"

	// 注册服务器
	opts = append(opts, glue.Server(buildMQCServer()))

	// 服务完全启动后运行生产者演示（异步，避免阻塞启动流程）
	opts = append(opts, glue.StartedHook(func(ctx context.Context) error {
		go runProducerDemo(ctx)
		return nil
	}))
}

// buildMQCServer 创建并配置 MQC 消费者服务器。
// 路由路径需与 config.json 的 servers.mqcserver.tasks[].service 字段保持一致。
func buildMQCServer() transport.Server {
	srv := mqc.New("mqcserver")

	// 处理普通订单消息（demo.order）
	srv.Handle("/demo/order", func(ctx gluectx.Context) interface{} {
		var order map[string]interface{}
		if err := ctx.Request().Body().ScanTo(&order); err != nil {
			ctx.Log().Errorf("解析消息体失败: %v", err)
			return err // 返回 error → 框架自动 Nack，触发重试
		}
		fmt.Printf("[消费者] 收到订单消息: %+v\n", order)
		return nil // 返回 nil → 框架自动 Ack，标记消费成功
	})

	// 处理延迟消息（demo.delay）
	srv.Handle("/demo/delay", func(ctx gluectx.Context) interface{} {
		var msg map[string]interface{}
		if err := ctx.Request().Body().ScanTo(&msg); err != nil {
			ctx.Log().Errorf("解析延迟消息失败: %v", err)
			return err
		}
		fmt.Printf("[消费者] 收到延迟消息（延迟后投递）: %+v\n", msg)
		return nil
	})

	// 处理死信队列消息（demo.deadletter）
	// 超过最大重试次数后，框架将消息转发至此队列
	srv.Handle("/demo/deadletter", func(ctx gluectx.Context) interface{} {
		fmt.Printf("[死信队列] 消息处理失败已转入死信: %s\n",
			ctx.Request().Body().Bytes())
		return nil
	})

	return srv
}

// runProducerDemo 演示三种发送方式：普通发送、批量发送、延迟发送。
func runProducerDemo(ctx context.Context) {
	// 稍等片刻，确保消费者订阅已就绪
	time.Sleep(2 * time.Second)

	q := glue.Queue("default")

	fmt.Println("──────────────────────────────────────")
	fmt.Println("【生产者演示开始】")

	// ── 1. 普通发送 ──────────────────────────────
	err := q.Send(ctx, "demo.order", map[string]interface{}{
		"order_id": "A001",
		"product":  "商品A",
		"amount":   199.0,
		"time":     time.Now().Format(time.RFC3339),
	})
	if err != nil {
		fmt.Printf("[生产者] 普通发送失败: %v\n", err)
	} else {
		fmt.Println("[生产者] 普通消息已发送 → demo.order")
	}

	// ── 2. 批量发送 ──────────────────────────────
	err = q.BatchSend(ctx, "demo.order",
		map[string]interface{}{"order_id": "B001", "product": "商品B1", "amount": 50.0},
		map[string]interface{}{"order_id": "B002", "product": "商品B2", "amount": 80.0},
		map[string]interface{}{"order_id": "B003", "product": "商品B3", "amount": 120.0},
	)
	if err != nil {
		fmt.Printf("[生产者] 批量发送失败: %v\n", err)
	} else {
		fmt.Println("[生产者] 批量消息已发送（3 条）→ demo.order")
	}

	// ── 3. 延迟发送（5 秒后投递） ────────────────
	// NSQ 原生支持 DeferredPublish，无需额外延迟队列
	err = q.DelaySend(ctx, "demo.delay", map[string]interface{}{
		"order_id": "C001",
		"note":     "5 秒延迟投递",
		"sent_at":  time.Now().Format(time.RFC3339),
	}, 5)
	if err != nil {
		fmt.Printf("[生产者] 延迟发送失败: %v\n", err)
	} else {
		fmt.Println("[生产者] 延迟消息已投递，将在 5 秒后到达 → demo.delay")
	}

	fmt.Println("【生产者演示结束，等待消息消费...】")
	fmt.Println("──────────────────────────────────────")
}
