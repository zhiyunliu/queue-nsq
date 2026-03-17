package nsq

import (
	"context"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/zhiyunliu/glue/config"
	"github.com/zhiyunliu/glue/queue"
)

var _ queue.IMQP = (*Producer)(nil)

// Producer publishes messages to NSQ topics.
type Producer struct {
	producer *nsq.Producer
	opts     *ProducerOptions
	onceLock sync.Once
}

// NewProducer creates a new NSQ producer using the provided glue config.
func NewProducer(cfg config.Config, opts ...queue.Option) (p *Producer, err error) {
	_, serverCfg, err := getNsqConfig(cfg)
	if err != nil {
		return nil, err
	}
	addr := firstNsqdAddr(serverCfg)
	nsqCfg := nsq.NewConfig()
	prod, err := nsq.NewProducer(addr, nsqCfg)
	if err != nil {
		return nil, err
	}
	p = &Producer{
		producer: prod,
		opts:    &ProducerOptions{DelayInterval: 2},
	}
	_ = cfg.ScanTo(p.opts)
	return p, nil
}

func (p *Producer) Name() string { return Proto }

func (p *Producer) Push(ctx context.Context, key string, msg queue.Message) error {
	data, err := msg.MarshalBinary()
	if err != nil {
		return err
	}
	return p.producer.Publish(key, data)
}

func (p *Producer) DelayPush(ctx context.Context, key string, msg queue.Message, delaySeconds int64) error {
	if delaySeconds <= 0 {
		return p.Push(ctx, key, msg)
	}
	data, err := msg.MarshalBinary()
	if err != nil {
		return err
	}
	return p.producer.DeferredPublish(key, time.Duration(delaySeconds)*time.Second, data)
}

// BatchPush publishes multiple messages to the same topic in sequence.
func (p *Producer) BatchPush(ctx context.Context, key string, msgList ...queue.Message) error {
	for _, msg := range msgList {
		if err := p.Push(ctx, key, msg); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) Close() error {
	p.onceLock.Do(func() { p.producer.Stop() })
	return nil
}

type producerResolver struct{}

func (s *producerResolver) Name() string { return Proto }
func (s *producerResolver) Resolve(cfg config.Config, opts ...queue.Option) (queue.IMQP, error) {
	return NewProducer(cfg, opts...)
}

func init() {
	queue.RegisterProducer(&producerResolver{})
}
