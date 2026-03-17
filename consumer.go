package nsq

import (
	"fmt"
	"strings"
	"sync"

	"github.com/nsqio/go-nsq"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/zhiyunliu/glue/config"
	"github.com/zhiyunliu/glue/global"
	"github.com/zhiyunliu/glue/queue"
)

var _ queue.IMQC = (*Consumer)(nil)

// QueueItem holds the subscription metadata and callback for a single NSQ topic.
type QueueItem struct {
	queue.TaskInfo
	callback   queue.ConsumeCallback
	nsqCons    *nsq.Consumer
}

// Consumer subscribes to NSQ topics and dispatches messages to registered callbacks.
type Consumer struct {
	proto             string
	configName        string
	groupName         string
	deadLetterQueue   string
	enableDeadLetter  bool
	deadLetterProducer *nsq.Producer
	queues            cmap.ConcurrentMap[string, *QueueItem]
	closeCh           chan struct{}
	once              sync.Once
	wg                sync.WaitGroup
	config            config.Config
	lookupdAddrs      []string
	nsqdAddr          string
}

// NewConsumer constructs a Consumer; Connect must be called before Start.
func NewConsumer(proto, configName string, cfg config.Config) (*Consumer, error) {
	return &Consumer{
		proto:      proto,
		configName: configName,
		config:     cfg,
		closeCh:    make(chan struct{}),
		queues:     cmap.New[*QueueItem](),
	}, nil
}

func (c *Consumer) ServerURL() string {
	return fmt.Sprintf("%s://%s-%s", c.proto, global.LocalIp, c.configName)
}

func (c *Consumer) Connect() (err error) {
	_, serverCfg, err := getNsqConfig(c.config)
	if err != nil {
		return err
	}
	c.nsqdAddr = firstNsqdAddr(serverCfg)
	c.lookupdAddrs = lookupdAddrs(serverCfg)
	ccfg := &ConsumerConfig{}
	_ = c.config.ScanTo(ccfg)
	c.deadLetterQueue = ccfg.DeadLetterQueue
	c.enableDeadLetter = len(c.deadLetterQueue) > 0
	c.groupName = ccfg.GroupName
	if c.groupName == "" {
		c.groupName = global.AppName
	}
	if c.groupName == "" {
		c.groupName = "glue-queue"
	}
	if c.enableDeadLetter {
		c.deadLetterProducer, err = nsq.NewProducer(c.nsqdAddr, nsq.NewConfig())
		if err != nil {
			return fmt.Errorf("nsq dead letter producer: %w", err)
		}
	}
	return nil
}

func (c *Consumer) Consume(task queue.TaskInfo, callback queue.ConsumeCallback) error {
	queueName := task.GetQueue()
	if strings.EqualFold(queueName, "") {
		return fmt.Errorf("队列名称不能为空")
	}
	if callback == nil {
		return fmt.Errorf("queue:%s, 回调函数不能为nil", queueName)
	}
	item := &QueueItem{TaskInfo: task, callback: callback}
	c.queues.SetIfAbsent(queueName, item)
	return nil
}

func (c *Consumer) Unconsume(queueName string) {
	if item, ok := c.queues.Get(queueName); ok {
		if item.nsqCons != nil {
			item.nsqCons.Stop()
		}
	}
	c.queues.Remove(queueName)
}

func (c *Consumer) Start() error {
	for entry := range c.queues.IterBuffered() {
		item := entry.Val
		topic := item.GetQueue()
		concurrency := item.GetConcurrency()
		if concurrency <= 0 {
			concurrency = queue.DefaultMaxQueueLen
		}
		nsqCfg := nsq.NewConfig()
		nsqCfg.MaxInFlight = concurrency
		cons, err := nsq.NewConsumer(topic, c.groupName, nsqCfg)
		if err != nil {
			return fmt.Errorf("nsq NewConsumer %s: %w", topic, err)
		}
		item.nsqCons = cons
		cons.AddHandler(c.buildHandler(item))
		if len(c.lookupdAddrs) > 0 {
			for _, addr := range c.lookupdAddrs {
				addr = strings.TrimPrefix(strings.TrimPrefix(addr, "http://"), "https://")
				if err := cons.ConnectToNSQLookupd(addr); err != nil {
					cons.Stop()
					return fmt.Errorf("nsq ConnectToNSQLookupd %s: %w", addr, err)
				}
			}
		} else {
			if err := cons.ConnectToNSQD(c.nsqdAddr); err != nil {
				cons.Stop()
				return fmt.Errorf("nsq ConnectToNSQD %s: %w", c.nsqdAddr, err)
			}
		}
		c.wg.Add(1)
		go func(qi *QueueItem) {
			defer c.wg.Done()
			<-qi.nsqCons.StopChan
		}(item)
	}
	return nil
}

func (c *Consumer) buildHandler(item *QueueItem) nsq.Handler {
	return nsq.HandlerFunc(func(nmsg *nsq.Message) error {
		nMsg := &nsqMessage{msg: nmsg}
		item.callback(nMsg)
		if nMsg.err != nil && c.enableDeadLetter &&
			nMsg.RetryCount() >= queue.MaxRetrtCount &&
			!strings.EqualFold(item.GetQueue(), c.deadLetterQueue) {
			c.writeToDeadLetter(item.GetQueue(), nmsg.Body)
			return nil // FIN
		}
		if nMsg.err != nil {
			return nMsg.err // REQ
		}
		return nil // FIN
	})
}

func (c *Consumer) writeToDeadLetter(topic string, data []byte) {
	if !c.enableDeadLetter || c.deadLetterProducer == nil {
		return
	}
	_ = c.deadLetterProducer.Publish(c.deadLetterQueue, data)
}

func (c *Consumer) Close() error {
	c.once.Do(func() { close(c.closeCh) })
	for entry := range c.queues.IterBuffered() {
		if entry.Val.nsqCons != nil {
			entry.Val.nsqCons.Stop()
		}
	}
	c.wg.Wait()
	if c.deadLetterProducer != nil {
		c.deadLetterProducer.Stop()
	}
	return nil
}

type consumeResolver struct{}

func (s *consumeResolver) Name() string { return Proto }
func (s *consumeResolver) Resolve(configName string, cfg config.Config) (queue.IMQC, error) {
	return NewConsumer(s.Name(), configName, cfg)
}

func init() {
	queue.RegisterConsumer(&consumeResolver{})
}
