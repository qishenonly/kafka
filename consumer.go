package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ConsumerMessage 表示从Kafka接收的消息
type ConsumerMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
	Timestamp time.Time
}

// MessageHandler 是处理消息的回调函数
type MessageHandler func(ctx context.Context, msg *ConsumerMessage) error

// Consumer 是Kafka消费者的接口
type Consumer interface {
	// Subscribe 订阅主题并开始消费
	Subscribe(ctx context.Context, topics []string, handler MessageHandler) error

	// Close 关闭消费者
	Close() error
}

// consumer 实现Consumer接口
type consumer struct {
	sync.RWMutex
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	config        *Config
	closed        bool
}

// NewConsumer 创建一个新的Kafka消费者
func NewConsumer(opts ...Option) (Consumer, error) {
	// 使用默认配置
	config := DefaultConfig()

	// 应用选项
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, ErrNoBrokers
	}

	if config.Consumer.GroupID == "" {
		return nil, ErrNoGroupID
	}

	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, WrapError("config", err)
	}

	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, WrapError("new_client", err)
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(config.Consumer.GroupID, client)
	if err != nil {
		client.Close()
		return nil, WrapError("new_consumer_group", err)
	}

	return &consumer{
		client:        client,
		consumerGroup: consumerGroup,
		config:        config,
	}, nil
}

// NewConsumerWithConfig 使用配置创建一个新的Kafka消费者
func NewConsumerWithConfig(config *Config) (Consumer, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if len(config.Brokers) == 0 {
		return nil, ErrNoBrokers
	}

	if config.Consumer.GroupID == "" {
		return nil, ErrNoGroupID
	}

	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, WrapError("config", err)
	}

	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, WrapError("new_client", err)
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(config.Consumer.GroupID, client)
	if err != nil {
		client.Close()
		return nil, WrapError("new_consumer_group", err)
	}

	return &consumer{
		client:        client,
		consumerGroup: consumerGroup,
		config:        config,
	}, nil
}

// Subscribe 订阅主题并开始消费
func (c *consumer) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	c.RLock()
	if c.closed {
		c.RUnlock()
		return ErrConsumerClosed
	}
	c.RUnlock()

	if len(topics) == 0 {
		return ErrTopicEmpty
	}

	// 创建消费者处理器
	consumerHandler := &consumerGroupHandler{
		handler: handler,
	}

	// 启动消费循环
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := c.consumerGroup.Consume(ctx, topics, consumerHandler)
				if err != nil {
					if err == context.Canceled || err == context.DeadlineExceeded {
						return
					}
					// 记录错误但继续尝试
					// 这里可以添加日志记录
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()

	return nil
}

// Close 关闭消费者
func (c *consumer) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	err1 := c.consumerGroup.Close()
	err2 := c.client.Close()

	if err1 != nil {
		return WrapError("close_consumer_group", err1)
	}

	if err2 != nil {
		return WrapError("close_client", err2)
	}

	return nil
}

// consumerGroupHandler 实现sarama.ConsumerGroupHandler接口
type consumerGroupHandler struct {
	handler MessageHandler
}

// Setup 在消费者会话开始时调用
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 在消费者会话结束时调用
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理分配给消费者的消息
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 转换消息格式
		msg := &ConsumerMessage{
			Topic:     message.Topic,
			Partition: message.Partition,
			Offset:    message.Offset,
			Key:       message.Key,
			Value:     message.Value,
			Timestamp: message.Timestamp,
		}

		if len(message.Headers) > 0 {
			msg.Headers = make([]MessageHeader, len(message.Headers))
			for i, h := range message.Headers {
				msg.Headers[i] = MessageHeader{
					Key:   string(h.Key),
					Value: h.Value,
				}
			}
		}

		// 调用用户处理函数
		err := h.handler(session.Context(), msg)

		if err == nil {
			// 标记消息已处理
			session.MarkMessage(message, "")
		} else {
			// 这里可以添加错误处理逻辑
			// 例如重试或记录日志
		}
	}

	return nil
}
