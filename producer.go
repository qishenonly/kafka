package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Producer 是Kafka生产者的接口
type Producer interface {
	// Send 发送消息到指定主题
	Send(ctx context.Context, topic string, key, value []byte) error

	// SendMessage 发送消息到指定主题，支持更多选项
	SendMessage(ctx context.Context, msg *ProducerMessage) error

	// Close 关闭生产者
	Close() error
}

// ProducerMessage 表示要发送的消息
type ProducerMessage struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
	Timestamp time.Time
	Partition int32
}

// MessageHeader 表示消息头
type MessageHeader struct {
	Key   string
	Value []byte
}

// producer 实现Producer接口
type producer struct {
	sync.RWMutex
	saramaProducer sarama.SyncProducer
	closed         bool
}

// NewProducer 创建一个新的Kafka生产者
func NewProducer(opts ...Option) (Producer, error) {
	// 使用默认配置
	config := DefaultConfig()

	// 应用选项
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, ErrNoBrokers
	}

	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, WrapError("config", err)
	}

	// 确保同步生产者设置
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	saramaProducer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, WrapError("new_producer", err)
	}

	return &producer{
		saramaProducer: saramaProducer,
	}, nil
}

// NewProducerWithConfig 使用配置创建一个新的Kafka生产者
func NewProducerWithConfig(config *Config) (Producer, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if len(config.Brokers) == 0 {
		return nil, ErrNoBrokers
	}

	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, WrapError("config", err)
	}

	// 确保同步生产者设置
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	saramaProducer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, WrapError("new_producer", err)
	}

	return &producer{
		saramaProducer: saramaProducer,
	}, nil
}

// Send 发送消息到指定主题
func (p *producer) Send(ctx context.Context, topic string, key, value []byte) error {
	msg := &ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	}
	return p.SendMessage(ctx, msg)
}

// SendMessage 发送消息到指定主题，支持更多选项
func (p *producer) SendMessage(ctx context.Context, msg *ProducerMessage) error {
	p.RLock()
	if p.closed {
		p.RUnlock()
		return ErrProducerClosed
	}
	p.RUnlock()

	if msg.Topic == "" {
		return ErrTopicEmpty
	}

	saramaMsg := &sarama.ProducerMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
	}

	if msg.Key != nil {
		saramaMsg.Key = sarama.ByteEncoder(msg.Key)
	}

	if msg.Value != nil {
		saramaMsg.Value = sarama.ByteEncoder(msg.Value)
	}

	if !msg.Timestamp.IsZero() {
		saramaMsg.Timestamp = msg.Timestamp
	}

	if len(msg.Headers) > 0 {
		saramaMsg.Headers = make([]sarama.RecordHeader, len(msg.Headers))
		for i, h := range msg.Headers {
			saramaMsg.Headers[i] = sarama.RecordHeader{
				Key:   []byte(h.Key),
				Value: h.Value,
			}
		}
	}

	// 使用context控制超时
	done := make(chan error, 1)

	go func() {
		_, _, err := p.saramaProducer.SendMessage(saramaMsg)
		done <- err
	}()

	select {
	case <-ctx.Done():
		return WrapError("send", ctx.Err())
	case err := <-done:
		return WrapError("send", err)
	}
}

// Close 关闭生产者
func (p *producer) Close() error {
	p.Lock()
	defer p.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	err := p.saramaProducer.Close()
	if err != nil {
		return WrapError("close", err)
	}

	return nil
}
