package kafka

import (
	"crypto/tls"
	"time"
)

// Option 是配置函数类型，用于设置配置选项
type Option func(*Config)

// WithBrokers 设置 Kafka 服务器地址
func WithBrokers(brokers ...string) Option {
	return func(c *Config) {
		c.Brokers = brokers
	}
}

// WithClientID 设置客户端 ID
func WithClientID(clientID string) Option {
	return func(c *Config) {
		c.ClientID = clientID
	}
}

// WithDialTimeout 设置连接超时时间
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.DialTimeout = timeout
	}
}

// WithReadTimeout 设置读取超时时间
func WithReadTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.ReadTimeout = timeout
	}
}

// WithWriteTimeout 设置写入超时时间
func WithWriteTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.WriteTimeout = timeout
	}
}

// WithTLS 设置 TLS 配置
func WithTLS(tlsConfig *tls.Config) Option {
	return func(c *Config) {
		c.TLS = tlsConfig
	}
}

// WithSASL 设置 SASL 认证
func WithSASL(user, password string) Option {
	return func(c *Config) {
		c.SASL.Enable = true
		c.SASL.User = user
		c.SASL.Password = password
	}
}

// Producer 相关选项

// WithProducerRequireACK 设置生产者确认模式
func WithProducerRequireACK(requireACK bool) Option {
	return func(c *Config) {
		c.Producer.RequireACK = requireACK
	}
}

// WithProducerRetryMax 设置生产者最大重试次数
func WithProducerRetryMax(retryMax int) Option {
	return func(c *Config) {
		c.Producer.RetryMax = retryMax
	}
}

// WithProducerRetryInterval 设置生产者重试间隔
func WithProducerRetryInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.Producer.RetryInterval = interval
	}
}

// WithProducerCompression 设置生产者压缩类型
func WithProducerCompression(compression string) Option {
	return func(c *Config) {
		c.Producer.Compression = compression
	}
}

// Consumer 相关选项

// WithConsumerGroupID 设置消费者组 ID
func WithConsumerGroupID(groupID string) Option {
	return func(c *Config) {
		c.Consumer.GroupID = groupID
	}
}

// WithConsumerInitialOffset 设置消费者初始偏移量
func WithConsumerInitialOffset(offset string) Option {
	return func(c *Config) {
		c.Consumer.InitialOffset = offset
	}
}

// WithConsumerAutoCommitInterval 设置消费者自动提交间隔
func WithConsumerAutoCommitInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.Consumer.AutoCommitInterval = interval
	}
}

// WithConsumerSessionTimeout 设置消费者会话超时
func WithConsumerSessionTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Consumer.SessionTimeout = timeout
	}
}

// WithConsumerHeartbeatInterval 设置消费者心跳间隔
func WithConsumerHeartbeatInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.Consumer.HeartbeatInterval = interval
	}
}

// WithConsumerBalanceStrategy 设置消费者重平衡策略
func WithConsumerBalanceStrategy(strategy string) Option {
	return func(c *Config) {
		c.Consumer.BalanceStrategy = strategy
	}
}
