package kafka

import (
	"crypto/tls"
	"time"

	"github.com/IBM/sarama"
)

// Config 定义了Kafka客户端的配置
type Config struct {
	// Brokers Kafka服务器地址列表
	Brokers []string

	// ClientID 客户端ID
	ClientID string

	// 超时设置
	DialTimeout  time.Duration // 连接超时时间
	WriteTimeout time.Duration // 写入超时时间
	ReadTimeout  time.Duration // 读取超时时间

	// TLS配置
	TLS *tls.Config

	// SASL配置
	SASL struct {
		Enable   bool
		User     string
		Password string
	}

	// Producer特定配置
	Producer struct {
		// 是否等待服务器确认
		RequireACK bool
		// 重试次数
		RetryMax int
		// 重试间隔
		RetryInterval time.Duration
		// 压缩类型: none, gzip, snappy, lz4, zstd
		Compression string
	}

	// Consumer特定配置
	Consumer struct {
		// 消费者组ID
		GroupID string
		// 初始偏移量: newest, oldest
		InitialOffset string
		// 自动提交间隔
		AutoCommitInterval time.Duration
		// 会话超时
		SessionTimeout time.Duration
		// 心跳间隔
		HeartbeatInterval time.Duration
		// 重平衡策略: range, roundrobin, sticky
		BalanceStrategy string
	}
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	cfg := &Config{
		ClientID:     "bytedance-kafka-client",
		DialTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}

	// 生产者默认配置
	cfg.Producer.RequireACK = true
	cfg.Producer.RetryMax = 3
	cfg.Producer.RetryInterval = 100 * time.Millisecond
	cfg.Producer.Compression = "snappy"

	// 消费者默认配置
	cfg.Consumer.InitialOffset = "newest"
	cfg.Consumer.AutoCommitInterval = 1 * time.Second
	cfg.Consumer.SessionTimeout = 10 * time.Second
	cfg.Consumer.HeartbeatInterval = 3 * time.Second
	cfg.Consumer.BalanceStrategy = "sticky"

	return cfg
}

// ToSaramaConfig 将自定义配置转换为Sarama配置
func (c *Config) ToSaramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()

	// 基本配置
	config.ClientID = c.ClientID
	config.Net.DialTimeout = c.DialTimeout
	config.Net.WriteTimeout = c.WriteTimeout
	config.Net.ReadTimeout = c.ReadTimeout

	// TLS配置
	if c.TLS != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = c.TLS
	}

	// SASL配置
	if c.SASL.Enable {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.SASL.User
		config.Net.SASL.Password = c.SASL.Password
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	// 生产者配置
	if c.Producer.RequireACK {
		config.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		config.Producer.RequiredAcks = sarama.NoResponse
	}

	config.Producer.Retry.Max = c.Producer.RetryMax
	config.Producer.Retry.Backoff = c.Producer.RetryInterval

	// 设置压缩类型
	switch c.Producer.Compression {
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	// 消费者配置
	switch c.Consumer.InitialOffset {
	case "oldest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = c.Consumer.AutoCommitInterval
	config.Consumer.Group.Session.Timeout = c.Consumer.SessionTimeout
	config.Consumer.Group.Heartbeat.Interval = c.Consumer.HeartbeatInterval

	// 设置重平衡策略
	switch c.Consumer.BalanceStrategy {
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	default:
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	}

	// 版本设置为最新版本
	config.Version = sarama.V2_8_0_0

	return config, nil
}
