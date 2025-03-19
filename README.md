# Kafka 工具包

这是一个基于 Sarama 的 Kafka 工具包，提供了简单易用的生产者和消费者 API，

## 特性

- 简单易用的 API
- 完善的配置选项
- 支持同步生产者
- 支持消费者组
- 内置指标收集
- 优雅的错误处理
- 支持 TLS 和 SASL 认证
- 支持消息头部
- 支持消息压缩

## 安装

```bash
go get github.com/qishenonly/kafka
```

## 快速开始

### 基本配置

```go
// 创建默认配置
config := kafka.DefaultConfig()

// 设置 Broker 地址
config.Brokers = []string{"localhost:9092"}

// 设置客户端 ID
config.ClientID = "my-application"

// 设置超时
config.DialTimeout = 30 * time.Second
config.ReadTimeout = 30 * time.Second
config.WriteTimeout = 30 * time.Second
```

### 生产者示例

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/qishenonly/kafka"
)

func main() {
    // 创建配置
    config := kafka.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}
    config.ClientID = "example-producer"
    
    // 创建生产者
    producer, err := kafka.NewProducer(config)
    if err != nil {
        log.Fatalf("Failed to create producer: %v", err)
    }
    defer producer.Close()
    
    // 简单发送消息
    ctx := context.Background()
    err = producer.Send(ctx, "example-topic", []byte("key"), []byte("Hello, Kafka!"))
    if err != nil {
        log.Fatalf("Failed to send message: %v", err)
    }
    
    // 使用更多选项发送消息
    msg := &kafka.ProducerMessage{
        Topic: "example-topic",
        Key:   []byte("custom-key"),
        Value: []byte("Custom message with headers"),
        Headers: []kafka.MessageHeader{
            {Key: "source", Value: []byte("example-app")},
            {Key: "timestamp", Value: []byte(time.Now().String())},
        },
        Timestamp: time.Now(),
    }
    
    err = producer.SendMessage(ctx, msg)
    if err != nil {
        log.Fatalf("Failed to send message: %v", err)
    }
    
    // 获取生产者指标
    metrics := producer.Metrics()
    log.Printf("Producer metrics: %+v", metrics)
}
```

### 消费者示例

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/qishenonly/kafka"
)

func main() {
    // 创建配置
    config := kafka.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}
    config.ClientID = "example-consumer"
    config.Consumer.GroupID = "example-group"
    
    // 创建消费者
    consumer, err := kafka.NewConsumer(config)
    if err != nil {
        log.Fatalf("Failed to create consumer: %v", err)
    }
    defer consumer.Close()
    
    // 创建上下文，用于控制消费者生命周期
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // 定义消息处理函数
    handler := func(ctx context.Context, msg *kafka.ConsumerMessage) error {
        log.Printf("Received message from topic %s [partition=%d] [offset=%d]",
            msg.Topic, msg.Partition, msg.Offset)
        log.Printf("Key: %s", string(msg.Key))
        log.Printf("Value: %s", string(msg.Value))
        
        // 处理消息头
        for _, header := range msg.Headers {
            log.Printf("Header %s: %s", header.Key, string(header.Value))
        }
        
        return nil
    }
    
    // 订阅主题
    topics := []string{"example-topic"}
    err = consumer.Subscribe(ctx, topics, handler)
    if err != nil {
        log.Fatalf("Failed to subscribe to topics: %v", err)
    }
    
    // 等待中断信号
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    
    log.Println("Shutting down consumer...")
}
```

## 高级配置

### TLS 配置

```go
// 加载 TLS 证书
cert, err := tls.LoadX509KeyPair("client.crt", "client.key")
if err != nil {
    log.Fatal(err)
}

config := kafka.DefaultConfig()
config.TLS = &tls.Config{
    Certificates:       []tls.Certificate{cert},
    InsecureSkipVerify: false,
}
```

### SASL 认证

```go
config := kafka.DefaultConfig()
config.SASL.Enable = true
config.SASL.User = "username"
config.SASL.Password = "password"
```

### 生产者配置

```go
config := kafka.DefaultConfig()

// 设置确认模式
config.Producer.RequireACK = true

// 设置重试
config.Producer.RetryMax = 5
config.Producer.RetryInterval = 200 * time.Millisecond

// 设置压缩
config.Producer.Compression = "snappy" // 可选: none, gzip, snappy, lz4, zstd
```

### 消费者配置

```go
config := kafka.DefaultConfig()

// 设置消费者组 ID
config.Consumer.GroupID = "my-consumer-group"

// 设置初始偏移量
config.Consumer.InitialOffset = "oldest" // 可选: newest, oldest

// 设置自动提交
config.Consumer.AutoCommitInterval = 5 * time.Second

// 设置会话超时
config.Consumer.SessionTimeout = 30 * time.Second

// 设置心跳间隔
config.Consumer.HeartbeatInterval = 10 * time.Second

// 设置重平衡策略
config.Consumer.BalanceStrategy = "sticky" // 可选: range, roundrobin, sticky
```

## 错误处理

该库提供了详细的错误类型，可以通过 `errors.Is` 和 `errors.As` 进行检查：

```go
if err := producer.Send(ctx, "topic", nil, []byte("message")); err != nil {
    switch {
    case errors.Is(err, kafka.ErrProducerClosed):
        log.Println("Producer is closed")
    case errors.Is(err, kafka.ErrTopicEmpty):
        log.Println("Topic name is empty")
    case errors.Is(err, kafka.ErrNoBrokers):
        log.Println("No brokers configured")
    default:
        log.Printf("Failed to send message: %v", err)
    }
}
```

## 指标收集

该库内置了指标收集功能，可以通过 `Metrics()` 方法获取生产者和消费者的运行指标：

```go
// 生产者指标
producerMetrics := producer.Metrics()
log.Printf("Messages sent: %d", producerMetrics["messages_sent"])
log.Printf("Messages failed: %d", producerMetrics["messages_failed"])
log.Printf("Bytes written: %d", producerMetrics["bytes_written"])
log.Printf("Last send duration: %s", producerMetrics["last_send_duration"])

// 消费者指标
consumerMetrics := consumer.Metrics()
log.Printf("Messages received: %d", consumerMetrics["messages_received"])
log.Printf("Bytes read: %d", consumerMetrics["bytes_read"])
log.Printf("Last poll duration: %s", consumerMetrics["last_poll_duration"])
```

## 完整示例

完整的示例代码可以在 [examples](./examples) 目录下找到：

- [producer_example](./examples/producer/main.go) - 生产者示例
- [consumer_example](./examples/consumer/main.go) - 消费者示例
- [tls_example](./examples/tls/main.go) - TLS 配置示例
- [sasl_example](./examples/sasl/main.go) - SASL 认证示例

## 贡献

欢迎提交 Issue 和 Pull Request。

## 许可证

Apache License 2.0

