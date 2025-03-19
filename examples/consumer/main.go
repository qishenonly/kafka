package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/qishenonly/kafka"
)

func main() {
	// 创建消费者
	consumer, err := kafka.NewConsumer(
		kafka.WithBrokers("localhost:9092"),
		kafka.WithClientID("example-consumer"),
		kafka.WithConsumerGroupID("example-group"),
		kafka.WithConsumerInitialOffset("oldest"),
		kafka.WithConsumerAutoCommitInterval(1*time.Second),
		kafka.WithConsumerSessionTimeout(30*time.Second),
		kafka.WithConsumerHeartbeatInterval(3*time.Second),
		kafka.WithConsumerBalanceStrategy("sticky"),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 计数器
	var messageCount int64

	// 定义消息处理函数
	handler := func(ctx context.Context, msg *kafka.ConsumerMessage) error {
		// 增加计数
		atomic.AddInt64(&messageCount, 1)

		// 打印消息详情
		log.Printf("Received message:")
		log.Printf("Topic: %s, Partition: %d, Offset: %d",
			msg.Topic, msg.Partition, msg.Offset)
		log.Printf("Key: %s", string(msg.Key))
		log.Printf("Value: %s", string(msg.Value))

		// 打印消息头
		if len(msg.Headers) > 0 {
			log.Printf("Headers:")
			for _, header := range msg.Headers {
				log.Printf("  %s: %s", header.Key, string(header.Value))
			}
		}

		// 模拟处理时间
		time.Sleep(100 * time.Millisecond)

		return nil
	}

	// 订阅主题
	topics := []string{"example-topic"}
	err = consumer.Subscribe(ctx, topics, handler)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
	}

	// 启动指标打印协程
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Printf("Total messages processed: %d", atomic.LoadInt64(&messageCount))
			}
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down consumer...")
}
