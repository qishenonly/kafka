package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/qishenonly/kafka"
)

func main() {
	// 创建生产者
	producer, err := kafka.NewProducer(
		kafka.WithBrokers("localhost:9092"),
		kafka.WithClientID("example-producer"),
		kafka.WithProducerRequireACK(true),
		kafka.WithProducerRetryMax(3),
		kafka.WithProducerRetryInterval(100*time.Millisecond),
		kafka.WithProducerCompression("snappy"),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// 创建上下文
	ctx := context.Background()

	// 1. 简单发送消息
	err = producer.Send(ctx, "example-topic", []byte("key-1"), []byte("Simple message"))
	if err != nil {
		log.Printf("Failed to send simple message: %v", err)
	}

	// 2. 发送带头部的消息
	msg := &kafka.ProducerMessage{
		Topic: "example-topic",
		Key:   []byte("key-2"),
		Value: []byte("Message with headers"),
		Headers: []kafka.MessageHeader{
			{Key: "source", Value: []byte("example-producer")},
			{Key: "timestamp", Value: []byte(time.Now().String())},
		},
		Timestamp: time.Now(),
	}

	err = producer.SendMessage(ctx, msg)
	if err != nil {
		log.Printf("Failed to send message with headers: %v", err)
	}

	// 3. 批量发送消息
	for i := 0; i < 20; i++ {
		msg := &kafka.ProducerMessage{
			Topic: "example-topic",
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("Batch message %d", i)),
			Headers: []kafka.MessageHeader{
				{Key: "batch_id", Value: []byte("batch-1")},
				{Key: "index", Value: []byte(fmt.Sprintf("%d", i))},
			},
		}

		err = producer.SendMessage(ctx, msg)
		if err != nil {
			log.Printf("Failed to send batch message %d: %v", i, err)
			continue
		}
	}

	fmt.Println("send success")

}
