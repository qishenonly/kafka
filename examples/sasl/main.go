package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qishenonly/kafka"
)

func main() {
	// 创建生产者
	producer, err := kafka.NewProducer(
		kafka.WithBrokers("localhost:9092"),
		kafka.WithClientID("sasl-example"),
		kafka.WithSASL(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_PASSWORD")),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// 创建消费者
	consumer, err := kafka.NewConsumer(
		kafka.WithBrokers("localhost:9092"),
		kafka.WithClientID("sasl-consumer"),
		kafka.WithConsumerGroupID("sasl-consumer-group"),
		kafka.WithSASL(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_PASSWORD")),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 发送消息
	msg := &kafka.ProducerMessage{
		Topic: "secure-topic",
		Key:   []byte("sasl-key"),
		Value: []byte("Message with SASL authentication"),
		Headers: []kafka.MessageHeader{
			{Key: "auth", Value: []byte("sasl")},
			{Key: "timestamp", Value: []byte(time.Now().String())},
		},
	}

	err = producer.SendMessage(ctx, msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Println("Successfully sent message with SASL authentication")

	// 消费消息
	handler := func(ctx context.Context, msg *kafka.ConsumerMessage) error {
		log.Printf("Received message with SASL authentication:")
		log.Printf("Topic: %s, Key: %s, Value: %s",
			msg.Topic, string(msg.Key), string(msg.Value))
		return nil
	}

	err = consumer.Subscribe(ctx, []string{"secure-topic"}, handler)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down SASL example...")
}
