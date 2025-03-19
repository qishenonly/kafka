package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"time"

	"github.com/qishenonly/kafka"
)

func main() {
	// 加载客户端证书
	cert, err := tls.LoadX509KeyPair("client.crt", "client.key")
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}

	// 加载CA证书
	caCert, err := os.ReadFile("ca.crt")
	if err != nil {
		log.Fatalf("Failed to load CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// 创建TLS配置
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}

	// 创建Kafka配置
	config := kafka.DefaultConfig()
	config.Brokers = []string{"localhost:9093"} // TLS通常使用9093端口
	config.ClientID = "tls-example"
	config.TLS = tlsConfig

	// 创建生产者
	producer, err := kafka.NewProducer(
		kafka.WithBrokers("localhost:9093"),
		kafka.WithClientID("tls-example"),
		kafka.WithTLS(tlsConfig),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// 发送测试消息
	ctx := context.Background()
	msg := &kafka.ProducerMessage{
		Topic: "secure-topic",
		Key:   []byte("tls-key"),
		Value: []byte("Message sent over TLS"),
		Headers: []kafka.MessageHeader{
			{Key: "secure", Value: []byte("true")},
			{Key: "timestamp", Value: []byte(time.Now().String())},
		},
	}

	err = producer.SendMessage(ctx, msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Println("Successfully sent message over TLS connection")
}
