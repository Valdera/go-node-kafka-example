package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const (
	PRIME_TOPIC        = "PRIME_TOPIC"
	PRIME_TOPIC_UPDATE = "PRIME_TOPIC_UPDATE"
)

func newKafkaWriter(kafkaUrl, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaUrl),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func newKafkaReader(kafkaUrl, groupId, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaUrl},
		Topic:   topic,
		GroupID: groupId,
	})
}

func main() {
	writer := newKafkaWriter("localhost:29092", PRIME_TOPIC)
	go consume()

	defer writer.Close()

	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprintf("you value is %d", i)),
		}

		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("we have produce message with key: ", key)
		}
	}
}

func consume() {
	reader := newKafkaReader("localhost:29092", "go", PRIME_TOPIC_UPDATE)
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(string(msg.Key), string(msg.Key))
	}
}
