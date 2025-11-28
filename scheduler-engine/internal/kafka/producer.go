package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
)

type Producer struct {
	BootstrapServers string
	Topic            string
	KafkaProducer    *kafka.Producer
}

func (producer *Producer) createProducer() {
	slog.Info("Creating producer")
	var err error
	producer.KafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": producer.BootstrapServers,
	})

	if err != nil {
		panic(err)
	}
}

func (producer *Producer) Produce(topic, key, value string) {
	producer.KafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)
}

func (producer *Producer) Setup() {
	producer.createProducer()
	slog.Info("Producer created")
	go producer.Produce("test", "testkey", "testval")
	slog.Info("Message sent")
}
