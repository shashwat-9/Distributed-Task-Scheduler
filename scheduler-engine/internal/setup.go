package internal

import (
	"log/slog"
	"scheduler-engine/internal/kafka"
)

func Init() error {
	bootStrapServers := "localhost:9092"
	groupId := "abc"
	topic := "test"
	cons := kafka.Consumer{
		BootstrapServers: bootStrapServers,
		GroupId:          groupId,
		Topic:            topic,
		KafkaConsumer:    nil,
	}
	cons.Setup()
	slog.Info("Consumer setup")
	prod := kafka.Producer{
		BootstrapServers: bootStrapServers,
		Topic:            topic,
		KafkaProducer:    nil,
	}
	prod.Setup()
	return nil
}
