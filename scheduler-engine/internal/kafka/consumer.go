package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
)

type Consumer struct {
	BootstrapServers string
	GroupId          string
	Topic            string
	KafkaConsumer    *kafka.Consumer
}

func (consumer *Consumer) createConsumer() {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": consumer.BootstrapServers,
		"group.id":          consumer.GroupId,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	consumer.KafkaConsumer = cons

}

func (consumer *Consumer) Subscribe() {
	consumer.KafkaConsumer.SubscribeTopics([]string{consumer.Topic}, nil)
}

func (consumer *Consumer) consume() {
	for {
		msg, err := consumer.KafkaConsumer.ReadMessage(-1)
		if err == nil {
			slog.Info("Message on", msg.TopicPartition, " ", string(msg.Value))
		}
	}
}

func (consumer *Consumer) Setup() error {
	consumer.createConsumer()
	consumer.Subscribe()
	go consumer.consume()

	return nil
}
