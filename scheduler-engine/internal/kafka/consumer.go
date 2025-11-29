package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
)

type Consumer struct {
	BootstrapServers    string
	GroupId             string
	EnableAutoCommit    bool
	AutoOffsetReset     string
	SessionTimeoutMs    int
	HeartbeatIntervalMs int
	MaxPollIntervalMs   int
	FetchMinBytes       int
	FetchMaxWaitMs      int
	Topic               []string
	KafkaConsumer       *kafka.Consumer
}

func (consumer *Consumer) createConsumer() {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             consumer.BootstrapServers,
		"group.id":                      consumer.GroupId,
		"auto.offset.reset":             consumer.AutoOffsetReset,
		"session.timeout.ms":            consumer.SessionTimeoutMs,
		"heartbeat.interval.ms":         consumer.HeartbeatIntervalMs,
		"max.poll.interval.ms":          consumer.MaxPollIntervalMs,
		"fetch.min.bytes":               consumer.FetchMinBytes,
		"fetch.max.wait.ms":             consumer.FetchMaxWaitMs,
		"enable.auto.commit":            consumer.EnableAutoCommit,
		"partition.assignment.strategy": "Cooperative-Sticky",
	})

	if err != nil {
		panic(err)
	}
	consumer.KafkaConsumer = cons

}

func (consumer *Consumer) Subscribe() {
	err := consumer.KafkaConsumer.SubscribeTopics(consumer.Topic, nil)

	if err != nil {
		panic(err)
	}
}

func (consumer *Consumer) handleError(err error) {

}

func (consumer *Consumer) consume() {
	for {
		msg := consumer.KafkaConsumer.Poll(100)
		switch e := msg.(type) {
		case *kafka.Message:
			slog.Info("Received message: %v", string(e.Value))
			err := processMessage(e)
			if err != nil {
				slog.Error("Error processing message: %v", err)
				consumer.handleError(err)
			}
			commitMsg, err := consumer.KafkaConsumer.CommitMessage(e)
			if err != nil {
				slog.Error("Error committing message: %v", err)
			} else {
				slog.Info("Committed message: %v", commitMsg)
			}
		case kafka.Error:
			slog.Error("Error: %v", e)
			if e.IsFatal() {
				panic(e)
			}
		default:
			slog.Info("Ignored message")
		}
	}
}

func processMessage(msg *kafka.Message) error {

	slog.Info("Processing message key: %v", msg.Key, "value :%v", msg.Value, " received in TopicPartition: %v", msg.TopicPartition)

	return nil
}

func (consumer *Consumer) Close() {
	consumer.KafkaConsumer.Close()
}

func (consumer *Consumer) Setup() error {
	consumer.createConsumer()
	consumer.Subscribe()
	go consumer.consume()

	return nil
}
