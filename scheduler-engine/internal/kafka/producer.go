package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
)

type Producer struct {
	BootstrapServers                 string
	ClientID                         string
	Acks                             string
	DeliveryTimeoutMs                int
	RequestTimeoutMs                 int
	LingerMs                         int
	QueueBufferingMaxKbytes          int
	CompressionType                  string
	BatchSize                        int
	MaxInFlightRequestsPerConnection int
	MaxRequestSize                   int
	EnableIdempotence                bool
	KafkaProducer                    *kafka.Producer
}

func (producer *Producer) createProducer() {
	slog.Info("Creating producer")
	var err error
	producer.KafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     producer.BootstrapServers,
		"client.id":                             producer.ClientID,
		"acks":                                  producer.Acks,
		"delivery.timeout.ms":                   producer.DeliveryTimeoutMs,
		"request.timeout.ms":                    producer.RequestTimeoutMs,
		"linger.ms":                             producer.LingerMs,
		"queue.buffering.max.kbytes":            producer.QueueBufferingMaxKbytes,
		"compression.type":                      producer.CompressionType,
		"batch.size":                            producer.BatchSize,
		"max.in.flight.requests.per.connection": producer.MaxInFlightRequestsPerConnection,
		"max.request.size":                      producer.MaxRequestSize,
		"enable.idempotence":                    producer.EnableIdempotence,
	})

	if err != nil {
		panic(err)
	}
}

func (producer *Producer) Produce(topic, key, value string) {
	err := producer.KafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)
	if err != nil {
		return
	}
}

func (producer *Producer) Setup() {
	producer.createProducer()
	slog.Info("Producer created")
	go producer.Produce("test", "testkey", "testval")
	slog.Info("Message sent")
}
