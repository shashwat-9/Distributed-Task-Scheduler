package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"scheduler-engine/internal/config"
)

type StatusProducer struct {
	Config         config.KafkaProducerConfig
	StatusProducer *kafka.Producer
}

func (producer *StatusProducer) Produce(topic, key, value string) {
	err := producer.StatusProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)
	if err != nil {
		return
	}
}

func NewProducer(producerConfig config.KafkaProducerConfig) (*StatusProducer, error) {
	slog.Info("Creating producer")
	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":          producerConfig.BootstrapServers,
		"client.id":                  producerConfig.ClientID,
		"acks":                       producerConfig.Acks,
		"delivery.timeout.ms":        *producerConfig.DeliveryTimeoutMs,
		"request.timeout.ms":         *producerConfig.RequestTimeoutMs,
		"linger.ms":                  *producerConfig.LingerMs,
		"queue.buffering.max.kbytes": *producerConfig.QueueBufferingMaxKbytes,
		//"compression.type":                      producerConfig.CompressionType,
		"batch.size":                            *producerConfig.BatchSize,
		"max.in.flight.requests.per.connection": *producerConfig.MaxInFlightRequestsPerConnection,
		"enable.idempotence":                    *producerConfig.EnableIdempotence,
	})

	producer := &StatusProducer{Config: producerConfig, StatusProducer: kafkaProducer}

	if err != nil {
		return nil, err
	}

	slog.Info("Producer created")

	return producer, nil
}
