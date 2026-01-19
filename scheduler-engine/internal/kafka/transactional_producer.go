package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"scheduler-engine/internal/config"
)

type TransactionalProducer struct {
	producer *kafka.Producer
	config   config.KafkaProducerConfig
}

func (transactionalProducer TransactionalProducer) Produce(topic, key, value string) {
	slog.Info("Producing message")
	transactionalProducer.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)
}

func NewTransactionalProducer(transactionalProducerConfig config.KafkaProducerConfig) (TransactionalProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     transactionalProducerConfig.BootstrapServers,
		"client.id":                             transactionalProducerConfig.ClientID,
		"acks":                                  transactionalProducerConfig.Acks,
		"delivery.timeout.ms":                   *transactionalProducerConfig.DeliveryTimeoutMs,
		"request.timeout.ms":                    *transactionalProducerConfig.RequestTimeoutMs,
		"linger.ms":                             *transactionalProducerConfig.LingerMs,
		"queue.buffering.max.kbytes":            *transactionalProducerConfig.QueueBufferingMaxKbytes,
		"batch.size":                            *transactionalProducerConfig.BatchSize,
		"max.in.flight.requests.per.connection": *transactionalProducerConfig.MaxInFlightRequestsPerConnection,
		"enable.idempotence":                    *transactionalProducerConfig.EnableIdempotence,
	})

	if err != nil {
		return TransactionalProducer{}, err
	}
	return TransactionalProducer{producer: producer, config: transactionalProducerConfig}, nil
}
