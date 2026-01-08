package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"scheduler-engine/internal/config"
)

type Producer struct {
	Config        config.KafkaProducerConfig
	QueryProducer *kafka.Producer
}

func (producer *Producer) Produce(topic, key, value string) {
	err := producer.QueryProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)
	if err != nil {
		return
	}
}

//func (producer *Producer) Setup() {
//	go producer.Produce("test", "testkey", "testval")
//	slog.Info("Message sent")
//}

func CreateProducer(producerConfig config.KafkaProducerConfig) (*Producer, error) {
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
		//"max.request.size":                      *producerConfig.MaxRequestSize,
		"enable.idempotence": *producerConfig.EnableIdempotence,
	})

	producer := &Producer{Config: producerConfig, QueryProducer: kafkaProducer}

	if err != nil {
		return nil, err
	}

	slog.Info("Producer created")

	return producer, nil
}
