package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/util"
)

var transactionalProducerLogger *zap.Logger

type TransactionalProducer struct {
	producer *kafka.Producer
	config   config.KafkaProducerConfig
}

func (transactionalProducer TransactionalProducer) Produce(topic, key, value string, offset int) {
	transactionalProducerLogger.Info("Producing message")
	transactionalProducer.producer.BeginTransaction()
	transactionalProducer.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(value),
	}, nil)

	//offsets := map[ck.TopicPartition]ck.Offset{
	//	e.TopicPartition: e.TopicPartition.Offset + 1,
	//}

	//transactionalProducer.producer.SendOffsetsToTransaction(offsets, consumer.string());

	transactionalProducer.producer.CommitTransaction(nil)

}

func NewTransactionalProducer(transactionalProducerConfig config.KafkaProducerConfig) (*TransactionalProducer, error) {
	transactionalProducerLogger = util.GetLogger("logs/kafkaTransaction.log", 10, 5, 28)
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
		return &TransactionalProducer{}, err
	}

	transactionalProducerLogger.Info("initiating transactional producer")
	err = producer.InitTransactions(nil)

	if err != nil {
		return &TransactionalProducer{}, err
	}

	return &TransactionalProducer{producer: producer, config: transactionalProducerConfig}, nil
}
