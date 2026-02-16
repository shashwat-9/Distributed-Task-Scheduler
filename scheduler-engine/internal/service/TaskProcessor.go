package service

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TaskProcessor struct {
	KafkaProducer       *kafka.Producer
	PartitionAssignment *map[int32]chan *kafka.Message
}

func New(kafkaProducer *kafka.Producer, partitionAssignment map[int32]chan *kafka.Message) *TaskProcessor {
	return &TaskProcessor{
		KafkaProducer: kafkaProducer,
	}
}
