package service

import (
	//"log"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TaskProcessor struct {
	KafkaProducer       *kafka.Producer
	PartitionAssignment *map[int32]chan *kafka.Message
	//MsgStore
	//
}

func New(kafkaProducer *kafka.Producer, partitionAssignment map[int32]chan *kafka.Message) *TaskProcessor {
	return &TaskProcessor{
		KafkaProducer: kafkaProducer,
	}
}

//func (taskProcessor *TaskProcessor) processTask(msg *kafka.Message) {
//	log.Println("Scheduling Task %v", msg.Value)
//	//load task from msg to internal memory
//}
