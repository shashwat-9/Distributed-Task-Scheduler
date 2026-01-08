package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"scheduler-engine/internal/config"
	"sync"
)

type PartitionAssignment struct {
	TaskChannel chan *kafka.Message
	Completion  *sync.WaitGroup
}

type Consumer struct {
	Config                 config.KafkaConsumerConfig
	KafkaConsumer          *kafka.Consumer
	run                    bool
	PartitionAssignmentMap map[int32]PartitionAssignment
}

func (consumer *Consumer) Subscribe() error {
	err := consumer.KafkaConsumer.SubscribeTopics(consumer.Config.Topic, nil)

	if err != nil {
		return err
	}

	return nil
}

func (consumer *Consumer) handleError(err error) {
	//send to DLQ
	slog.Error("Error: %v", err)
}

func (consumer *Consumer) consume() {
	for consumer.run {
		msg := consumer.KafkaConsumer.Poll(100)
		switch e := msg.(type) {
		case *kafka.Message:
			slog.Info("Received message: ", string(e.Value))
			if consumer.PartitionAssignmentMap[e.TopicPartition.Partition].TaskChannel == nil {
				slog.Info("Oh GOd")
			}
			slog.Info("Sending message to channel for partition %v", e.TopicPartition.Partition)
			consumer.PartitionAssignmentMap[e.TopicPartition.Partition].TaskChannel <- e
			slog.Info("Sent message to channel for partition %v", e.TopicPartition.Partition)
		case kafka.Error:
			slog.Error("Error: %v", e)
			if e.IsFatal() {
				//panic(e)
			}
		case kafka.AssignedPartitions:
			slog.Info("Received assignment")
			for i := 0; i < len(e.Partitions); i++ {
				slog.Info("Assigned partition: %v", e.Partitions[i].Partition)
				consumer.PartitionAssignmentMap[e.Partitions[i].Partition] = PartitionAssignment{
					TaskChannel: make(chan *kafka.Message, *consumer.Config.WorkerPoolSize),
					Completion:  &sync.WaitGroup{},
				}
				consumer.PartitionAssignmentMap[e.Partitions[i].Partition].Completion.Add(1)
				go func(partitionMap map[int32]PartitionAssignment, i int32) {
					for val := range partitionMap[i].TaskChannel {
						slog.Info("Processing message: %v", val)
						//process message
					}
					partitionMap[i].Completion.Done()
				}(consumer.PartitionAssignmentMap, e.Partitions[i].Partition)
			}
			err := consumer.KafkaConsumer.Assign(e.Partitions)

			if err != nil {
				slog.Error("Failed to assign partitions: %v", err)
				return
			}
		case kafka.RevokedPartitions:
			for _, p := range e.Partitions {
				slog.Info("Revoked partition: %v", p.Partition)
				slog.Info("Closing channel for partition %v", p.Partition)
				close(consumer.PartitionAssignmentMap[p.Partition].TaskChannel)
				consumer.PartitionAssignmentMap[p.Partition].Completion.Wait()
				slog.Info("Closed channel for partition %v", p.Partition)
				delete(consumer.PartitionAssignmentMap, p.Partition)
			}
			err := consumer.KafkaConsumer.Unassign()
			if err != nil {
				slog.Error("Failed to unassign partitions: %v", err)
				return
			}
		default:

			slog.Info("Ignored message")
		}
	}
}

func (consumer *Consumer) Close() error {
	slog.Info("Stopping Message Consumption")
	consumer.run = false

	for partition, assignment := range consumer.PartitionAssignmentMap {
		slog.Info("Closing channel for partition %v", partition)
		close(assignment.TaskChannel)
		assignment.Completion.Wait()
		slog.Info("Closed channel for partition %v", partition)
	}

	slog.Info("Closing consumer")
	err := consumer.KafkaConsumer.Close()

	if err != nil {
		slog.Error("Failed to close consumer: %v", err)
		return err
	} else {
		slog.Info("Closed consumer successfully")
	}

	return nil
}

func (consumer *Consumer) Setup() error {
	err := consumer.Subscribe()
	if err != nil {
		return err
	}
	consumer.run = true
	go consumer.consume()

	return nil
}

func CreateConsumer(consumerConfig config.KafkaConsumerConfig) (*Consumer, error) {
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     consumerConfig.BootstrapServers,
		"group.id":              consumerConfig.GroupId,
		"auto.offset.reset":     consumerConfig.AutoOffsetReset,
		"session.timeout.ms":    *consumerConfig.SessionTimeoutMs,
		"heartbeat.interval.ms": *consumerConfig.HeartbeatIntervalMs,
		"max.poll.interval.ms":  *consumerConfig.MaxPollIntervalMs,
		"fetch.min.bytes":       *consumerConfig.FetchMinBytes,
		//"fetch.max.wait.ms":             *consumerConfig.FetchMaxWaitMs,
		"enable.auto.commit":              *consumerConfig.EnableAutoCommit,
		"go.application.rebalance.enable": true,
		"partition.assignment.strategy":   "cooperative-sticky",
	})

	if err != nil {
		return nil, err
	}

	consumer := &Consumer{KafkaConsumer: kafkaConsumer, Config: consumerConfig, PartitionAssignmentMap: make(map[int32]PartitionAssignment)}
	return consumer, nil
}
