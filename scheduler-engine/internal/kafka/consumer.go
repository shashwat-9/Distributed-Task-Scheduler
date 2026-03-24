package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"scheduler-engine/internal/Processor"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/util"
	"sync"
)

var Logger *zap.Logger

type PartitionMapping struct {
	TaskChannel chan *kafka.Message
	Completion  *sync.WaitGroup
}

type Consumer struct {
	Config        config.KafkaConsumerConfig
	KafkaConsumer *kafka.Consumer
	run           bool
	PartitionMap  map[int32]PartitionMapping
}

func (consumer *Consumer) Subscribe() error {
	err := consumer.KafkaConsumer.SubscribeTopics(consumer.Config.Topic, func(c *kafka.Consumer, ev kafka.Event) error {
		switch e := ev.(type) {

		case kafka.AssignedPartitions:
			Logger.Info("Received Partition Assignment")
			for i := 0; i < len(e.Partitions); i++ {
				Logger.Info(fmt.Sprintf("Assigned partition: %v", e.Partitions[i].Partition))
				consumer.PartitionMap[e.Partitions[i].Partition] = PartitionMapping{
					TaskChannel: make(chan *kafka.Message, *consumer.Config.WorkerPoolSize),
					Completion:  &sync.WaitGroup{},
				}
				consumer.PartitionMap[e.Partitions[i].Partition].Completion.Add(1)
				go func(partitionMapping PartitionMapping) {
					var jobProcessor Processor.JobProcessor
					jobProcessor = Processor.NewRebalancedJobProcessor()
					for val := range partitionMapping.TaskChannel {
						Logger.Info(fmt.Sprintf("Processing message: %v", val))
						err := jobProcessor.Process(val.Value)
						if err != nil {
							//nil to be replaced with error handling below
							if err == nil {
								jobProcessor = Processor.NewScheduledJobProcessor()
								err = jobProcessor.Process(val.Value)
								if err != nil {
									return
								}
								continue
							}
							return
						}
					}
					partitionMapping.Completion.Done()
				}(consumer.PartitionMap[e.Partitions[i].Partition])
			}
			err := consumer.KafkaConsumer.IncrementalAssign(e.Partitions)

			if err != nil {
				Logger.Error(fmt.Sprintf("Failed to assign partitions: %v", err))
			}
		case kafka.RevokedPartitions:
			for _, p := range e.Partitions {
				Logger.Info(fmt.Sprintf("Revoked partition: %v", p.Partition))
				Logger.Info(fmt.Sprintf("Closing channel for partition %v", p.Partition))
				close(consumer.PartitionMap[p.Partition].TaskChannel)
				consumer.PartitionMap[p.Partition].Completion.Wait()
				Logger.Info(fmt.Sprintf("Closed channel for partition %v", p.Partition))
				delete(consumer.PartitionMap, p.Partition)
			}
			err := consumer.KafkaConsumer.IncrementalUnassign(e.Partitions)

			if err != nil {
				Logger.Error(fmt.Sprintf("Failed to unassign partitions: %v", err))
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (consumer *Consumer) StartConsumption() error {
	err := consumer.Subscribe()
	if err != nil {
		return err
	}
	consumer.run = true
	go consumer.consume()

	return nil
}

func (consumer *Consumer) consume() {
	for consumer.run {
		msg := consumer.KafkaConsumer.Poll(100)
		switch e := msg.(type) {
		case *kafka.Message:
			Logger.Info(fmt.Sprintf("Received message: ", string(e.Value)))
			Logger.Info(fmt.Sprintf("Sending message to channel for partition %v", e.TopicPartition.Partition))
			consumer.PartitionMap[e.TopicPartition.Partition].TaskChannel <- e
			Logger.Info(fmt.Sprintf("Sent message to channel for partition %v", e.TopicPartition.Partition))
		case kafka.Error:
			Logger.Error(fmt.Sprintf("Error: %v", e))
			if e.IsFatal() {
				//panic(e)
			}
		default:
			if e != nil {
				Logger.Info(fmt.Sprint("Ignored message: %v", e))
			}
		}
	}
}

func (consumer *Consumer) handleError(err error) {
	//send to DLQ
}

func (consumer *Consumer) Close() error {
	Logger.Info("Stopping Message Consumption")
	consumer.run = false

	for partition, assignment := range consumer.PartitionMap {
		Logger.Info(fmt.Sprintf("Closing channel for partition %v", partition))
		close(assignment.TaskChannel)
		assignment.Completion.Wait()
		Logger.Info(fmt.Sprintf("Closed channel for partition %v", partition))
	}

	Logger.Info("Closing consumer")
	err := consumer.KafkaConsumer.Close()

	if err != nil {
		Logger.Error(fmt.Sprintf("Failed to close consumer: %v", err))
		return err
	} else {
		Logger.Info("Closed consumer successfully")
	}

	return nil
}

func NewTaskConsumer(consumerConfig config.KafkaConsumerConfig) (*Consumer, error) {
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               consumerConfig.BootstrapServers,
		"group.id":                        consumerConfig.GroupId,
		"auto.offset.reset":               consumerConfig.AutoOffsetReset,
		"session.timeout.ms":              *consumerConfig.SessionTimeoutMs,
		"heartbeat.interval.ms":           *consumerConfig.HeartbeatIntervalMs,
		"max.poll.interval.ms":            *consumerConfig.MaxPollIntervalMs,
		"fetch.min.bytes":                 *consumerConfig.FetchMinBytes,
		"enable.auto.commit":              *consumerConfig.EnableAutoCommit,
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        false,
		"partition.assignment.strategy":   "cooperative-sticky",
		//"debug":                         "consumer",
	})

	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		KafkaConsumer: kafkaConsumer,
		Config:        consumerConfig,
		PartitionMap:  make(map[int32]PartitionMapping),
	}
	Logger = util.GetLogger("logs/kafkaConsumer.log", 10, 5, 28)

	return consumer, nil
}
