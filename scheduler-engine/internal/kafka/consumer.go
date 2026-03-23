package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/service"
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
	Logger                 *zap.Logger
}

func (consumer *Consumer) Subscribe() error {
	err := consumer.KafkaConsumer.SubscribeTopics(consumer.Config.Topic, func(c *kafka.Consumer, ev kafka.Event) error {
		switch e := ev.(type) {

		case kafka.AssignedPartitions:
			consumer.Logger.Info("Received Partition Assignment")
			for i := 0; i < len(e.Partitions); i++ {
				consumer.Logger.Info(fmt.Sprintf("Assigned partition: %v", e.Partitions[i].Partition))
				consumer.PartitionAssignmentMap[e.Partitions[i].Partition] = PartitionAssignment{
					TaskChannel: make(chan *kafka.Message, *consumer.Config.WorkerPoolSize),
					Completion:  &sync.WaitGroup{},
				}
				consumer.PartitionAssignmentMap[e.Partitions[i].Partition].Completion.Add(1)
				go func(partitionAssignmentDetails PartitionAssignment) {
					jobProcessor := service.ScheduledJobProcessor{}
					for val := range partitionAssignmentDetails.TaskChannel {
						consumer.Logger.Info(fmt.Sprintf("Processing message: %v", val))
						err := jobProcessor.Process(val.Value)
						if err != nil {
							return
						}
					}
					partitionAssignmentDetails.Completion.Done()
				}(consumer.PartitionAssignmentMap[e.Partitions[i].Partition])
			}
			err := consumer.KafkaConsumer.IncrementalAssign(e.Partitions)

			if err != nil {
				consumer.Logger.Error(fmt.Sprintf("Failed to assign partitions: %v", err))
			}
		case kafka.RevokedPartitions:
			for _, p := range e.Partitions {
				consumer.Logger.Info(fmt.Sprintf("Revoked partition: %v", p.Partition))
				consumer.Logger.Info(fmt.Sprintf("Closing channel for partition %v", p.Partition))
				close(consumer.PartitionAssignmentMap[p.Partition].TaskChannel)
				consumer.PartitionAssignmentMap[p.Partition].Completion.Wait()
				consumer.Logger.Info(fmt.Sprintf("Closed channel for partition %v", p.Partition))
				delete(consumer.PartitionAssignmentMap, p.Partition)
			}
			err := consumer.KafkaConsumer.IncrementalUnassign(e.Partitions)

			if err != nil {
				consumer.Logger.Error(fmt.Sprintf("Failed to unassign partitions: %v", err))
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (consumer *Consumer) handleError(err error) {
	//send to DLQ
}

func (consumer *Consumer) consume() {
	for consumer.run {
		msg := consumer.KafkaConsumer.Poll(100)
		switch e := msg.(type) {
		case *kafka.Message:
			consumer.Logger.Info(fmt.Sprintf("Received message: ", string(e.Value)))
			consumer.Logger.Info(fmt.Sprintf("Sending message to channel for partition %v", e.TopicPartition.Partition))
			consumer.PartitionAssignmentMap[e.TopicPartition.Partition].TaskChannel <- e
			consumer.Logger.Info(fmt.Sprintf("Sent message to channel for partition %v", e.TopicPartition.Partition))
		case kafka.Error:
			consumer.Logger.Error(fmt.Sprintf("Error: %v", e))
			if e.IsFatal() {
				//panic(e)
			}
		default:
			if e != nil {
				consumer.Logger.Info(fmt.Sprint("Ignored message: %v", e))
			}
		}
	}
}

func (consumer *Consumer) Close() error {
	consumer.Logger.Info("Stopping Message Consumption")
	consumer.run = false

	for partition, assignment := range consumer.PartitionAssignmentMap {
		consumer.Logger.Info(fmt.Sprintf("Closing channel for partition %v", partition))
		close(assignment.TaskChannel)
		assignment.Completion.Wait()
		consumer.Logger.Info(fmt.Sprintf("Closed channel for partition %v", partition))
	}

	consumer.Logger.Info("Closing consumer")
	err := consumer.KafkaConsumer.Close()

	if err != nil {
		consumer.Logger.Error(fmt.Sprintf("Failed to close consumer: %v", err))
		return err
	} else {
		consumer.Logger.Info("Closed consumer successfully")
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

	consumer := &Consumer{KafkaConsumer: kafkaConsumer, Config: consumerConfig, Logger: getLogger(), PartitionAssignmentMap: make(map[int32]PartitionAssignment)}
	return consumer, nil
}

func getLogger() *zap.Logger {
	lumberjackLogger := &lumberjack.Logger{
		Filename:   "logs/kafkaConsumer.log",
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     28,
		Compress:   false,
	}

	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "time"
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder

	return zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		zapcore.AddSync(lumberjackLogger),
		zap.InfoLevel), zap.AddCaller())

}
