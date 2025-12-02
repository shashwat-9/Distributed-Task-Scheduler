package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"sync"
)

type Consumer struct {
	BootstrapServers    string
	GroupId             string
	EnableAutoCommit    bool
	AutoOffsetReset     string
	SessionTimeoutMs    int
	HeartbeatIntervalMs int
	MaxPollIntervalMs   int
	FetchMinBytes       int
	FetchMaxWaitMs      int
	Topic               []string
	KafkaConsumer       *kafka.Consumer
	run                 bool
	workerPoolSize      int
	PartitionAssignment map[int32]chan *kafka.Message
	jobs                []chan *kafka.Message
	wg                  *sync.WaitGroup
}

func (consumer *Consumer) createConsumer() {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             consumer.BootstrapServers,
		"group.id":                      consumer.GroupId,
		"auto.offset.reset":             consumer.AutoOffsetReset,
		"session.timeout.ms":            consumer.SessionTimeoutMs,
		"heartbeat.interval.ms":         consumer.HeartbeatIntervalMs,
		"max.poll.interval.ms":          consumer.MaxPollIntervalMs,
		"fetch.min.bytes":               consumer.FetchMinBytes,
		"fetch.max.wait.ms":             consumer.FetchMaxWaitMs,
		"enable.auto.commit":            consumer.EnableAutoCommit,
		"partition.assignment.strategy": "Cooperative-Sticky",
	})

	if err != nil {
		panic(err)
	}
	consumer.KafkaConsumer = cons

}

func (consumer *Consumer) Subscribe() {
	err := consumer.KafkaConsumer.SubscribeTopics(consumer.Topic, nil)

	if err != nil {
		panic(err)
	}
}

func (consumer *Consumer) handleError(err error) {
	//send to DLQ
	slog.Error("Error: %v", err)
}

func (consumer *Consumer) consume() {
	defer func() {
		for i := 0; i < consumer.workerPoolSize; i++ {
			close(consumer.jobs[i])
		}
	}()
	for consumer.run {
		msg := consumer.KafkaConsumer.Poll(100)
		switch e := msg.(type) {
		case *kafka.Message:
			slog.Info("Received message: %v", string(e.Value))
			consumer.PartitionAssignment[e.TopicPartition.Partition] <- e
		case kafka.Error:
			slog.Error("Error: %v", e)
			if e.IsFatal() {
				//panic(e)
			}
		case kafka.AssignedPartitions:
			if consumer.PartitionAssignment == nil {
				consumer.PartitionAssignment = make(map[int32]chan *kafka.Message)
			}
			for i := 0; i < len(e.Partitions); i++ {
				consumer.PartitionAssignment[e.Partitions[i].Partition] = consumer.jobs[i%consumer.workerPoolSize]
			}
			err := consumer.KafkaConsumer.Assign(e.Partitions)

			if err != nil {
				return
			}
		case kafka.RevokedPartitions:
			for _, p := range e.Partitions {
				delete(consumer.PartitionAssignment, p.Partition)
			}
			err := consumer.KafkaConsumer.Unassign()
			if err != nil {
				return
			}
		default:
			slog.Info("Ignored message")
		}
	}
}

func processMessage(msg *kafka.Message) error {

	slog.Info("Processing message key: %v", msg.Key, "value :%v", msg.Value, " received in TopicPartition: %v", msg.TopicPartition)
	//pkg.method()
	return nil
}

func (consumer *Consumer) startWorkerPool() {
	consumer.jobs = make([]chan *kafka.Message, consumer.workerPoolSize)

	for i := 0; i < consumer.workerPoolSize; i++ {
		consumer.jobs[i] = make(chan *kafka.Message, 10)
		consumer.wg.Add(1)
		go func() {
			defer consumer.wg.Done()
			for msg := range consumer.jobs[i] {
				err := processMessage(msg)
				if err != nil {
					slog.Error("Error processing message: %v", err)
					consumer.handleError(err)
				}
			}
		}()
	}
}

func (consumer *Consumer) Close() error {
	consumer.run = false

	slog.Info("Closing consumer")
	err := consumer.KafkaConsumer.Close()

	slog.Info("Waiting for consumer to finish")
	consumer.wg.Wait()

	if err != nil {
		slog.Error("Failed to close consumer: %v", err)
		return err
	} else {
		slog.Info("Closed consumer successfully")
	}

	return nil
}

func (consumer *Consumer) Setup() error {
	consumer.createConsumer()
	consumer.Subscribe()
	consumer.startWorkerPool()
	go consumer.consume()

	return nil
}
