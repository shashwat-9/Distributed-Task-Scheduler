package internal

import (
	"log"
	"log/slog"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/kafka"
)

func Init() (*kafka.Consumer, *kafka.Producer, error) {
	slog.Info("Setting up application")
	appConfig, err := config.LoadConfigAndValidate()

	if err != nil {
		return nil, nil, err
	}

	log.Println(appConfig)

	kafkaProducer, err := kafka.NewProducer(appConfig.KafkaConfig.ProducerConfig)
	if err != nil {
		return nil, nil, err
	}

	kafkaConsumer, err := kafka.New(appConfig.KafkaConfig.ConsumerConfig)

	if err != nil {
		return nil, nil, err
	}

	kafkaConsumer.Setup()
	//TaskProcessor
	//	taskProcessor := service.NewTaskProcessor(kafkaProducer, kafkaConsumer.PartitionAssignment)

	//Container pool connection
	//Setup S3

	return kafkaConsumer, kafkaProducer, nil
}
