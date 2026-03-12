package internal

import (
	"go.uber.org/zap"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/kafka"
)

type AppState struct {
	KafkaConsumer              *kafka.Consumer
	KafkaStatusProducer        *kafka.StatusProducer
	KafkaTransactionalProducer *kafka.TransactionalProducer
}

func Init(appConfig config.AppConfig, logger *zap.Logger) (AppState, error) {
	logger.Info("Creating Kafka Producers and Consumers")

	appState := AppState{}
	producer, err := kafka.NewProducer(appConfig.KafkaConfig.ProducerConfig)
	if err != nil {
		return appState, err
	}
	appState.KafkaStatusProducer = producer

	consumer, err := kafka.NewTaskConsumer(appConfig.KafkaConfig.ConsumerConfig)
	if err != nil {
		return appState, err
	}
	appState.KafkaConsumer = consumer

	transactionalProducer, err := kafka.NewTransactionalProducer(appConfig.KafkaConfig.ProducerConfig)
	if err != nil {
		return appState, err
	}
	appState.KafkaTransactionalProducer = transactionalProducer

	return appState, nil
}

func (appstate AppState) StartApplication() error {
	err := appstate.KafkaConsumer.StartConsumption()
	if err != nil {
		return err
	}

	return nil
}

func (appState AppState) ShutdownGracefully() error {

	return nil
}
