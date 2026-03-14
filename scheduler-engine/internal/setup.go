package internal

import (
	"go.uber.org/zap"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/kafka"
	"scheduler-engine/internal/service"
)

type AppState struct {
	KafkaConsumer              *kafka.Consumer
	KafkaStatusProducer        *kafka.StatusProducer
	KafkaTransactionalProducer *kafka.TransactionalProducer
	KubernetesClient           *service.KubernetesManager
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

	logger.Info("Creating Kubernetes Client")
	kubernetesClient, err := service.NewKubernetesManager()
	if err != nil {
		return appState, err
	}

	appState.KubernetesClient = kubernetesClient
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
	err := appState.KafkaConsumer.Close()
	if err != nil {
		return err
	}
	appState.KafkaStatusProducer.StatusProducer.Close()

	return nil
}
