package internal

import (
	"go.uber.org/zap"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/k8s"
	"scheduler-engine/internal/kafka"
)

var appState = AppState{}

type AppState struct {
	KafkaConsumer              *kafka.Consumer
	KafkaStatusProducer        *kafka.StatusProducer
	KafkaTransactionalProducer *kafka.TransactionalProducer
	KubernetesClient           *k8s.KubernetesManager
}

func Init(appConfig config.AppConfig, logger *zap.Logger) (AppState, error) {
	logger.Info("Creating Application State:")

	logger.Info("Creating Kafka Producers")
	producer, err := kafka.GetProducer(appConfig.KafkaConfig.ProducerConfig)
	if err != nil {
		return appState, err
	}
	appState.KafkaStatusProducer = producer

	logger.Info("Creating Kafka Consumers")
	consumer, err := kafka.NewTaskConsumer(appConfig.KafkaConfig.ConsumerConfig)
	if err != nil {
		return appState, err
	}
	appState.KafkaConsumer = consumer

	logger.Info("Creating Kafka Transactional Producer")
	transactionalProducer, err := kafka.NewTransactionalProducer(appConfig.KafkaConfig.ProducerConfig)
	if err != nil {
		return appState, err
	}
	appState.KafkaTransactionalProducer = transactionalProducer

	logger.Info("Creating Kubernetes Client")
	kubernetesClient, err := k8s.GetKubernetesManager()
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

	//add k8s pool closing
	return nil
}
