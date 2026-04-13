package internal

import (
	"go.uber.org/zap"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/consumer"
	"scheduler-engine/internal/k8s"
)

var appState = AppState{}

type AppState struct {
	TaskConsumer     *consumer.SQSClient
	KubernetesClient *k8s.KubernetesManager
}

func Init(appConfig config.AppConfig, logger *zap.Logger) (AppState, error) {
	var err error
	logger.Info("Creating Application State:")

	logger.Info("Creating Task Consumers")
	appState.TaskConsumer, err = consumer.NewSQSClient(appConfig.TaskClient.ConsumerConfig)
	if err != nil {
		return appState, err
	}

	logger.Info("Creating Kubernetes Client")
	kubernetesClient, err := k8s.GetKubernetesManager()
	if err != nil {
		return appState, err
	}

	appState.KubernetesClient = kubernetesClient
	return appState, nil
}

func (appstate AppState) StartApplication() error {

	appstate.TaskConsumer.Start()
	return nil
}

func (appState AppState) ShutdownGracefully() error {
	appState.TaskConsumer.Stop()
	return nil
}
