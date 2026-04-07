package internal

import (
	"go.uber.org/zap"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/consumer"
	"scheduler-engine/internal/k8s"
)

var appState = AppState{}

type AppState struct {
	TaskConsumer     *consumer.TaskConsumer
	KubernetesClient *k8s.KubernetesManager
}

func Init(appConfig config.AppConfig, logger *zap.Logger) (AppState, error) {
	var err error
	logger.Info("Creating Application State:")
	appState.TaskConsumer, err = consumer.NewTaskConsumer(appConfig.TaskClient.ConsumerConfig)
	if err != nil {
		return AppState{}, err
	}
	logger.Info("Creating Task Producers")

	logger.Info("Creating Task Consumers")

	logger.Info("Creating Kubernetes Client")
	kubernetesClient, err := k8s.GetKubernetesManager()
	if err != nil {
		return appState, err
	}

	appState.KubernetesClient = kubernetesClient
	return appState, nil
}

func (appstate AppState) StartApplication() error {

	return nil
}

func (appState AppState) ShutdownGracefully() error {
	return nil
}
