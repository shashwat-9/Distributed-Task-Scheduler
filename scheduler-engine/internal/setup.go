package internal

import (
	"go.uber.org/zap"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/k8s"
)

var appState = AppState{}

type AppState struct {
	KubernetesClient *k8s.KubernetesManager
}

func Init(appConfig config.AppConfig, logger *zap.Logger) (AppState, error) {
	logger.Info("Creating Application State:")

	logger.Info("Creating Message Producers")

	logger.Info("Creating Message Consumers")

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
