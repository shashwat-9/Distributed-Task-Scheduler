package main

import (
	"go.uber.org/zap"
	"os"
	"os/signal"
	"scheduler-engine/internal"
	"scheduler-engine/internal/config"
	"scheduler-engine/internal/util"
	"syscall"
)

func main() {
	logger := util.GetLogger("logs/app.log", 10, 5, 28)

	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			logger.Fatal(err.Error())
		}
	}(logger)

	//load configurations
	logger.Info("Loading configurations for the application : ")
	appConfig, err := config.LoadConfig(logger.Named("Config Loader"))
	if err != nil {
		logger.Fatal("Error Loading config", zap.Error(err))
	}
	logger.Info("App configurations", zap.Any("appConfig", appConfig))

	//Initialize application
	logger.Info("Initializing application")
	appState, err := internal.Init(appConfig, logger.Named("Application Initializer"))
	if err != nil {
		logger.Fatal("Error initializing application", zap.Error(err))
	}

	//Start server
	logger.Info("Starting service")
	err = appState.StartApplication()
	if err != nil {
		logger.Fatal("Error starting message consumption", zap.Error(err))
	}

	//Shutdown Server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	err = appState.ShutdownGracefully()
	if err != nil {
		logger.Fatal("Error shutting down server", zap.Error(err))
	}
	logger.Info("Shutting down server gracefully")

}
