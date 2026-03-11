package main

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"os/signal"
	"scheduler-engine/internal"
	"scheduler-engine/internal/config"
	"syscall"
)

func main() {
	logger := getLogger()

	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			logger.Fatal(err.Error())
		}
	}(logger)

	//load configurations
	logger.Info("Loading configurations")
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
	logger.Info("Shutting down server gracefully")

}

func getLogger() *zap.Logger {
	lumberjackLogger := &lumberjack.Logger{
		Filename:   "logs/app.log",
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
