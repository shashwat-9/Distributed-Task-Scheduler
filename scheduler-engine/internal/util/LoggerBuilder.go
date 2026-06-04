package util

import (
	"go.uber.org/zap"
)

func GetLogger() *zap.Logger {

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	return logger
}
