package config

import (
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type TaskConsumerConfig struct {
	QueueURL          string `mapstructure:"queue_url" validate:"required"`
	Region            string `mapstructure:"region" validate:"required"`
	MaxMessages       int32  `mapstructure:"max_messages" validate:"required"`
	WaitTimeSeconds   int32  `mapstructure:"wait_time_seconds" validate:"required"`
	VisibilityTimeout int32  `mapstructure:"visibility_timeout" validate:"required"`
}

type DLQProducerConfig struct {
	QueueURL             string            `mapstructure:"queue_url" validate:"required"`
	Region               string            `mapstructure:"region" validate:"required"`
	MessageAttributes    map[string]string `mapstructure:"message_attributes,omitempty"`
	MessageRetentionDays int32             `mapstructure:"message_retention_days,omitempty"`
}

type TaskClient struct {
	ConsumerConfig TaskConsumerConfig `mapstructure:"consumer" validate:"required"`
	ProducerConfig DLQProducerConfig  `mapstructure:"producer" validate:"required"`
	WorkersThreads int                `mapstructure:"workers_threads" validate:"required"`
}

type AppConfig struct {
	TaskClient TaskClient `mapstructure:"task_client" validate:"required"`
}

func LoadConfig(logger *zap.Logger) (AppConfig, error) {
	var appConfig AppConfig

	logger.Info("Setting up configurations from config.yaml")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return appConfig, err
	}

	if err := viper.Unmarshal(&appConfig); err != nil {
		return appConfig, err
	}

	validate := validator.New()
	if err := validate.Struct(appConfig); err != nil {
		return appConfig, err
	}

	return appConfig, nil
}

func (appConfig AppConfig) String() string {
	return ""
}
