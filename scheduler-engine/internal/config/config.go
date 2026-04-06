package config

import (
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type TaskConsumerConfig struct {
	queueURL          string `mapstructure:"queue_url" validate:"required"`
	maxMessages       int32  `mapstructure:"max_messages" validate:"required"`
	waitTimeSeconds   int32  `mapstructure:"wait_time_seconds" validate:"required"`
	visibilityTimeout int32  `mapstructure:"visibility_timeout" validate:"required"`
	workers           int    `mapstructure:"workers" validate:"required"`
	shutdown          chan struct{}
}

type SQSProducerConfig struct {
	BootstrapServers                 string `mapstructure:"bootstrap_servers" validate:"required"`
	Topic                            string `mapstructure:"topic" validate:"required"`
	ClientID                         string `mapstructure:"client_id" validate:"required"`
	Acks                             string `mapstructure:"acks" validate:"required"`
	DeliveryTimeoutMs                *int   `mapstructure:"delivery_timeout_ms" validate:"required"`
	RequestTimeoutMs                 *int   `mapstructure:"request_timeout_ms" validate:"required"`
	LingerMs                         *int   `mapstructure:"linger_ms" validate:"required"`
	QueueBufferingMaxKbytes          *int   `mapstructure:"queue_buffering_max_kbytes" validate:"required"`
	CompressionType                  string `mapstructure:"compression_type" validate:"required"`
	BatchSize                        *int   `mapstructure:"batch_size" validate:"required"`
	MaxInFlightRequestsPerConnection *int   `mapstructure:"max_in_flight_requests_per_connection" validate:"required"`
	MaxRequestSize                   *int   `mapstructure:"max_request_size" validate:"required"`
	EnableIdempotence                *bool  `mapstructure:"enable_idempotence" validate:"required"`
	TransactionalID                  string `mapstructure:"transactional_id" validate:"required"`
}

type SQSConfig struct {
	ConsumerConfig TaskConsumerConfig `mapstructure:"consumer" validate:"required"`
	ProducerConfig SQSProducerConfig  `mapstructure:"producer" validate:"required"`
}

type AppConfig struct {
	SQSConfig SQSConfig `mapstructure:"kafka" validate:"required"`
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
