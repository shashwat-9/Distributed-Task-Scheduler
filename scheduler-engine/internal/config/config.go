package config

import (
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
	"log"
)

type KafkaConsumerConfig struct {
	BootstrapServers    string   `mapstructure:"bootstrap_servers" validate:"required"`
	GroupId             string   `mapstructure:"group_id" validate:"required"`
	Topic               []string `mapstructure:"topic" validate:"required,min=1"`
	EnableAutoCommit    *bool    `mapstructure:"enable_auto_commit" validate:"required"`
	AutoOffsetReset     string   `mapstructure:"auto_offset_reset" validate:"required"`
	SessionTimeoutMs    *int     `mapstructure:"session_timeout_ms" validate:"required"`
	HeartbeatIntervalMs *int     `mapstructure:"heartbeat_interval_ms" validate:"required"`
	MaxPollIntervalMs   *int     `mapstructure:"max_poll_interval_ms" validate:"required"`
	FetchMinBytes       *int     `mapstructure:"fetch_min_bytes" validate:"required"`
	FetchMaxWaitMs      *int     `mapstructure:"fetch_max_wait_ms" validate:"required"`
	WorkerPoolSize      *int     `mapstructure:"worker_pool_size" validate:"required"`
}

type KafkaProducerConfig struct {
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
}

type KafkaConfig struct {
	ConsumerConfig KafkaConsumerConfig `mapstructure:"consumer" validate:"required"`
	ProducerConfig KafkaProducerConfig `mapstructure:"producer" validate:"required"`
}

type AppConfig struct {
	KafkaConfig KafkaConfig `mapstructure:"kafka" validate:"required"`
}

func LoadConfigAndValidate() (AppConfig, error) {
	var appConfig AppConfig

	log.Println("Setting up configurations from config.yaml")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return AppConfig{}, err
	}

	if err := viper.Unmarshal(&appConfig); err != nil {
		return AppConfig{}, err
	}

	validate := validator.New()
	if err := validate.Struct(appConfig); err != nil {
		return AppConfig{}, err
	}

	return appConfig, nil
}
