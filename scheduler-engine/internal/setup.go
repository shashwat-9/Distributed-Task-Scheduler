package internal

import (
	"log"
	"scheduler-engine/internal/config"
	//"scheduler-engine/internal/kafka"
)

func Init() error {
	log.Println("Setting up application")
	appConfig, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}
	log.Println(appConfig)
	//setUpProducer(appConfig.ProducerConfig)
	//setUpConsumer(appConfig.ConsumerConfig)
	return nil
}

//func setUpConsumer(consumerConfig config.KafkaConsumerConfig) (consumer kafka.Consumer, err error) {
//	consumer = kafka.Consumer{
//		BootstrapServers:    consumerConfig.BootstrapServers,
//		GroupId:             consumerConfig.GroupId,
//		Topic:               consumerConfig.Topic,
//		SessionTimeoutMs:    consumerConfig.SessionTimeoutMs,
//		MaxPollIntervalMs:   consumerConfig.MaxPollIntervalMs,
//		HeartbeatIntervalMs: consumerConfig.HeartbeatIntervalMs,
//	}
//
//	return consumer, nil
//}
//
//func setUpProducer(producerConfig config.KafkaProducerConfig) (producer kafka.Producer, err error) {
//
//}
