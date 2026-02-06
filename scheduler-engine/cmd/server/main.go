package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"scheduler-engine/internal"
	"syscall"
)

func main() {
	slog.Info("Initializing server...")
	kafkaConsumer, kafkaProducer, kafkaTransactionalProducer, err := internal.Init()

	if err != nil {
		log.Fatal(err)
	}

	quit := make(chan os.Signal, 1)

	kafkaTransactionalProducer.Produce("events", "testkey", "testval", 0)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("Server is running. Press Ctrl+C to exit.")

	<-quit

	slog.Info("Shutting down server...")

	err = kafkaConsumer.Close()
	if err != nil {
		slog.Error("Error", err)
	}
	kafkaProducer.StatusProducer.Close()
	slog.Info("Server shut down gracefully.")
}
