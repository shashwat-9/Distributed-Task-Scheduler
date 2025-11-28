package main

import (
	"log/slog"
	"os"
	"os/signal"
	"scheduler-engine/internal"
	"syscall"
)

func main() {
	slog.Info("Starting server")
	err := internal.Init()

	if err != nil {
		panic(err)
	}

	quit := make(chan os.Signal, 1)

	// 2. Tell the OS to notify our channel for SIGINT (Ctrl+C) or SIGTERM.
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("Server is running. Press Ctrl+C to exit.")

	// 3. Block the main function here, waiting for a signal.
	//    Your consumer will run in the background during this time.
	<-quit

	slog.Info("Shutting down server...")
	// You can add any cleanup logic here (e.g., flushing producer).
	slog.Info("Server shut down gracefully.")
}
