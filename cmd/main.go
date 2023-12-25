package main

import (
	"context"
	"flag"
	"github.com/ssfilatov/ts/pkg/server"
	"github.com/ssfilatov/ts/pkg/storage"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	defaultPartitionSize = 4096
	defaultDir = "/app/test-files"
	partitionDir = "partitions"
)

func main() {

	partitionSize := flag.Int("partition-size",
		defaultPartitionSize, "sets number of records per partition")
	dir := flag.String("dir", defaultDir, "dir containing data files")
	flag.Parse()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx := context.Background()
	partitionStorage, err := storage.NewStorage(ctx, *partitionSize, *dir)
	if err != nil {
		log.Fatalf("error building storage: %v", err)
	}

	srv := server.NewServer(partitionStorage)
	go func() {
		if err := srv.Run(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("error running server: %v", err)
		}
	}()
	log.Print("server started")

	<-done
	log.Print("server stopped")
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer func() {
		if err := os.RemoveAll(partitionDir); err != nil {
			log.Printf("error removing partition data: %v", err)
		}
		cancel()
	}()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server shutdown failed: %v", err)
	}
	log.Print("server exited properly")
}