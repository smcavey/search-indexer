// Copyright Contributors to the Open Cluster Management project

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stolostron/search-indexer/api/proto"
	"github.com/stolostron/search-indexer/pkg/clustersync"
	"github.com/stolostron/search-indexer/pkg/config"
	"github.com/stolostron/search-indexer/pkg/database"
	grpcserver "github.com/stolostron/search-indexer/pkg/grpc"
	"github.com/stolostron/search-indexer/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/klog/v2"
)

func main() {
	// Initialize the logger.
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()
	klog.Info("Starting search-indexer.")

	// Read the config from the environment.
	config.Cfg.PrintConfig()

	// Validate required configuration to proceed.
	configError := config.Cfg.Validate()
	if configError != nil {
		klog.Fatal(configError)
	}

	ctx, exitRoutines := context.WithCancel(context.Background())

	// Initialize the database
	dao := database.NewDAO(nil)
	dao.InitializeTables(ctx)

	// Start cluster sync.
	go clustersync.ElectLeaderAndStart(ctx)

	// Start the HTTP server.
	srv := &server.ServerConfig{
		Dao: &dao,
	}
	go srv.StartAndListen(ctx)

	// Start the gRPC server if enabled.
	if config.Cfg.EnableGRPC {
		go startGRPCServer(ctx, &dao)
	}

	// Listen and wait for termination signal.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigs // Waits for termination signal.
	klog.Warningf("Received termination signal %s. Exiting server and clustersync routines. ", sig)
	exitRoutines()

	// We could use a waitgroup to wait for leader election and server to shutdown
	// but it add more complexity so keeping simple for now.
	time.Sleep(5 * time.Second)
	klog.Warning("Exiting search-indexer.")
}

func startGRPCServer(ctx context.Context, dao *database.DAO) {
	lis, err := net.Listen("tcp", config.Cfg.GRPCAddress)
	if err != nil {
		klog.Fatalf("Failed to listen on gRPC address %s: %v", config.Cfg.GRPCAddress, err)
	}

	// Configure TLS for gRPC
	var grpcServer *grpc.Server
	if !config.Cfg.DevelopmentMode {
		// Production mode: Use TLS certificates
		cert, err := tls.LoadX509KeyPair("./sslcert/tls.crt", "./sslcert/tls.key")
		if err != nil {
			klog.Fatalf("Failed to load TLS certificates for gRPC: %v", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		creds := credentials.NewTLS(tlsConfig)
		grpcServer = grpc.NewServer(grpc.Creds(creds))
	} else {
		// Development mode: No TLS
		grpcServer = grpc.NewServer()
	}

	// Register the search indexer service
	searchIndexerService := grpcserver.NewSearchIndexerService(dao)
	proto.RegisterSearchIndexerServer(grpcServer, searchIndexerService)

	klog.Info("Starting gRPC server on: ", config.Cfg.GRPCAddress)

	// Start the gRPC server in a goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			klog.Errorf("gRPC server failed to serve: %v", err)
		}
	}()

	// Wait for cancel signal
	<-ctx.Done()
	klog.Warning("Stopping the gRPC server.")
	grpcServer.GracefulStop()
	klog.Warning("gRPC server stopped.")
}
