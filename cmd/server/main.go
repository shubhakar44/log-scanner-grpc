package main

import (
	"context"
	db "example/log-scanner-grpc/internal/database"
	Engine "example/log-scanner-grpc/internal/scrapper"
	telemtry "example/log-scanner-grpc/internal/telemtry"
	pb "example/log-scanner-grpc/pkg"
	"log"
	"net"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedLogScannerServer
	dbPool *pgxpool.Pool
}

func (s *server) Search(req *pb.SearchRequest, stream pb.LogScanner_SearchServer) error {
	resultChannel := make(chan Engine.FileStates, 100)
	log.Println("Query and Path", req)
	db.Insert(s.dbPool, req.Pattern, req.Path, stream.Context())
	go Engine.InitiateScrapper(req.Path, req.Pattern, stream.Context(), resultChannel)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case item, ok := <-resultChannel:
			if !ok {
				return nil // Stream complete
			}

			resp := &pb.SearchResponse{
				File:       item.File,
				LineNumber: item.LineNumber,
				Content:    item.Content,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func main() {
	ctx := context.Background()
	pool, err := db.Init(ctx)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics endpoint ready on http://localhost:2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(telemtry.StreamMetricsInterceptor),
	)
	pb.RegisterLogScannerServer(grpcServer, &server{dbPool: pool})
	reflection.Register(grpcServer)
	log.Println("gRPC server is running on port :50051...")
	err = grpcServer.Serve(lis)

	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
