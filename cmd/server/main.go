package main

import (
	Engine "example/log-scanner-grpc/internal/scrapper"
	pb "example/log-scanner-grpc/pkg"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedLogScannerServer
}

func (*server) Search(req *pb.SearchRequest, stream pb.LogScanner_SearchServer) error {
	resultChannel := make(chan Engine.FileStates, 100)
	go Engine.InitiateScrapper(req.Path, req.Pattern, stream.Context(), resultChannel)
	for {
		select {
		case <-stream.Context().Done():
			// Client disconnected or timeout occurred
			return stream.Context().Err()
		case item, ok := <-resultChannel:
			if !ok {
				// Channel was closed by the producer; streaming is complete
				return nil
			}

			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			default:
			}

			// Construct and send the response
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
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterLogScannerServer(grpcServer, &server{})
	reflection.Register(grpcServer)
	log.Println("gRPC server is running on port :50051...")
	err = grpcServer.Serve(lis)

	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
