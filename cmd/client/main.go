package main

import (
	"context"
	pb "example/log-scanner-grpc/pkg"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	log.Println("Connection succfull")

	defer conn.Close()

	client := pb.NewLogScannerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stream, err := client.Search(ctx, &pb.SearchRequest{
		Path:    "../../test/logs",
		Pattern: "connection pool exhausted",
	})

	if err != nil {
		log.Fatal("An issue occured", err)
	}

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			log.Printf("End of stream")
			break
		}
		if err != nil {
			log.Fatal("Something happened", err)
		}
		log.Println("File", res.File, "\n Line number", res.LineNumber, "\n Content", res.Content)
	}
}
