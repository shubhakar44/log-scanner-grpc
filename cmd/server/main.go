package main

import (
	Engine "example/log-scanner-grpc/internal/scrapper"
	pb "example/log-scanner-grpc/pkg"
)

type server struct {
	pb.UnimplementedLogScannerServer
}

func (*server) Search(req *pb.SearchRequest, stream pb.LogScanner_SearchServer) error {
	resultChannel := make(chan Engine.FileStates, 100)
	Engine.InitiateScrapper(req.Path, req.Pattern, stream.Context(), resultChannel)
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

}
