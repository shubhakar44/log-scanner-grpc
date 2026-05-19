package main

import (
	pb "example/log-scanner-grpc/pkg";
	"google.golang.org/grpc"
	
)


type Server struct {
	pb.UnimplementedLogScannerServer
}

