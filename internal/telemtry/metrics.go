package telemtry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	streamEndCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_stream_started_total",
			Help: "Total number of RPC streams started.",
		},
		[]string{"method", "status_code"},
	)

	streamDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_stream_duration_seconds",
			Help:    "Distribution of runtime durations for gRPC streams.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(streamEndCounter)
	prometheus.MustRegister(streamDurationHistogram)
}

func StreamMetricsInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss)
	duration := time.Since(start)
	st, _ := status.FromError(err)
	statusCodeStr := st.Code().String()
	streamDurationHistogram.WithLabelValues(info.FullMethod).Observe(duration.Seconds())
	streamEndCounter.WithLabelValues(info.FullMethod, statusCodeStr).Inc()
	return err
}
