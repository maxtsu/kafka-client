package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requestCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "request_count",
			Help: "Total number of requests",
		},
	)
	requestLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "request_latency_seconds",
			Help:    "Request latency",
			Buckets: prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(requestLatency)
}

func processRequest() {
	timer := prometheus.NewTimer(requestLatency)
	defer timer.ObserveDuration()

	requestCount.Inc()
	// Simulate processing time
	fmt.Println("Start time sleep")
	time.Sleep(time.Second * 10)
	fmt.Println("End time sleep")
}

func metricServer() {
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(":8000", nil); err != nil {
		fmt.Printf("unable to start server: %v", err)
	}
	fmt.Println("After blocking call")
}

func main() {
	// http.Handle("/metrics", promhttp.Handler())
	go metricServer()

	for {
		processRequest()
	}

	// if err := http.ListenAndServe(":2112", nil); err != nil {
	// 	fmt.Printf("unable to start server: %v", err)
	// }
	// fmt.Println("After blocking call")
}
