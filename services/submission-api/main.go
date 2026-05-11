package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func main() {
	seaweedfsEndpoint := envStr("SEAWEEDFS_ENDPOINT", "http://seaweedfs.platform.svc.cluster.local:8333")
	redpandaBrokers := strings.Split(envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092"), ",")
	port := envStr("PORT", "8080")
	maxUploadMB := envInt64("MAX_UPLOAD_SIZE_MB", 128)

	storage, err := NewStorage(seaweedfsEndpoint)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}

	publisher, err := NewPublisher(redpandaBrokers)
	if err != nil {
		log.Fatalf("init publisher: %v", err)
	}
	defer publisher.Close()

	h := NewHandler(storage, publisher, maxUploadMB)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("POST /submissions", h.handleSubmit)

	log.Printf("submission-api listening :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
