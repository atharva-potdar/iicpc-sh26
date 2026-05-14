package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BotMetricsEvent struct {
	Event            string  `json:"event"`
	TeamName         string  `json:"team_name"`
	SubmissionID     string  `json:"submission_id"`
	TestRunID        string  `json:"test_run_id"`
	DurationMS       int64   `json:"duration_ms"`
	OrdersSent       int64   `json:"orders_sent"`
	AcksRecv         int64   `json:"acks_recv"`
	FillsRecv        int64   `json:"fills_recv"`
	RejectsRecv      int64   `json:"rejects_recv"`
	ConnDrops        int64   `json:"conn_drops"`
	AckP50US         int64   `json:"ack_p50_us"`
	AckP90US         int64   `json:"ack_p90_us"`
	AckP99US         int64   `json:"ack_p99_us"`
	AckP999US        int64   `json:"ack_p999_us"`
	AckMaxUS         int64   `json:"ack_max_us"`
	FillP50US        int64   `json:"fill_p50_us"`
	FillP90US        int64   `json:"fill_p90_us"`
	FillP99US        int64   `json:"fill_p99_us"`
	FillP999US       int64   `json:"fill_p999_us"`
	FillMaxUS        int64   `json:"fill_max_us"`
	TPS              float64 `json:"tps"`
	CorrectnessScore float64 `json:"correctness_score"`
	EmittedAt        int64   `json:"emitted_at"`
}

type Consumer struct {
	client *kgo.Client
}

func NewConsumer(brokers []string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("telemetry-ingester"),
		kgo.ConsumeTopics("bot.metrics"),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Run(ctx context.Context, handler func(context.Context, BotMetricsEvent)) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("fetch error topic=%s partition=%d: %v", t, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var event BotMetricsEvent
			if err := json.Unmarshal(r.Value, &event); err != nil {
				log.Printf("unmarshal bot.metrics: %v", err)
				return
			}
			if event.Event != "bot.metrics" {
				return
			}
			handler(ctx, event)
		})
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
