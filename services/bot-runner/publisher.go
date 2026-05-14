package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

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

func publishMetrics(client *kgo.Client, agg *AggregateMetrics, duration time.Duration, teamName, submissionID, testRunID string, correctnessScore float64) error {
	if client == nil {
		return nil
	}

	tps := float64(agg.ordersSent) / duration.Seconds()

	event := BotMetricsEvent{
		Event:            "bot.metrics",
		TeamName:         teamName,
		SubmissionID:     submissionID,
		TestRunID:        testRunID,
		DurationMS:       duration.Milliseconds(),
		OrdersSent:       agg.ordersSent,
		AcksRecv:         agg.acksRecv,
		FillsRecv:        agg.fillsRecv,
		RejectsRecv:      agg.rejectsRecv,
		ConnDrops:        agg.connDrops,
		AckP50US:         agg.ackLatency.ValueAtQuantile(50),
		AckP90US:         agg.ackLatency.ValueAtQuantile(90),
		AckP99US:         agg.ackLatency.ValueAtQuantile(99),
		AckP999US:        agg.ackLatency.ValueAtQuantile(99.9),
		AckMaxUS:         agg.ackLatency.Max(),
		FillP50US:        agg.fillLatency.ValueAtQuantile(50),
		FillP90US:        agg.fillLatency.ValueAtQuantile(90),
		FillP99US:        agg.fillLatency.ValueAtQuantile(99),
		FillP999US:       agg.fillLatency.ValueAtQuantile(99.9),
		FillMaxUS:        agg.fillLatency.Max(),
		TPS:              tps,
		CorrectnessScore: correctnessScore,
		EmittedAt:        time.Now().UnixNano(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	record := &kgo.Record{
		Topic: "bot.metrics",
		Key:   []byte(submissionID),
		Value: payload,
	}

	client.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Printf("async metrics produce failed: %v", err)
		}
	})
	return nil
}
