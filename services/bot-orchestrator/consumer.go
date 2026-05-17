package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

type SandboxReadyEvent struct {
	Event        string `json:"event"`
	SubmissionID string `json:"submission_id"`
	PodName      string `json:"pod_name"`
	PodIP        string `json:"pod_ip"`
	HTTPPort     int    `json:"http_port"`
	WSPort       int    `json:"ws_port"`
	TeamName     string `json:"team_name"`
	ReadyAt      int64  `json:"ready_at"`
}

type Consumer struct {
	client *kgo.Client
	topic  string
}

func NewConsumer(brokers []string, topic string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("bot-orchestrator"),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, topic: topic}, nil
}

func (c *Consumer) Run(ctx context.Context, handler func(context.Context, SandboxReadyEvent)) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var base struct {
				Event string `json:"event"`
			}
			if err := json.Unmarshal(r.Value, &base); err != nil {
				slog.Warn("malformed JSON in record", "error", err, "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
				return
			}
			if base.Event != "sandbox.ready" {
				return
			}
			var event SandboxReadyEvent
			if err := json.Unmarshal(r.Value, &event); err != nil {
				slog.Error("failed to unmarshal sandbox.ready", "error", err)
				return
			}
			go func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("panic in consumer handler", "error", r)
					}
				}()
				handler(ctx, event)
				if err := c.client.CommitRecords(ctx, r); err != nil {
					slog.Error("failed to commit record", "error", err)
				}
			}()
		})

		// We use manual per-record commit, so no need for CommitUncommittedOffsets here
		// unless we want to commit skipped records.
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
