package main

import (
	"context"
	"encoding/json"
	"log/slog"

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
}

func NewConsumer(brokers []string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("bot-orchestrator"),
		kgo.ConsumeTopics("submission.lifecycle"),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Run(ctx context.Context, handler func(context.Context, SandboxReadyEvent)) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Error("fetch error", "topic", t, "partition", p, "error", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var base struct {
				Event string `json:"event"`
			}
			if err := json.Unmarshal(r.Value, &base); err != nil {
				return
			}
			if base.Event != "sandbox.ready" {
				return
			}
			var event SandboxReadyEvent
			if err := json.Unmarshal(r.Value, &event); err != nil {
				slog.Error("unmarshal sandbox.ready", "error", err)
				return
			}
			handler(ctx, event)
		})
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
