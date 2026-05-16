package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SubmissionCreatedEvent mirrors the event published by the submission API.
type SubmissionCreatedEvent struct {
	Event        string `json:"event"`
	SubmissionID string `json:"submission_id"`
	Language     string `json:"language"`
	TeamName     string `json:"team_name"`
	ArtifactPath string `json:"artifact_path"`
	CreatedAt    int64  `json:"created_at"`
}

type Consumer struct {
	client    *kgo.Client
	builder   *Builder
	publisher *Publisher
	topic     string
}

func NewConsumer(brokers []string, topic string, builder *Builder, publisher *Publisher) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("build-service"),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	return &Consumer{client: client, builder: builder, publisher: publisher, topic: topic}, nil
}

// Run consumes events until the context is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				slog.Error("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
			}
			continue
		}

		for _, record := range fetches.Records() {
			if err := c.handleRecord(ctx, record); err != nil {
				return fmt.Errorf("handle record: %w", err)
			}
			if err := c.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit record: %w", err)
			}
		}
	}
}

func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record) error {
	var event SubmissionCreatedEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		slog.Error("unmarshal event", "error", err)
		return nil // skip malformed events
	}

	if event.Event != "submission.created" {
		return nil
	}

	slog.Info("processing build",
		"submission", event.SubmissionID,
		"lang", event.Language,
		"team", event.TeamName,
	)

	result, err := c.builder.Build(ctx, event)
	if err != nil {
		slog.Error("build failed", "submission", event.SubmissionID, "error", err)
		if pubErr := c.publisher.PublishBuildFailed(ctx, event.SubmissionID, err.Error()); pubErr != nil {
			slog.Error("publish build.failed", "error", pubErr)
		}
		return nil // we published a failure, so this record is "handled"
	}

	slog.Info("build complete", "submission", event.SubmissionID, "binary", result.BinaryPath)
	if pubErr := c.publisher.PublishBuildComplete(
		ctx, event.SubmissionID, result.BinaryPath, event.Language, event.TeamName,
	); pubErr != nil {
		return fmt.Errorf("publish build.complete: %w", pubErr)
	}

	return nil
}

func (c *Consumer) Close() {
	c.client.Close()
}
