package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

// BuildCompleteEvent mirrors the event published by the build service.
type BuildCompleteEvent struct {
	Event        string `json:"event"`
	SubmissionID string `json:"submission_id"`
	BinaryPath   string `json:"binary_path"`
	Language     string `json:"language"`
	TeamName     string `json:"team_name"`
	BuiltAt      int64  `json:"built_at"`
}

type Consumer struct {
	client       *kgo.Client
	orchestrator *Orchestrator
	publisher    *Publisher
	topic        string
}

func NewConsumer(brokers []string, topic string, orchestrator *Orchestrator, publisher *Publisher) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("sandbox-orchestrator"),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	return &Consumer{client: client, orchestrator: orchestrator, publisher: publisher, topic: topic}, nil
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
			if err := func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("panic in handleRecord: %v", r)
						slog.Error("recovered from panic", "error", err)
					}
				}()
				return c.handleRecord(ctx, record)
			}(); err != nil {
				return fmt.Errorf("handle record: %w", err)
			}
			if err := c.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit record: %w", err)
			}
		}
	}
}

func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record) error {
	var event BuildCompleteEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		slog.Error("unmarshal event", "error", err)
		return nil // skip malformed events
	}

	if event.Event != "build.complete" {
		return nil
	}

	slog.Info("processing sandbox",
		"submission", event.SubmissionID,
		"team", event.TeamName,
	)

	result, err := c.orchestrator.Deploy(ctx, event)
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			slog.Info("sandbox already exists, treating as success", "submission", event.SubmissionID)
			return nil
		}
		slog.Error("sandbox failed", "submission", event.SubmissionID, "error", err)
		if pubErr := c.publisher.PublishSandboxFailed(ctx, event.SubmissionID, err.Error()); pubErr != nil {
			slog.Error("publish sandbox.failed", "error", pubErr)
		}
		return nil // sandbox failure is a business error, not a consumer error
	}

	slog.Info("sandbox ready",
		"submission", event.SubmissionID,
		"pod", result.PodName,
		"ip", result.PodIP,
	)
	if pubErr := c.publisher.PublishSandboxReady(
		ctx, event.SubmissionID, result.PodName, result.PodIP, event.TeamName,
	); pubErr != nil {
		return fmt.Errorf("publish sandbox.ready: %w", pubErr)
	}

	return nil
}

func (c *Consumer) Close() {
	c.client.Close()
}
