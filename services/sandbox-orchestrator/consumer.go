package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
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
}

func NewConsumer(brokers []string, orchestrator *Orchestrator, publisher *Publisher) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("sandbox-orchestrator"),
		kgo.ConsumeTopics("submission.lifecycle"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	return &Consumer{client: client, orchestrator: orchestrator, publisher: publisher}, nil
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
				log.Printf("fetch error: topic=%s partition=%d err=%v", e.Topic, e.Partition, e.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			c.handleRecord(ctx, record)
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
	}
}

func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record) {
	var event BuildCompleteEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		log.Printf("unmarshal event: %v", err)
		return
	}

	if event.Event != "build.complete" {
		return
	}

	log.Printf("processing sandbox: submission=%s team=%s",
		event.SubmissionID, event.TeamName)

	result, err := c.orchestrator.Deploy(ctx, event)
	if err != nil {
		log.Printf("sandbox failed: submission=%s err=%v", event.SubmissionID, err)
		if pubErr := c.publisher.PublishSandboxFailed(ctx, event.SubmissionID, err.Error()); pubErr != nil {
			log.Printf("publish sandbox.failed: %v", pubErr)
		}
		return
	}

	log.Printf("sandbox ready: submission=%s pod=%s ip=%s",
		event.SubmissionID, result.PodName, result.PodIP)
	if pubErr := c.publisher.PublishSandboxReady(
		ctx, event.SubmissionID, result.PodName, result.PodIP, event.TeamName,
	); pubErr != nil {
		log.Printf("publish sandbox.ready: %v", pubErr)
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
