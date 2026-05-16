package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

const lifecycleTopic = "submission.lifecycle"

// BuildCompleteEvent is published when a build succeeds.
type BuildCompleteEvent struct {
	Event        string `json:"event"`
	SubmissionID string `json:"submission_id"`
	BinaryPath   string `json:"binary_path"`
	Language     string `json:"language"`
	TeamName     string `json:"team_name"`
	BuiltAt      int64  `json:"built_at"`
}

// BuildFailedEvent is published when a build fails.
type BuildFailedEvent struct {
	Event        string `json:"event"`
	SubmissionID string `json:"submission_id"`
	Reason       string `json:"reason"`
	FailedAt     int64  `json:"failed_at"`
}

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(brokers []string) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	return &Publisher{client: client}, nil
}

func (p *Publisher) PublishBuildComplete(
	ctx context.Context,
	submissionID, binaryPath, language, teamName string,
) error {
	event := BuildCompleteEvent{
		Event:        "build.complete",
		SubmissionID: submissionID,
		BinaryPath:   binaryPath,
		Language:     language,
		TeamName:     teamName,
		BuiltAt:      time.Now().UnixNano(),
	}
	return p.publish(ctx, submissionID, event)
}

func (p *Publisher) PublishBuildFailed(
	ctx context.Context,
	submissionID, reason string,
) error {
	event := BuildFailedEvent{
		Event:        "build.failed",
		SubmissionID: submissionID,
		Reason:       truncate(reason, 4096),
		FailedAt:     time.Now().UnixNano(),
	}
	return p.publish(ctx, submissionID, event)
}

func (p *Publisher) publish(ctx context.Context, key string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	record := &kgo.Record{
		Topic: lifecycleTopic,
		Key:   []byte(key),
		Value: payload,
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}

func (p *Publisher) Close() {
	p.client.Close()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	s = s[:max]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
