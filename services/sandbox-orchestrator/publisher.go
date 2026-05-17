package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SandboxReadyEvent is published when a sandbox pod becomes healthy.
type SandboxReadyEvent struct {
	Event        string `json:"event"`
	SubmissionID string `json:"submission_id"`
	PodName      string `json:"pod_name"`
	PodIP        string `json:"pod_ip"`
	HTTPPort int `json:"http_port"`
	TeamName     string `json:"team_name"`
	ReadyAt      int64  `json:"ready_at"`
}

// SandboxFailedEvent is published when sandbox deployment fails.
type SandboxFailedEvent struct {
	Event        string `json:"event"`
	SubmissionID string `json:"submission_id"`
	Reason       string `json:"reason"`
	FailedAt     int64  `json:"failed_at"`
}

type Publisher struct {
	client *kgo.Client
	topic  string
}

func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	return &Publisher{client: client, topic: topic}, nil
}

func (p *Publisher) PublishSandboxReady(
	ctx context.Context,
	submissionID, podName, podIP, teamName string,
) error {
	event := SandboxReadyEvent{
		Event:        "sandbox.ready",
		SubmissionID: submissionID,
		PodName:      podName,
		PodIP:        podIP,
		HTTPPort:     httpPort,
		TeamName:     teamName,
		ReadyAt:      time.Now().UnixNano(),
	}
	return p.publish(ctx, submissionID, event)
}

func (p *Publisher) PublishSandboxFailed(
	ctx context.Context,
	submissionID, reason string,
) error {
	event := SandboxFailedEvent{
		Event:        "sandbox.failed",
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
		Topic: p.topic,
		Key:   []byte(key),
		Value: payload,
	}

	pCtx, pCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pCancel()
	if err := p.client.ProduceSync(pCtx, record).FirstErr(); err != nil {
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
	return s[:max]
}
