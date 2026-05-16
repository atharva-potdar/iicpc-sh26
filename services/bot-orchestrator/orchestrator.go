package main

import (
	"context"
	"fmt"
	"log"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Orchestrator struct {
	k8s       *kubernetes.Clientset
	publisher *Publisher
	cfg       Config
}

type Config struct {
	NumBots         int
	DurationSeconds int
	JobTimeoutSec   int
	RedpandaBrokers string
	BotRunnerImage  string
}

func NewOrchestrator(publisher *Publisher, cfg Config) (*Orchestrator, error) {
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}
	return &Orchestrator{k8s: clientset, publisher: publisher, cfg: cfg}, nil
}

func (o *Orchestrator) Handle(ctx context.Context, event SandboxReadyEvent) {
	log.Printf("handling sandbox.ready: submission=%s pod=%s ip=%s",
		event.SubmissionID, event.PodName, event.PodIP)

	err := o.runTest(ctx, event)

	success := err == nil
	reason := ""
	if err != nil {
		reason = err.Error()
		log.Printf("test failed: submission=%s err=%v", event.SubmissionID, err)
	}

	// Always delete sandbox pod
	if delErr := o.deleteSandboxPod(ctx, event.PodName); delErr != nil {
		log.Printf("failed to delete sandbox pod %s: %v", event.PodName, delErr)
	}

	if pubErr := o.publisher.PublishTestComplete(ctx, TestCompleteEvent{
		SubmissionID: event.SubmissionID,
		TeamName:     event.TeamName,
		Success:      success,
		Reason:       reason,
	}); pubErr != nil {
		log.Printf("failed to publish test.complete: %v", pubErr)
	}
}

func (o *Orchestrator) runTest(ctx context.Context, event SandboxReadyEvent) error {
	jobName := fmt.Sprintf("bot-runner-%s", event.SubmissionID[:8])
	targetEndpoint := fmt.Sprintf("ws://%s:%d/stream", event.PodIP, event.WSPort)

	ttl := int32(300)
	backoff := int32(0)
	completions := int32(1)
	parallelism := int32(1)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: "bots",
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoff,
			Completions:             &completions,
			Parallelism:             &parallelism,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "bot-runner",
							Image: o.cfg.BotRunnerImage,
							Env: []corev1.EnvVar{
								{Name: "TARGET_ENDPOINT", Value: targetEndpoint},
								{Name: "NUM_BOTS", Value: fmt.Sprintf("%d", o.cfg.NumBots)},
								{Name: "DURATION_SECONDS", Value: fmt.Sprintf("%d", o.cfg.DurationSeconds)},
								{Name: "TEAM_NAME", Value: event.TeamName},
								{Name: "TEST_RUN_ID", Value: event.SubmissionID},
								{Name: "REDPANDA_BROKERS", Value: o.cfg.RedpandaBrokers},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    mustParseQuantity("100m"),
									corev1.ResourceMemory: mustParseQuantity("512Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    mustParseQuantity("2"),
									corev1.ResourceMemory: mustParseQuantity("1Gi"),
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := o.k8s.BatchV1().Jobs("bots").Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	log.Printf("created bot runner job: %s", jobName)

	log.Printf("warming up sandbox: submission=%s", event.SubmissionID)
	time.Sleep(15 * time.Second)

	if err := o.waitForJob(ctx, jobName); err != nil {
		if delErr := o.deleteJob(ctx, jobName); delErr != nil {
			log.Printf("failed to delete job %s after success: %v", jobName, delErr)
		}
		return err
	}

	if delErr := o.deleteJob(ctx, jobName); delErr != nil {
		log.Printf("failed to delete job %s after success: %v", jobName, delErr)
	}
	return nil
}

func (o *Orchestrator) waitForJob(ctx context.Context, jobName string) error {
	timeout := int64(o.cfg.JobTimeoutSec)
	watcher, err := o.k8s.BatchV1().Jobs("bots").Watch(ctx, metav1.ListOptions{
		FieldSelector:  fmt.Sprintf("metadata.name=%s", jobName),
		TimeoutSeconds: &timeout,
	})
	if err != nil {
		return fmt.Errorf("watch job: %w", err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		if event.Type == watch.Modified {
			job, ok := event.Object.(*batchv1.Job)
			if !ok {
				continue
			}
			if job.Status.Succeeded > 0 {
				log.Printf("job %s succeeded", jobName)
				return nil
			}
			if job.Status.Failed > 0 {
				return fmt.Errorf("job %s failed", jobName)
			}
		}
	}
	return fmt.Errorf("job %s timed out after %ds", jobName, o.cfg.JobTimeoutSec)
}

func (o *Orchestrator) deleteJob(ctx context.Context, jobName string) error {
	prop := metav1.DeletePropagationForeground
	return o.k8s.BatchV1().Jobs("bots").Delete(ctx, jobName, metav1.DeleteOptions{
		PropagationPolicy: &prop,
	})
}

func (o *Orchestrator) deleteSandboxPod(ctx context.Context, podName string) error {
	if podName == "" {
		return nil
	}
	return o.k8s.CoreV1().Pods("sandboxes").Delete(ctx, podName, metav1.DeleteOptions{})
}

func mustParseQuantity(s string) resource.Quantity {
	return resource.MustParse(s)
}
