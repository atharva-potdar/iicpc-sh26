package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	buildNamespace     = "builds"
	srcBucket          = "submissions"
	binaryBucket       = "builds"
)

var buildImages = map[string]string{
	"cpp":  "gcc:16-trixie",
	"rust": "rust:1.95-alpine",
	"go":   "golang:1.26-alpine",
}

var buildCommands = map[string]string{
	"cpp":  "g++ -static -O2 -o /workspace/binary /workspace/main.cpp",
	"rust": "cd /workspace && RUSTFLAGS=\"-C target-feature=+crt-static\" cargo build --release --offline && cp $(find target/release -maxdepth 1 -type f -perm -111 ! -name '*.d' | head -1) /workspace/binary",
	"go":   "cd /workspace && CGO_ENABLED=0 go build -mod=vendor -o /workspace/binary .",
}

type BuildResult struct {
	BinaryPath string
}

type Builder struct {
	s3Client      *s3.Client
	presignClient *s3.PresignClient
	k8sClient     kubernetes.Interface
	restConfig    *rest.Config
	timeout       time.Duration
	maxLogBytes   int
}

func NewBuilder(seaweedfsEndpoint string, timeoutSec, maxLogBytes int) (*Builder, error) {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("any", "any", ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(seaweedfsEndpoint)
		o.UsePathStyle = true
	})
	presignClient := s3.NewPresignClient(s3Client)

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s in-cluster config: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s clientset: %w", err)
	}

	return &Builder{
		s3Client:      s3Client,
		presignClient: presignClient,
		k8sClient:     k8s,
		restConfig:    restCfg,
		timeout:       time.Duration(timeoutSec) * time.Second,
		maxLogBytes:   maxLogBytes,
	}, nil
}

func (b *Builder) Build(ctx context.Context, event SubmissionCreatedEvent) (*BuildResult, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	image, ok := buildImages[event.Language]
	if !ok {
		return nil, fmt.Errorf("unsupported language: %s", event.Language)
	}
	buildCmd, ok := buildCommands[event.Language]
	if !ok {
		return nil, fmt.Errorf("no build command for language: %s", event.Language)
	}

	downloadReq, err := b.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(event.ArtifactPath),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return nil, fmt.Errorf("presign download: %w", err)
	}

	binaryPath := fmt.Sprintf("builds/%s/binary", event.SubmissionID)
	uploadReq, err := b.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(binaryBucket),
		Key:    aws.String(binaryPath),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return nil, fmt.Errorf("presign upload: %w", err)
	}

	jobName := fmt.Sprintf("build-%s", event.SubmissionID)
	
	_, err = b.createBuildJob(ctx, jobName, image, buildCmd, downloadReq.URL, uploadReq.URL)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	defer b.cleanupJob(context.Background(), jobName)

	if err := b.waitForJobComplete(ctx, jobName); err != nil {
		logs := b.collectJobLogs(context.Background(), jobName)
		reason := fmt.Sprintf("build failed: %v\n\nlogs:\n%s", err, logs)
		if len(reason) > b.maxLogBytes {
			reason = reason[:b.maxLogBytes]
		}
		return nil, fmt.Errorf("%s", reason)
	}

	return &BuildResult{BinaryPath: binaryPath}, nil
}

func (b *Builder) createBuildJob(ctx context.Context, name, image, buildCmd, downloadURL, uploadURL string) (*batchv1.Job, error) {
	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: buildNamespace,
			Labels: map[string]string{
				"app":  "build",
				"role": "build-job",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "build",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					InitContainers: []corev1.Container{
						{
							Name:       "download-and-extract",
							Image:      "alpine:3.23",
							Command:    []string{"sh", "-c", fmt.Sprintf("wget -qO /workspace/src.tar.gz '%s' && cd /workspace && tar xzf src.tar.gz", downloadURL)},
							WorkingDir: "/workspace",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/workspace"},
							},
						},
						{
							Name:       "compile",
							Image:      image,
							Command:    []string{"sh", "-c", buildCmd},
							WorkingDir: "/workspace",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/workspace"},
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("2"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:       "upload",
							Image:      "alpine:3.23",
							Command:    []string{"sh", "-c", fmt.Sprintf("apk add --no-cache curl && curl -f -s -X PUT -T /workspace/binary '%s'", uploadURL)},
							WorkingDir: "/workspace",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/workspace"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "workspace",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{
									SizeLimit: resourcePtr(resource.MustParse("512Mi")),
								},
							},
						},
					},
				},
			},
		},
	}

	return b.k8sClient.BatchV1().Jobs(buildNamespace).Create(ctx, job, metav1.CreateOptions{})
}

func (b *Builder) waitForJobComplete(ctx context.Context, jobName string) error {
	watcher, err := b.k8sClient.BatchV1().Jobs(buildNamespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", jobName),
	})
	if err != nil {
		return fmt.Errorf("watch job: %w", err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		job, ok := event.Object.(*batchv1.Job)
		if !ok {
			continue
		}
		if job.Status.Succeeded > 0 {
			return nil
		}
		if job.Status.Failed > 0 {
			return fmt.Errorf("job failed")
		}
	}
	return fmt.Errorf("watch closed before job completion")
}

func (b *Builder) collectJobLogs(ctx context.Context, jobName string) string {
	pods, err := b.k8sClient.CoreV1().Pods(buildNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil || len(pods.Items) == 0 {
		return "(could not find pod to fetch logs)"
	}
	pod := pods.Items[0]

	var failedContainer string
	for _, status := range pod.Status.InitContainerStatuses {
		if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
			failedContainer = status.Name
			break
		}
	}
	if failedContainer == "" {
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
				failedContainer = status.Name
				break
			}
		}
	}

	if failedContainer == "" {
		return "(could not determine which container failed)"
	}

	req := b.k8sClient.CoreV1().Pods(buildNamespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		Container: failedContainer,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Sprintf("(failed to fetch logs for %s: %v)", failedContainer, err)
	}
	defer stream.Close()

	data, err := io.ReadAll(io.LimitReader(stream, int64(b.maxLogBytes)))
	if err != nil {
		return fmt.Sprintf("(failed to read logs: %v)", err)
	}
	return string(data)
}

func (b *Builder) cleanupJob(ctx context.Context, name string) {
	bg := metav1.DeletePropagationBackground
	if err := b.k8sClient.BatchV1().Jobs(buildNamespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil {
		log.Printf("cleanup job %s: %v", name, err)
	}
}

func resourcePtr(q resource.Quantity) *resource.Quantity {
	return &q
}
