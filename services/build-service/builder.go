package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type limitWriter struct {
	w     io.Writer
	limit int
	n     int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if w.n >= w.limit {
		return len(p), nil
	}
	if w.n+len(p) > w.limit {
		p = p[:w.limit-w.n]
	}
	n, err := w.w.Write(p)
	w.n += n
	return len(p), err
}

const (
	buildNamespace = "builds"
	srcBucket      = "submissions"
	binaryBucket   = "builds"
)

var buildImages = map[string]string{
	"cpp":  "gcc:16-trixie",
	"rust": "rust:1.95-alpine",
	"go":   "golang:1.26-alpine",
}

// buildCommands returns the shell command to build in /workspace, produce /workspace/binary,
// and upload the binary to the pre-signed PUT URL.
var buildCommands = map[string]string{
	"cpp":  `g++ -static -O2 -o /workspace/binary /workspace/main.cpp && wget -q --method=PUT --body-file=/workspace/binary "$BINARY_UPLOAD_URL"`,
	"rust": `cd /workspace && RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --offline && cp $(find target/release -maxdepth 1 -type f -perm -111 ! -name '*.d' | head -1) /workspace/binary && wget -q --method=PUT --body-file=/workspace/binary "$BINARY_UPLOAD_URL"`,
	"go":   `cd /workspace && CGO_ENABLED=0 go build -mod=vendor -o /workspace/binary . && wget -q --method=PUT --body-file=/workspace/binary "$BINARY_UPLOAD_URL"`,
}

// BuildResult is returned on successful build.
type BuildResult struct {
	BinaryPath string
}

type Builder struct {
	s3Client    *s3.Client
	k8sClient   kubernetes.Interface
	restConfig  *rest.Config
	maxLogBytes int
}

func NewBuilder(seaweedfsEndpoint string, maxLogBytes int) (*Builder, error) {
	// S3 client for SeaweedFS
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

	// K8s client (in-cluster)
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s in-cluster config: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s clientset: %w", err)
	}

	return &Builder{
		s3Client:    s3Client,
		k8sClient:   k8s,
		restConfig:  restCfg,
		maxLogBytes: maxLogBytes,
	}, nil
}

// Build runs the full build lifecycle for a submission:
//  1. Generate pre-signed URL for downloading source tar.gz from SeaweedFS
//  2. Generate pre-signed URL for uploading compiled binary to SeaweedFS
//  3. Create build pod (InitContainer downloads/extracts, Main Container builds/uploads)
//  4. Watch the pod until completion (Success/Failure)
//  5. Read logs on failure, return failure reason
//  6. Cleanup the pod
func (b *Builder) Build(ctx context.Context, event SubmissionCreatedEvent) (*BuildResult, error) {
	image, ok := buildImages[event.Language]
	if !ok {
		return nil, fmt.Errorf("unsupported language: %s", event.Language)
	}
	buildCmd, ok := buildCommands[event.Language]
	if !ok {
		return nil, fmt.Errorf("no build command for language: %s", event.Language)
	}

	// 1. Generate pre-signed GET URL for source
	presignCtx, presignCancel := context.WithTimeout(ctx, 15*time.Second)
	defer presignCancel()
	sourceURL, err := b.GeneratePresignedGetURL(presignCtx, event.ArtifactPath, 15*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("generate presigned source URL: %w", err)
	}

	// 2. Generate pre-signed PUT URL for binary
	binaryPath := fmt.Sprintf("builds/%s/binary", event.SubmissionID)
	binaryUploadURL, err := b.GeneratePresignedPutURL(presignCtx, binaryPath, 15*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("generate presigned binary upload URL: %w", err)
	}

	// 3. Create build pod
	podCtx, podCancel := context.WithTimeout(ctx, 150*time.Second)
	defer podCancel()
	podName := fmt.Sprintf("build-%s", event.SubmissionID)
	pod, err := b.createBuildPod(podCtx, podName, image, buildCmd, sourceURL, binaryUploadURL)
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	defer b.cleanupPod(cleanupCtx, podName)

	// 4. Watch pod to completion
	slog.Info("watching build pod", "pod", podName)
	if err := b.waitForPodCompletion(podCtx, pod.Name, pod.ResourceVersion); err != nil {
		// Read container logs on failure
		logCtx, logCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer logCancel()
		logs, logErr := b.readPodLogs(logCtx, podName)
		if logErr != nil {
			slog.Error("failed to read pod logs", "pod", podName, "error", logErr)
			return nil, fmt.Errorf("build failed: %w", err)
		}
		return nil, fmt.Errorf("build error: %s", logs)
	}

	slog.Info("build succeeded", "pod", podName)
	return &BuildResult{BinaryPath: binaryPath}, nil
}

func (b *Builder) createBuildPod(ctx context.Context, name, image, buildCmd, sourceURL, binaryUploadURL string) (*corev1.Pod, error) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: buildNamespace,
			Labels: map[string]string{
				"app":                    "build",
				"role":                   "build-pod",
				"app.kubernetes.io/name": "build",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			InitContainers: []corev1.Container{
				{
					Name:  "download-source",
					Image: "alpine:3.23",
					Command: []string{"sh", "-c"},
					Args: []string{
						`wget -q -O /workspace/source.tar.gz "$SOURCE_URL" && ` +
							`tar xzf /workspace/source.tar.gz -C /workspace && ` +
							`rm /workspace/source.tar.gz`,
					},
					Env: []corev1.EnvVar{
						{Name: "SOURCE_URL", Value: sourceURL},
					},
					SecurityContext: &corev1.SecurityContext{
						RunAsUser:                ptr[int64](65534),
						RunAsNonRoot:             ptr(true),
						ReadOnlyRootFilesystem:   ptr(true),
						AllowPrivilegeEscalation: ptr(false),
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"ALL"},
						},
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
						AppArmorProfile: &corev1.AppArmorProfile{
							Type: corev1.AppArmorProfileTypeRuntimeDefault,
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
						{Name: "tmp", MountPath: "/tmp"},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:       "build",
					Image:      image,
					Command:    []string{"sh", "-c"},
					Args:       []string{buildCmd},
					WorkingDir: "/workspace",
					Env: []corev1.EnvVar{
						{Name: "BINARY_UPLOAD_URL", Value: binaryUploadURL},
						{Name: "GOCACHE", Value: "/tmp/go-build-cache"},
						{Name: "GOPATH", Value: "/tmp/go-path"},
					},
					SecurityContext: &corev1.SecurityContext{
						RunAsUser:                ptr[int64](65534),
						RunAsNonRoot:             ptr(true),
						ReadOnlyRootFilesystem:   ptr(true),
						AllowPrivilegeEscalation: ptr(false),
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"ALL"},
						},
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
						AppArmorProfile: &corev1.AppArmorProfile{
							Type: corev1.AppArmorProfileTypeRuntimeDefault,
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
						{Name: "tmp", MountPath: "/tmp"},
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
			Volumes: []corev1.Volume{
				{
					Name: "workspace",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							SizeLimit: resourcePtr(resource.MustParse("512Mi")),
						},
					},
				},
				{
					Name: "tmp",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}

	created, err := b.k8sClient.CoreV1().Pods(buildNamespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}
	return created, nil
}

func (b *Builder) waitForPodCompletion(ctx context.Context, name, resourceVersion string) error {
	watcher, err := b.k8sClient.CoreV1().Pods(buildNamespace).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fmt.Sprintf("metadata.name=%s", name),
		ResourceVersion: resourceVersion,
	})
	if err != nil {
		return fmt.Errorf("watch pod: %w", err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		pod, ok := event.Object.(*corev1.Pod)
		if !ok {
			continue
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			return fmt.Errorf("pod failed")
		}
	}
	return fmt.Errorf("watch closed before pod completed")
}

func (b *Builder) readPodLogs(ctx context.Context, name string) (string, error) {
	req := b.k8sClient.CoreV1().Pods(buildNamespace).GetLogs(name, &corev1.PodLogOptions{
		Container: "build",
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("get logs stream: %w", err)
	}
	defer stream.Close()

	var buf bytes.Buffer
	w := &limitWriter{w: &buf, limit: b.maxLogBytes}
	if _, err := io.Copy(w, stream); err != nil {
		return "", fmt.Errorf("read logs: %w", err)
	}

	reason := buf.String()
	for len(reason) > 0 && !utf8.FullRuneInString(reason[len(reason)-1:]) && !utf8.ValidString(reason) {
		reason = reason[:len(reason)-1]
	}
	return reason, nil
}

func (b *Builder) cleanupPod(ctx context.Context, name string) {
	if err := b.k8sClient.CoreV1().Pods(buildNamespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		slog.Error("cleanup pod", "pod", name, "error", err)
	}
}

func resourcePtr(q resource.Quantity) *resource.Quantity {
	return &q
}

func ptr[T any](v T) *T {
	return &v
}
