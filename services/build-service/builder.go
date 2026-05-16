package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strconv"
	"strings"
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
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	buildNamespace     = "builds"
	srcBucket          = "submissions"
	binaryBucket       = "builds"
	maxBinarySizeBytes = 50 * 1024 * 1024 // 50MB
)

var buildImages = map[string]string{
	"cpp":  "gcc:16-trixie",
	"rust": "rust:1.95-alpine",
	"go":   "golang:1.26-alpine",
}

// buildCommands returns the shell command to build in /workspace and produce /workspace/binary.
// All binaries are statically linked for portability under gVisor.
// C++ contestants use header-only libraries; we compile main.cpp directly.
// Rust uses cargo with --offline for vendored deps.
// Go uses -mod=vendor for vendored deps.
var buildCommands = map[string]string{
	"cpp":  "g++ -static -O2 -o /workspace/binary /workspace/main.cpp",
	"rust": "cd /workspace && RUSTFLAGS=\"-C target-feature=+crt-static\" cargo build --release --offline && cp $(find target/release -maxdepth 1 -type f -perm -111 ! -name '*.d' | head -1) /workspace/binary",
	"go":   "cd /workspace && CGO_ENABLED=0 go build -mod=vendor -o /workspace/binary .",
}

// BuildResult is returned on successful build.
type BuildResult struct {
	BinaryPath string
}

type Builder struct {
	s3Client    *s3.Client
	k8sClient   kubernetes.Interface
	restConfig  *rest.Config
	timeout     time.Duration
	maxLogBytes int
}

func NewBuilder(seaweedfsEndpoint string, timeoutSec, maxLogBytes int) (*Builder, error) {
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
		timeout:     time.Duration(timeoutSec) * time.Second,
		maxLogBytes: maxLogBytes,
	}, nil
}

// Build runs the full build lifecycle for a submission:
//  1. Download source tar.gz from SeaweedFS
//  2. Create a build pod (sleep entrypoint to keep it alive)
//  3. Stream source into the pod and extract
//  4. Execute the language-specific build command
//  5. Extract the binary and upload to SeaweedFS
//  6. Cleanup the pod
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

	// 1. Download source from SeaweedFS
	source, err := b.downloadArtifact(ctx, event.ArtifactPath)
	if err != nil {
		return nil, fmt.Errorf("download source: %w", err)
	}
	slog.Info("downloaded source", "bytes", len(source))

	// 2. Create build pod
	podName := fmt.Sprintf("build-%s", event.SubmissionID)
	pod, err := b.createBuildPod(ctx, podName, image)
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	defer b.cleanupPod(cleanupCtx, podName)

	// Wait for pod to be running
	if err := b.waitForPodRunning(ctx, pod.Name); err != nil {
		return nil, fmt.Errorf("wait for pod: %w", err)
	}
	slog.Info("build pod running", "pod", podName)

	// 3. Stream source into pod and extract
	if err := b.injectSource(ctx, podName, source); err != nil {
		return nil, fmt.Errorf("inject source: %w", err)
	}

	// 4. Execute build
	buildStderr, err := b.execBuild(ctx, podName, buildCmd)
	if err != nil {
		reason := fmt.Sprintf("build error: %s", buildStderr)
		if len(reason) > b.maxLogBytes {
			reason = reason[:b.maxLogBytes]
			for len(reason) > 0 && !utf8.FullRuneInString(reason[len(reason)-1:]) && !utf8.ValidString(reason) {
				reason = reason[:len(reason)-1]
			}
		}
		return nil, fmt.Errorf("%s", reason)
	}
	slog.Info("build succeeded", "pod", podName)

	// 5. Extract binary and upload
	binaryPath := fmt.Sprintf("builds/%s/binary", event.SubmissionID)
	if err := b.extractAndUploadBinary(ctx, podName, binaryPath); err != nil {
		return nil, fmt.Errorf("extract binary: %w", err)
	}

	return &BuildResult{BinaryPath: binaryPath}, nil
}

func (b *Builder) downloadArtifact(ctx context.Context, key string) ([]byte, error) {
	out, err := b.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer func() {
		if err := out.Body.Close(); err != nil {
			slog.Error("s3 body close error", "error", err)
		}
	}()
	return io.ReadAll(out.Body)
}

func (b *Builder) createBuildPod(ctx context.Context, name, image string) (*corev1.Pod, error) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: buildNamespace,
			Labels: map[string]string{
				"app":  "build",
				"role": "build-pod",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:       "build",
					Image:      image,
					Command:    []string{"sh", "-c", "trap 'exit 0' TERM; sleep infinity & wait $!"},
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
	}

	created, err := b.k8sClient.CoreV1().Pods(buildNamespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}
	return created, nil
}

func (b *Builder) waitForPodRunning(ctx context.Context, name string) error {
	watcher, err := b.k8sClient.CoreV1().Pods(buildNamespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", name),
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
		case corev1.PodRunning:
			return nil
		case corev1.PodFailed, corev1.PodSucceeded:
			return fmt.Errorf("pod terminated with phase %s", pod.Status.Phase)
		}
	}
	return fmt.Errorf("watch closed before pod became running")
}

// validateTarGz checks if the tarball contains malicious paths or symlinks
func validateTarGz(tarGz []byte) error {
	gz, err := gzip.NewReader(bytes.NewReader(tarGz))
	if err != nil {
		return fmt.Errorf("invalid gzip: %w", err)
	}
	defer func() {
		if err := gz.Close(); err != nil {
			slog.Error("gzip reader close error", "error", err)
		}
	}()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read error: %w", err)
		}

		if hdr.Typeflag == tar.TypeSymlink || hdr.Typeflag == tar.TypeLink {
			return fmt.Errorf("links not allowed: %s", hdr.Name)
		}

		cleanPath := path.Clean(hdr.Name)
		if path.IsAbs(cleanPath) || strings.HasPrefix(cleanPath, "../") || cleanPath == ".." {
			return fmt.Errorf("malicious path detected: %s", hdr.Name)
		}
	}
	return nil
}

// injectSource streams the tar.gz into the pod and extracts it to /workspace.
func (b *Builder) injectSource(ctx context.Context, podName string, tarGz []byte) error {
	if err := validateTarGz(tarGz); err != nil {
		return fmt.Errorf("invalid archive: %w", err)
	}

	cmd := []string{"tar", "xzf", "-", "-C", "/workspace"}
	var stdout, stderr bytes.Buffer

	if err := b.execInPod(ctx, podName, cmd, bytes.NewReader(tarGz), &stdout, &stderr); err != nil {
		return fmt.Errorf("extract source: %s: %w", stderr.String(), err)
	}
	return nil
}

// execBuild runs the build command and returns stderr output.
func (b *Builder) execBuild(ctx context.Context, podName, buildCmd string) (string, error) {
	cmd := []string{"sh", "-c", buildCmd}
	var stdout, stderr bytes.Buffer

	if err := b.execInPod(ctx, podName, cmd, nil, &stdout, &stderr); err != nil {
		return stderr.String(), err
	}
	return stderr.String(), nil
}

// extractAndUploadBinary reads the binary from the pod and uploads it to SeaweedFS.
func (b *Builder) extractAndUploadBinary(ctx context.Context, podName, binaryPath string) error {
	// Check binary exists and get its size
	var sizeOut, sizeErr bytes.Buffer
	if err := b.execInPod(
		ctx, podName,
		[]string{"stat", "-c", "%s", "/workspace/binary"},
		nil, &sizeOut, &sizeErr,
	); err != nil {
		return fmt.Errorf("binary not found: %s: %w", sizeErr.String(), err)
	}

	binarySize, err := strconv.ParseInt(strings.TrimSpace(sizeOut.String()), 10, 64)
	if err != nil {
		return fmt.Errorf("parse binary size: %w", err)
	}
	if binarySize > maxBinarySizeBytes {
		return fmt.Errorf("binary too large: %d bytes (max %d)", binarySize, maxBinarySizeBytes)
	}

	// Read the binary
	var binaryBuf bytes.Buffer
	var readErr bytes.Buffer
	if err := b.execInPod(
		ctx, podName,
		[]string{"cat", "/workspace/binary"},
		nil, &binaryBuf, &readErr,
	); err != nil {
		return fmt.Errorf("read binary: %s: %w", readErr.String(), err)
	}

	binaryData := binaryBuf.Bytes()
	if len(binaryData) > maxBinarySizeBytes {
		return fmt.Errorf("binary too large: %d bytes (max %d)", len(binaryData), maxBinarySizeBytes)
	}

	// Upload to SeaweedFS
	_, err = b.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(binaryBucket),
		Key:           aws.String(binaryPath),
		Body:          bytes.NewReader(binaryData),
		ContentLength: aws.Int64(int64(len(binaryData))),
	})
	if err != nil {
		return fmt.Errorf("upload binary: %w", err)
	}
	slog.Info("uploaded binary", "path", binaryPath, "bytes", len(binaryData))
	return nil
}

// execInPod executes a command in the build container of the given pod.
func (b *Builder) execInPod(
	ctx context.Context,
	podName string,
	command []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	req := b.k8sClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(buildNamespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "build",
			Command:   command,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(b.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("create executor: %w", err)
	}

	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	})
}

func (b *Builder) cleanupPod(ctx context.Context, name string) {
	if err := b.k8sClient.CoreV1().Pods(buildNamespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		slog.Error("cleanup pod", "pod", name, "error", err)
	}
}

func resourcePtr(q resource.Quantity) *resource.Quantity {
	return &q
}
