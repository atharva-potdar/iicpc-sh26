package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

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
	sandboxNamespace = "sandboxes"
	binaryBucket     = "builds"
	sandboxImage     = "alpine:3.23"
	httpPort         = 8080
	wsPort           = 8081
)

// DeployResult is returned on successful sandbox deployment.
type DeployResult struct {
	PodName string
	PodIP   string
}

type Orchestrator struct {
	s3Client       *s3.Client
	k8sClient      kubernetes.Interface
	restConfig     *rest.Config
	timeout        time.Duration
	maxLogBytes    int
	healthInterval time.Duration
	healthRetries  int
}

func NewOrchestrator(seaweedfsEndpoint string, timeoutSec, maxLogBytes int, healthInterval time.Duration, healthRetries int) (*Orchestrator, error) {
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

	return &Orchestrator{
		s3Client:       s3Client,
		k8sClient:      k8s,
		restConfig:     restCfg,
		timeout:        time.Duration(timeoutSec) * time.Second,
		maxLogBytes:    maxLogBytes,
		healthInterval: healthInterval,
		healthRetries:  healthRetries,
	}, nil
}

// Deploy runs the full sandbox deployment lifecycle for a submission:
//  1. Download binary from SeaweedFS
//  2. Create a sandbox pod (gVisor, sleep entrypoint)
//  3. Stream binary into the pod
//  4. Make binary executable and launch it in the background
//  5. Health check via exec (wget inside the pod)
//  6. Return pod info on success, cleanup on failure
func (o *Orchestrator) Deploy(ctx context.Context, event BuildCompleteEvent) (*DeployResult, error) {
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	// 1. Download binary from SeaweedFS
	binary, err := o.downloadBinary(ctx, event.BinaryPath)
	if err != nil {
		return nil, fmt.Errorf("download binary: %w", err)
	}
	log.Printf("downloaded binary: %d bytes", len(binary))

	// 2. Create sandbox pod
	podName := fmt.Sprintf("sandbox-%s", event.SubmissionID)
	pod, err := o.createSandboxPod(ctx, podName)
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}

	// On failure, cleanup the pod. On success, the pod stays alive.
	success := false
	defer func() {
		if !success {
			o.cleanupPod(context.Background(), podName)
		}
	}()

	// Wait for pod to be running
	if err := o.waitForPodRunning(ctx, pod.Name); err != nil {
		return nil, fmt.Errorf("wait for pod: %w", err)
	}

	// Get pod IP
	pod, err = o.k8sClient.CoreV1().Pods(sandboxNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}
	podIP := pod.Status.PodIP
	if podIP == "" {
		return nil, fmt.Errorf("pod has no IP assigned")
	}
	log.Printf("sandbox pod running: %s (ip=%s)", podName, podIP)

	// 3. Stream binary into the pod
	if err := o.injectBinary(ctx, podName, binary); err != nil {
		return nil, fmt.Errorf("inject binary: %w", err)
	}
	log.Printf("binary injected: %s", podName)

	// 4. Make binary executable and launch in background
	if err := o.launchBinary(ctx, podName); err != nil {
		return nil, fmt.Errorf("launch binary: %w", err)
	}
	log.Printf("binary launched: %s", podName)

	// 5. Health check via exec (wget inside the pod)
	if err := o.waitForHealthy(ctx, podName); err != nil {
		// Collect logs for the failure event
		logs := o.collectPodLogs(context.Background(), podName)
		reason := fmt.Sprintf("health check failed: %v\n\npod logs:\n%s", err, logs)
		if len(reason) > o.maxLogBytes {
			reason = reason[:o.maxLogBytes]
		}
		return nil, fmt.Errorf("%s", reason)
	}
	log.Printf("sandbox healthy: %s", podName)

	success = true
	return &DeployResult{
		PodName: podName,
		PodIP:   podIP,
	}, nil
}

func (o *Orchestrator) downloadBinary(ctx context.Context, key string) ([]byte, error) {
	out, err := o.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(binaryBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (o *Orchestrator) createSandboxPod(ctx context.Context, name string) (*corev1.Pod, error) {
	automount := false
	gvisorRuntime := "gvisor"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: sandboxNamespace,
			Labels: map[string]string{
				"app":  "sandbox",
				"role": "contestant-submission",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName:             &gvisorRuntime,
			AutomountServiceAccountToken: &automount,
			RestartPolicy:                corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:       "sandbox",
					Image:      sandboxImage,
					Command:    []string{"sleep", "infinity"},
					WorkingDir: "/sandbox",
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: int32(httpPort)},
						{Name: "ws", ContainerPort: int32(wsPort)},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "sandbox", MountPath: "/sandbox"},
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "sandbox",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							SizeLimit: resourcePtr(resource.MustParse("256Mi")),
						},
					},
				},
			},
		},
	}

	created, err := o.k8sClient.CoreV1().Pods(sandboxNamespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}
	return created, nil
}

func (o *Orchestrator) waitForPodRunning(ctx context.Context, name string) error {
	watcher, err := o.k8sClient.CoreV1().Pods(sandboxNamespace).Watch(ctx, metav1.ListOptions{
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

// injectBinary streams the binary into /sandbox/binary via cat.
func (o *Orchestrator) injectBinary(ctx context.Context, podName string, binary []byte) error {
	cmd := []string{"sh", "-c", "cat > /sandbox/binary"}
	var stdout, stderr bytes.Buffer

	if err := o.execInPod(ctx, podName, cmd, bytes.NewReader(binary), &stdout, &stderr); err != nil {
		return fmt.Errorf("write binary: %s: %w", stderr.String(), err)
	}
	return nil
}

// launchBinary makes the binary executable and starts it in the background.
// Uses nohup to ensure the process survives after the exec session closes.
func (o *Orchestrator) launchBinary(ctx context.Context, podName string) error {
	cmd := []string{"sh", "-c", "chmod +x /sandbox/binary && nohup /sandbox/binary > /sandbox/stdout.log 2>&1 &"}
	var stdout, stderr bytes.Buffer

	if err := o.execInPod(ctx, podName, cmd, nil, &stdout, &stderr); err != nil {
		return fmt.Errorf("launch: %s: %w", stderr.String(), err)
	}
	return nil
}

// waitForHealthy polls /healthz from inside the pod using wget.
// This avoids NetworkPolicy issues — the orchestrator runs in platform
// but the sandboxes namespace only allows ingress from bots.
func (o *Orchestrator) waitForHealthy(ctx context.Context, podName string) error {
	cmd := []string{"wget", "-q", "-O", "/dev/null", "--timeout=2",
		fmt.Sprintf("http://localhost:%d/healthz", httpPort)}

	for i := 0; i < o.healthRetries; i++ {
		var stdout, stderr bytes.Buffer
		err := o.execInPod(ctx, podName, cmd, nil, &stdout, &stderr)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("health check %d/%d failed for %s: %s",
			i+1, o.healthRetries, podName, stderr.String())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.healthInterval):
		}
	}
	return fmt.Errorf("health check failed after %d attempts", o.healthRetries)
}

// collectPodLogs fetches the pod logs for failure diagnostics.
func (o *Orchestrator) collectPodLogs(ctx context.Context, podName string) string {
	req := o.k8sClient.CoreV1().Pods(sandboxNamespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "sandbox",
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Sprintf("(failed to fetch logs: %v)", err)
	}
	defer stream.Close()

	data, err := io.ReadAll(io.LimitReader(stream, int64(o.maxLogBytes)))
	if err != nil {
		return fmt.Sprintf("(failed to read logs: %v)", err)
	}
	return string(data)
}

// execInPod executes a command in the sandbox container of the given pod.
func (o *Orchestrator) execInPod(
	ctx context.Context,
	podName string,
	command []string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	req := o.k8sClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(sandboxNamespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "sandbox",
			Command:   command,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(o.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("create executor: %w", err)
	}

	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	})
}

func (o *Orchestrator) cleanupPod(ctx context.Context, name string) {
	if err := o.k8sClient.CoreV1().Pods(sandboxNamespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		log.Printf("cleanup pod %s: %v", name, err)
	}
}

func resourcePtr(q resource.Quantity) *resource.Quantity {
	return &q
}
