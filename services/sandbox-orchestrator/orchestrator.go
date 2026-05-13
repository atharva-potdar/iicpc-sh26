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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	seaweedfsEndpoint string
	s3Client       *s3.Client
	k8sClient      kubernetes.Interface
	restConfig     *rest.Config
	timeout        time.Duration
	maxLogBytes    int
}

func NewOrchestrator(seaweedfsEndpoint string, timeoutSec, maxLogBytes int) (*Orchestrator, error) {
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
		seaweedfsEndpoint: seaweedfsEndpoint,
		s3Client:       s3Client,
		k8sClient:      k8s,
		restConfig:     restCfg,
		timeout:        time.Duration(timeoutSec) * time.Second,
		maxLogBytes:    maxLogBytes,
	}, nil
}

// Deploy runs the full sandbox deployment lifecycle for a submission:
//  1. Create a sandbox pod (gVisor) with an InitContainer
//  2. InitContainer downloads binary from SeaweedFS
//  3. Main container executes the binary directly
//  4. Wait for pod to become Running and Ready
//  5. Return pod info on success, cleanup on failure
func (o *Orchestrator) Deploy(ctx context.Context, event BuildCompleteEvent) (*DeployResult, error) {
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	// 1. Create sandbox pod
	podName := fmt.Sprintf("sandbox-%s", event.SubmissionID)
	pod, err := o.createSandboxPod(ctx, podName, event.BinaryPath)
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

	// Wait for pod to be running and ready
	if err := o.waitForPodRunning(ctx, pod.Name); err != nil {
		logs := o.collectPodLogs(context.Background(), podName)
		reason := fmt.Sprintf("wait for pod failed: %v\n\npod logs:\n%s", err, logs)
		if len(reason) > o.maxLogBytes {
			reason = reason[:o.maxLogBytes]
		}
		return nil, fmt.Errorf("%s", reason)
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
	log.Printf("sandbox pod running and ready: %s (ip=%s)", podName, podIP)

	success = true
	return &DeployResult{
		PodName: podName,
		PodIP:   podIP,
	}, nil
}

func (o *Orchestrator) createSandboxPod(ctx context.Context, name string, binaryPath string) (*corev1.Pod, error) {
	automount := false
	gvisorRuntime := "gvisor"
	binaryUrl := fmt.Sprintf("%s/%s/%s", o.seaweedfsEndpoint, binaryBucket, binaryPath)

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
			InitContainers: []corev1.Container{
				{
					Name:    "init-download",
					Image:   "alpine:3.23",
					Command: []string{"sh", "-c", fmt.Sprintf("wget -qO /sandbox/binary %s && chmod +x /sandbox/binary", binaryUrl)},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "sandbox", MountPath: "/sandbox"},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:       "sandbox",
					Image:      sandboxImage,
					Command:    []string{"/sandbox/binary"},
					WorkingDir: "/sandbox",
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: int32(httpPort)},
						{Name: "ws", ContainerPort: int32(wsPort)},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "sandbox", MountPath: "/sandbox"},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/healthz",
								Port: intstr.FromInt32(httpPort),
							},
						},
						InitialDelaySeconds: 1,
						PeriodSeconds:       2,
						FailureThreshold:    3,
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
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
		
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return fmt.Errorf("pod terminated with phase %s", pod.Status.Phase)
		}

		if pod.Status.Phase == corev1.PodRunning {
			// Check if pod is ready
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
		}
	}
	return fmt.Errorf("watch closed before pod became running and ready")
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

func (o *Orchestrator) cleanupPod(ctx context.Context, name string) {
	if err := o.k8sClient.CoreV1().Pods(sandboxNamespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		log.Printf("cleanup pod %s: %v", name, err)
	}
}

func resourcePtr(q resource.Quantity) *resource.Quantity {
	return &q
}
