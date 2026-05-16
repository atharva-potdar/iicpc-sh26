package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"regexp"
)

const (
	sandboxNamespace = "sandboxes"
	binaryBucket     = "builds"
	sandboxImage     = "alpine:3.23"
	httpPort         = 8080
)

// DeployResult is returned on successful sandbox deployment.
type DeployResult struct {
	PodName string
	PodIP   string
}

type SandboxConfig struct {
	Timeout        time.Duration
	MaxLogBytes    int
	CpuRequest     string
	CpuLimit       string
	MemoryRequest  string
	MemoryLimit    string
	SeccompProfile string
	RunAsUser      int64
	NodeSelectorK  string
	NodeSelectorV  string
	TolerationK    string
	TolerationV    string
}

type Orchestrator struct {
	seaweedfsEndpoint string
	s3Client          *s3.Client
	k8sClient         kubernetes.Interface
	restConfig        *rest.Config
	cfg               SandboxConfig
	binaryPathRegex   *regexp.Regexp
}

func NewOrchestrator(seaweedfsEndpoint string, cfg SandboxConfig) (*Orchestrator, error) {
	// S3 client for SeaweedFS
	awsCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("any", "any", ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
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
		s3Client:          s3Client,
		k8sClient:         k8s,
		restConfig:        restCfg,
		cfg:               cfg,
		binaryPathRegex:   regexp.MustCompile(`^[A-Za-z0-9/_.-]+$`),
	}, nil
}

// Deploy runs the full sandbox deployment lifecycle for a submission:
//  1. Create a sandbox pod with an InitContainer
//  2. InitContainer downloads binary from SeaweedFS
//  3. Main container executes the binary directly
//  4. Wait for pod to become Running and Ready
//  5. Return pod info on success, cleanup on failure
func (o *Orchestrator) Deploy(ctx context.Context, event BuildCompleteEvent) (*DeployResult, error) {
	ctx, cancel := context.WithTimeout(ctx, o.cfg.Timeout)
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
			// Use context.Background() to ensure cleanup runs even if the main context is cancelled
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			o.cleanupPod(cleanupCtx, podName)
		}
	}()

	// Wait for pod to be running and ready
	if err := o.waitForPodRunning(ctx, pod.Name, pod.ResourceVersion); err != nil {
		// Use context.Background() for diagnostics to ensure we get logs even if timed out
		logsCtx, logsCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer logsCancel()
		logs := o.collectPodLogs(logsCtx, podName)
		reason := fmt.Sprintf("wait for pod failed: %v\n\npod logs:\n%s", err, logs)
		if len(reason) > o.cfg.MaxLogBytes {
			reason = reason[:o.cfg.MaxLogBytes]
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
	slog.Info("sandbox pod running and ready", "pod", podName, "ip", podIP)

	success = true
	return &DeployResult{
		PodName: podName,
		PodIP:   podIP,
	}, nil
}

func (o *Orchestrator) createSandboxPod(ctx context.Context, name string, binaryPath string) (*corev1.Pod, error) {
	if !o.binaryPathRegex.MatchString(binaryPath) {
		return nil, fmt.Errorf("invalid binary path: %s", binaryPath)
	}
	automount := false
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
			AutomountServiceAccountToken: &automount,
			RestartPolicy:                corev1.RestartPolicyNever,
			NodeSelector: map[string]string{
				o.cfg.NodeSelectorK: o.cfg.NodeSelectorV,
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      o.cfg.TolerationK,
					Operator: corev1.TolerationOpEqual,
					Value:    o.cfg.TolerationV,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			InitContainers: []corev1.Container{
				{
					Name:    "init-download",
					Image:   "alpine:3.23",
					Command: []string{"sh", "-c", fmt.Sprintf("wget -qO /sandbox/binary %s && chmod +x /sandbox/binary", binaryUrl)},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "sandbox", MountPath: "/sandbox"},
					},
					SecurityContext: &corev1.SecurityContext{
						RunAsUser:                &o.cfg.RunAsUser,
						RunAsNonRoot:             boolPtr(true),
						AllowPrivilegeEscalation: boolPtr(false),
						ReadOnlyRootFilesystem:   boolPtr(true),
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
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "sandbox", MountPath: "/sandbox"},
						{Name: "tmp", MountPath: "/tmp"},
					},
					SecurityContext: &corev1.SecurityContext{
						RunAsUser:                &o.cfg.RunAsUser,
						RunAsNonRoot:             boolPtr(true),
						AllowPrivilegeEscalation: boolPtr(false),
						ReadOnlyRootFilesystem:   boolPtr(true),
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"ALL"},
						},
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeLocalhost,
							// NOTE: The seccomp profile must exist on the kubelet nodes at
							// /var/lib/kubelet/seccomp/<profile_path>. This is typically
							// managed via Terraform user-data or a DaemonSet.
							LocalhostProfile: &o.cfg.SeccompProfile,
						},
						AppArmorProfile: &corev1.AppArmorProfile{
							Type: corev1.AppArmorProfileTypeRuntimeDefault,
						},
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
							corev1.ResourceCPU:    resource.MustParse(o.cfg.CpuLimit),
							corev1.ResourceMemory: resource.MustParse(o.cfg.MemoryLimit),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(o.cfg.CpuRequest),
							corev1.ResourceMemory: resource.MustParse(o.cfg.MemoryRequest),
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
				{
					Name: "tmp",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
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

func (o *Orchestrator) waitForPodRunning(ctx context.Context, name, resourceVersion string) error {
	watcher, err := o.k8sClient.CoreV1().Pods(sandboxNamespace).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", name).String(),
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
	defer func() {
		if err := stream.Close(); err != nil {
			slog.Error("pod log stream close error", "error", err)
		}
	}()

	data, err := io.ReadAll(io.LimitReader(stream, int64(o.cfg.MaxLogBytes)))
	if err != nil {
		return fmt.Sprintf("(failed to read logs: %v)", err)
	}
	return string(data)
}

func (o *Orchestrator) cleanupPod(ctx context.Context, name string) {
	gracePeriod := int64(5)
	if err := o.k8sClient.CoreV1().Pods(sandboxNamespace).Delete(ctx, name, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}); err != nil {
		slog.Error("cleanup pod", "pod", name, "error", err)
	}
}

func (o *Orchestrator) Close() {
	// S3 client has no explicit Close method; k8s client is stateless.
	// Reserved for future resource cleanup needs.
}

func resourcePtr(q resource.Quantity) *resource.Quantity {
	return &q
}

func boolPtr(b bool) *bool {
	return &b
}
