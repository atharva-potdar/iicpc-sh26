variable "region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  default     = "iicpc-platform"
}

variable "k8s_version" {
  description = "Kubernetes version"
  default     = "1.31"
}

# Node group sizing — three separate groups for workload isolation

variable "platform_instance_type" {
  description = "Instance type for the general platform services"
  default     = "m5.xlarge"
}

variable "platform_min" {
  description = "Minimum nodes for platform group"
  default     = 2
}

variable "platform_max" {
  description = "Maximum nodes for platform group"
  default     = 10
}

variable "sandbox_instance_type" {
  description = "Instance type for sandbox workloads (needs dedicated cores for CPU pinning)"
  default     = "c5.2xlarge"
}

variable "sandbox_min" {
  description = "Minimum nodes for sandbox group"
  default     = 1
}

variable "sandbox_max" {
  description = "Maximum nodes for sandbox group"
  default     = 10
}

variable "bots_instance_type" {
  description = "Instance type for load generation bots (high network I/O)"
  default     = "c5n.xlarge"
}

variable "bots_min" {
  description = "Minimum nodes for bots group (scales to zero between runs)"
  default     = 0
}

variable "bots_max" {
  description = "Maximum nodes for bots group"
  default     = 20
}
