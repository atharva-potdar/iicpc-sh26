output "kubeconfig_command" {
  description = "Command to update local kubeconfig for the EKS cluster"
  value       = "aws eks update-kubeconfig --name ${var.cluster_name} --region ${var.region}"
}

# The registry_url and cluster_endpoint outputs will be populated when registry.tf and cluster.tf are added.

data "aws_caller_identity" "current" {}

output "registry_url" {
  description = "Base URL for the ECR registry"
  value       = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com"
}

output "cluster_endpoint" {
  description = "Endpoint for EKS control plane"
  value       = aws_eks_cluster.main.endpoint
}

output "cluster_security_group_id" {
  description = "Security group ids attached to the cluster control plane"
  value       = aws_eks_cluster.main.vpc_config[0].cluster_security_group_id
}
