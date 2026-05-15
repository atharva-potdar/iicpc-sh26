output "kubeconfig_command" {
  description = "Command to update local kubeconfig for the EKS cluster"
  value       = "aws eks update-kubeconfig --name ${var.cluster_name} --region ${var.region}"
}

# The registry_url and cluster_endpoint outputs will be populated when registry.tf and cluster.tf are added.
