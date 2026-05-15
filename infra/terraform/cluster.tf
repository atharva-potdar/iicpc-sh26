# IAM Role for EKS Cluster
resource "aws_iam_role" "cluster" {
  name = "${var.cluster_name}-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cluster_AmazonEKSClusterPolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.cluster.name
}

# IAM Role for EKS Node Groups
resource "aws_iam_role" "node" {
  name = "${var.cluster_name}-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "node_AmazonEKSWorkerNodePolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.node.name
}

resource "aws_iam_role_policy_attachment" "node_AmazonEKS_CNI_Policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.node.name
}

resource "aws_iam_role_policy_attachment" "node_AmazonEC2ContainerRegistryReadOnly" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.node.name
}

resource "aws_iam_role_policy_attachment" "node_AmazonSSMManagedInstanceCore" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  role       = aws_iam_role.node.name
}

# The EKS Cluster
resource "aws_eks_cluster" "main" {
  name     = var.cluster_name
  role_arn = aws_iam_role.cluster.arn
  version  = var.k8s_version

  vpc_config {
    subnet_ids = aws_subnet.private[*].id
  }

  depends_on = [
    aws_iam_role_policy_attachment.cluster_AmazonEKSClusterPolicy
  ]
}

# Platform Node Group
resource "aws_eks_node_group" "platform" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "platform"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.private[*].id
  ami_type        = "AL2023_x86_64_STANDARD"

  instance_types = [var.platform_instance_type]

  scaling_config {
    desired_size = var.platform_min
    max_size     = var.platform_max
    min_size     = var.platform_min
  }

  labels = {
    workload = "platform"
  }

  taint {
    key    = "workload"
    value  = "platform"
    effect = "NO_SCHEDULE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_AmazonEKSWorkerNodePolicy,
    aws_iam_role_policy_attachment.node_AmazonEKS_CNI_Policy,
    aws_iam_role_policy_attachment.node_AmazonEC2ContainerRegistryReadOnly,
  ]
}

# Bots Node Group
resource "aws_eks_node_group" "bots" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "bots"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.private[*].id
  ami_type        = "AL2023_x86_64_STANDARD"

  instance_types = [var.bots_instance_type]

  scaling_config {
    desired_size = var.bots_min
    max_size     = var.bots_max
    min_size     = var.bots_min
  }

  labels = {
    workload = "bots"
  }

  taint {
    key    = "workload"
    value  = "bots"
    effect = "NO_SCHEDULE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_AmazonEKSWorkerNodePolicy,
    aws_iam_role_policy_attachment.node_AmazonEKS_CNI_Policy,
    aws_iam_role_policy_attachment.node_AmazonEC2ContainerRegistryReadOnly,
  ]
}

# Sandbox Launch Template UserData
data "cloudinit_config" "sandbox" {
  gzip          = false
  base64_encode = true

  part {
    content_type = "application/node.eks.aws"
    content      = <<-EOT
      ---
      apiVersion: node.eks.aws/v1alpha1
      kind: NodeConfig
      spec:
        kubelet:
          config:
            cpuManagerPolicy: static
    EOT
  }

  part {
    content_type = "text/cloud-config"
    content      = <<-EOT
      #cloud-config
      write_files:
        - path: /var/lib/kubelet/seccomp/sandbox-seccomp.json
          permissions: "0644"
          owner: root:root
          content: |
${indent(12, file("${path.module}/files/sandbox-seccomp.json"))}
    EOT
  }
}

resource "aws_launch_template" "sandbox" {
  name_prefix = "${var.cluster_name}-sandbox-"
  user_data   = data.cloudinit_config.sandbox.rendered

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name = "${var.cluster_name}-sandbox"
    }
  }
}

# Sandbox Node Group
resource "aws_eks_node_group" "sandbox" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "sandbox"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.private[*].id
  ami_type        = "AL2023_x86_64_STANDARD"

  launch_template {
    id      = aws_launch_template.sandbox.id
    version = aws_launch_template.sandbox.latest_version
  }

  instance_types = [var.sandbox_instance_type]

  scaling_config {
    desired_size = var.sandbox_min
    max_size     = var.sandbox_max
    min_size     = var.sandbox_min
  }

  labels = {
    workload = "sandbox"
  }

  taint {
    key    = "workload"
    value  = "sandbox"
    effect = "NO_SCHEDULE"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_AmazonEKSWorkerNodePolicy,
    aws_iam_role_policy_attachment.node_AmazonEKS_CNI_Policy,
    aws_iam_role_policy_attachment.node_AmazonEC2ContainerRegistryReadOnly,
  ]
}

# OIDC Provider for IRSA (needed for Cluster Autoscaler etc)
data "tls_certificate" "eks" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
}
