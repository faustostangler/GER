# ==========================================
# GITHUB ACTIONS CI/CD PIPELINE - FASE 1
# ==========================================
# Este script está no nível .github/workflows/ci.yml
# Simulação da provisionamento da AWS via Terraform

provider "aws" {
  region = "sa-east-1"
}

# --- S3: OBJECT STORAGE (Stateless Analytics) ---
resource "aws_s3_bucket" "gercon_data_lake" {
  bucket = "gercon-enterprise-data-lake-prod"
}

resource "aws_s3_bucket_public_access_block" "gercon_s3_block" {
  bucket                  = aws_s3_bucket.gercon_data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- EKS KUBERNETES CLUSTER ---
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = "gercon-k8s-prod"
  cluster_version = "1.30"

  cluster_endpoint_public_access  = true

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_subnets
  control_plane_subnet_ids = module.vpc.intra_subnets

  # Node Groups: Minimalist worker nodes (FinOps)
  eks_managed_node_groups = {
    analytics_nodes = {
      min_size     = "1"
      max_size     = "3"
      desired_size = "2"
      instance_types = ["t3.medium"]
      capacity_type  = "SPOT"
      
      tags = { ExtraTag = "StatelessWorkloads" }
    }
  }

  enable_irsa = true
}

# --- IAM Least Privilege for K8s Service Accounts (IRSA) ---
module "s3_irsa_role" {
  source    = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  role_name = "gercon-s3-rw-role"

  oidc_providers = {
    ex = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["gercon:gercon-worker-sa"]
    }
  }

  role_policy_arns = {
    policy = aws_iam_policy.s3_access.arn
  }
}

resource "aws_iam_policy" "s3_access" {
  name        = "GerconDataLakeAccess"
  description = "Allows K8s pods to R/W parquets on S3"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.gercon_data_lake.arn,
          "${aws_s3_bucket.gercon_data_lake.arn}/*"
        ]
      },
    ]
  })
}
