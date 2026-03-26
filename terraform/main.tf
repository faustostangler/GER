# ==========================================
# PROVISIONAMENTO IAC TERRAFORM COM REMOTE BACKEND
# ==========================================
terraform {
  required_version = ">= 1.5.0"
  
  # === CONFIGURAÇÃO DO STATE LOCKING DISTRIBUÍDO SRE ===
  backend "s3" {
    bucket         = "gercon-terraform-states-prod"
    key            = "gercon-analytics/eks-cluster.tfstate"
    region         = "sa-east-1"
    encrypt        = true
    dynamodb_table = "gercon-terraform-locks" # Heartbeat Lock
  }
}

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

# --- EKS KUBERNETES CLUSTER (Placeholder para Módulos) ---
# module "eks" { ... }
# module "s3_irsa_role" { ... }
# Configurações de pods K8s via Service Accounts atrelados às roles de IAM da AWS para Acesso Nativo ao S3 (Sem Senhas)
