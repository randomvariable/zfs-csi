# Copyright 2026 Naadir Jeewa
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

variable "aws_region" {
  description = "AWS region where Tekton AWS E2E resources are located."
  type        = string
}

variable "ecr_repository_arns" {
  description = "ECR repository ARNs used by Tekton AWS E2E pipelines."
  type        = set(string)
}

variable "include_orphan_discovery_permissions" {
  description = "Allow read-only resource discovery used to find orphaned E2E resources."
  type        = bool
  default     = true
}

data "aws_iam_policy_document" "tekton_aws_e2e" {
  # CAPA provisioning and reconciliation permissions use a separate CAPA identity.
  statement {
    sid       = "ECRAuthorizationToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    sid    = "ECRImagePushPullInspect"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:CompleteLayerUpload",
      "ecr:DescribeImages",
      "ecr:DescribeRepositories",
      "ecr:GetDownloadUrlForLayer",
      "ecr:InitiateLayerUpload",
      "ecr:ListImages",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
    ]
    resources = tolist(var.ecr_repository_arns)

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    sid       = "CallerIdentity"
    effect    = "Allow"
    actions   = ["sts:GetCallerIdentity"]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = var.include_orphan_discovery_permissions ? [1] : []

    content {
      sid    = "OrphanedResourceDiscovery"
      effect = "Allow"
      actions = [
        "ec2:DescribeInstances",
        "ec2:DescribeVolumes",
        "tag:GetResources",
      ]
      resources = ["*"]

      condition {
        test     = "StringEquals"
        variable = "aws:RequestedRegion"
        values   = [var.aws_region]
      }
    }
  }
}
