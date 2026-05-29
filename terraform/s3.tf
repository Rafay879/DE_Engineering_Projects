# ── Bronze ────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project}-bronze"
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ── Silver ────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "silver" {
  bucket = "${var.project}-silver"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ── Gold ──────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "gold" {
  bucket = "${var.project}-gold"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ── Athena Results ────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "athena_result" {
  bucket = "${var.project}-athena-result"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_result" {
  bucket = aws_s3_bucket.athena_result.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}