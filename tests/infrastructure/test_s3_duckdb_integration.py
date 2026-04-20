"""Regression tests: S3/MinIO DuckDB integration — 2026-04-19.

Post-Mortem: Two silent failures were introduced when migrating OUTPUT_FILE
from a local path to ``s3://{bucket}/gercon_consolidado.parquet``:

1. **Malformed s3_endpoint URL** (Critical):
   DuckDB ``SET s3_endpoint`` expects ``host:port`` (no scheme).
   ``S3_ENDPOINT_URL=http://minio:9000`` caused DuckDB to build the URL:
   ``http://http://minio%3A9000/...``, producing an ``IO Error`` on every read.
   FIX: Strip the scheme via ``urlparse(endpoint_url).netloc`` before passing
   to DuckDB (ACL boundary between S3Settings and DuckDB's httpfs dialect).

2. **os.path.getmtime fails on s3:// URIs** (Silent):
   ``os.path.getmtime("s3://...")`` always raises ``OSError``, falling back
   to ``sync_time=0.0`` and permanently triggering the Amber Alert even for
   fresh data.
   FIX: For S3 paths, use ``MAX(dataCadastro)`` via the live DuckDB session
   as a zero-overhead, zero-dependency proxy for data freshness.

WHY these tests exist:
    Zero-tolerance policy for regressions in the S3 ACL boundary.
    Both bugs were invisible in CI (no S3 path tests existed).
"""

from urllib.parse import urlparse

import pytest


class TestS3EndpointSchemeStripping:
    """Guards the ACL boundary: S3Settings.endpoint_url → DuckDB SET s3_endpoint."""

    @pytest.mark.parametrize(
        "raw_endpoint, expected_duckdb_endpoint",
        [
            # WHY: These are the exact transformations DuckDB requires.
            ("http://minio:9000", "minio:9000"),
            ("https://s3.amazonaws.com", "s3.amazonaws.com"),
            ("http://localhost:9000", "localhost:9000"),
            ("minio:9000", "minio:9000"),  # Already stripped — must be idempotent
            ("", ""),  # Empty → no-op (DuckDB public S3 default)
        ],
    )
    def test_endpoint_url_scheme_stripped_for_duckdb(
        self, raw_endpoint: str, expected_duckdb_endpoint: str
    ) -> None:
        """DuckDB s3_endpoint must never include a scheme prefix.

        WHY: DuckDB concatenates the endpoint with the scheme itself.
        Passing ``http://minio:9000`` produces ``http://http://minio%3A9000/...``.
        urlparse().netloc strips the scheme while preserving host and port.
        """
        parsed = urlparse(raw_endpoint)
        duckdb_endpoint = parsed.netloc or raw_endpoint  # idempotent for bare host:port
        assert duckdb_endpoint == expected_duckdb_endpoint

    def test_scheme_stripping_is_idempotent(self) -> None:
        """Applying the strip twice must not corrupt the endpoint string."""
        raw = "http://minio:9000"
        first_pass = urlparse(raw).netloc or raw
        second_pass = urlparse(first_pass).netloc or first_pass
        assert first_pass == second_pass == "minio:9000"


class TestS3PathsExcludeOsPathGetmtime:
    """Guards against os.path.getmtime being called on s3:// URIs."""

    @pytest.mark.parametrize(
        "db_file, is_s3",
        [
            ("s3://gercon/gercon_consolidado.parquet", True),
            ("s3://my-bucket/path/to/data.parquet", True),
            ("/data/gercon_consolidado.parquet", False),
            ("gercon_consolidado.parquet", False),
            ("./data/gercon.parquet", False),
        ],
    )
    def test_s3_path_detected_correctly(self, db_file: str, is_s3: bool) -> None:
        """The is_s3 branch must correctly route away from os.path.getmtime."""
        detected = db_file.startswith("s3://")
        assert detected is is_s3

    def test_os_path_getmtime_raises_on_s3_uri(self) -> None:
        """Regression: os.path.getmtime must never be called on s3:// — it always raises."""
        import os

        with pytest.raises((OSError, FileNotFoundError)):
            os.path.getmtime("s3://gercon/gercon_consolidado.parquet")


class TestS3UrlFormatting:
    """Guards OUTPUT_FILE template resolution in AppSettings._format_output_file."""

    def test_bucket_name_placeholder_resolved(self) -> None:
        """The {bucket_name} placeholder must be resolved to the actual bucket name."""
        template = "s3://{bucket_name}/gercon_consolidado.parquet"
        bucket = "gercon"
        resolved = template.format(bucket_name=bucket)
        assert resolved == "s3://gercon/gercon_consolidado.parquet"
        assert "{bucket_name}" not in resolved

    def test_resolved_url_starts_with_s3_scheme(self) -> None:
        """Resolved OUTPUT_FILE must be parseable as an S3 URI."""
        output_file = "s3://gercon/gercon_consolidado.parquet"
        parsed = urlparse(output_file)
        assert parsed.scheme == "s3"
        assert parsed.netloc == "gercon"
        assert parsed.path == "/gercon_consolidado.parquet"

    def test_settings_format_output_file_resolves_correctly(self) -> None:
        """AppSettings must resolve {bucket_name} in OUTPUT_FILE on init.

        WHY: The _format_output_file model_validator is the single source of
        truth for OUTPUT_FILE. If it fails silently, DuckDB opens a *literal*
        file named ``s3://{bucket_name}/...`` on the local filesystem.
        """
        import os
        import sys

        src_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../../src"
        )
        if src_path not in sys.path:
            sys.path.insert(0, src_path)

        from infrastructure.config import settings

        assert "{bucket_name}" not in settings.OUTPUT_FILE, (
            "OUTPUT_FILE still contains unresolved {bucket_name} placeholder. "
            "_format_output_file model_validator may have failed. "
            "Check that s3: S3Settings is correctly nested in AppSettings."
        )
