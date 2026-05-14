"""
MinIO write utilities for HPS pipeline jobs.

Each record is written as an individual JSON object.  The object key
encodes a UTC timestamp and a short UUID fragment so concurrent
parallel task instances never collide on the same key.
"""
import io
import json
import uuid
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error


def get_minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    """Create a Minio client from an http(s):// endpoint string."""
    secure = endpoint.startswith("https://")
    host = endpoint.replace("https://", "").replace("http://", "")
    return Minio(host, access_key=access_key, secret_key=secret_key, secure=secure)


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Create the bucket if it does not already exist."""
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    except S3Error as exc:
        # BucketAlreadyOwnedByYou is harmless in concurrent scenarios
        if exc.code != "BucketAlreadyOwnedByYou":
            raise


def write_record(
    client: Minio,
    bucket: str,
    prefix: str,
    record: dict,
) -> str:
    """Serialise *record* as JSON and write it to MinIO.

    Returns the object key that was written.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    uid = uuid.uuid4().hex[:8]
    key = f"{prefix}/{ts}_{uid}.json"
    data = json.dumps(record, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    return key
