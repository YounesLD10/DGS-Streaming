"""
Parquet writer for MinIO as a passthrough MapFunction.
Writes records to MinIO as Parquet (side effect) and returns the value unchanged
so the stream can continue to its Kafka sink. Uses lazy initialization so the
PyArrow S3FileSystem is created inside the worker process after deserialization.
Not checkpoint-aware — PoC only.
"""
from __future__ import annotations
import os
import uuid

from pyflink.datastream import MapFunction


class ParquetWriterFn(MapFunction):
    def __init__(self, bucket: str, prefix: str, batch_size: int = 100):
        self._bucket = bucket
        self._prefix = prefix
        self._batch_size = batch_size
        self._buffer: list | None = None
        self._fs = None

    def map(self, value: str) -> str:
        if self._buffer is None:
            self._buffer = []
        if self._fs is None:
            self._init_fs()
        self._buffer.append(value)
        if len(self._buffer) >= self._batch_size:
            self._flush()
        return value  # pass the record through unchanged

    def _init_fs(self):
        import pyarrow.fs as pafs
        raw = os.environ.get("MINIO_ENDPOINT", "minio.stockage.svc:9000")
        endpoint = raw.replace("http://", "").replace("https://", "")
        scheme = "https" if raw.startswith("https://") else "http"
        self._fs = pafs.S3FileSystem(
            endpoint_override=endpoint,
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            scheme=scheme,
        )

    def _flush(self):
        if not self._buffer:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.table({"json_data": pa.array(self._buffer, type=pa.large_utf8())})
        path = f"{self._bucket}/{self._prefix}/part-{uuid.uuid4().hex}.parquet"
        pq.write_table(table, path, filesystem=self._fs)
        self._buffer.clear()
