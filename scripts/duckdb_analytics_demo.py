#!/usr/bin/env python3
"""
DuckDB Gold-layer analytics demo.

Standalone skill-demonstration script — NOT part of the production
pipeline. It doesn't run continuously, doesn't write anywhere, and isn't
consumed by anything else.

Demonstrates querying the MinIO Gold-layer files directly with DuckDB's
httpfs/S3 extension, with no intermediate ETL or load step. Note: the
Gold layer in this PoC is written as one plain JSON object per record
(see flink-jobs/common/minio_sink.py), not Parquet, so this script reads
JSON directly via DuckDB's read_json_auto() rather than read_parquet().

Run with:
    kubectl port-forward svc/minio -n minio 9000:9000 &
    python3 scripts/duckdb_analytics_demo.py
"""
import os

import duckdb

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "rt-payments")
GOLD_GLOB        = f"s3://{MINIO_BUCKET}/gold/*.json"


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    return con


def print_table(con: duckdb.DuckDBPyConnection, title: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    result = con.execute(sql)
    cols = [d[0] for d in result.description]
    rows = result.fetchall()
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c)) for i, c in enumerate(cols)]
    print(" | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(" | ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def main() -> None:
    con = connect()

    con.execute(f"""
        CREATE VIEW gold AS
        SELECT *
        FROM read_json_auto('{GOLD_GLOB}', union_by_name=true, ignore_errors=true)
    """)

    total = con.execute("SELECT COUNT(*) FROM gold").fetchone()[0]
    print(f"Loaded {total} Gold-layer records from {GOLD_GLOB}")

    print_table(con, "Transaction count and total amount by risk score", """
        SELECT
            risk_score,
            COUNT(*)                              AS transaction_count,
            ROUND(SUM(transaction.TRANSACTION_AMOUNT), 2) AS total_amount
        FROM gold
        WHERE risk_score IS NOT NULL
        GROUP BY risk_score
        ORDER BY total_amount DESC
    """)

    print_table(con, "Transaction count by payment channel", """
        SELECT
            payment_channel,
            COUNT(*) AS transaction_count
        FROM gold
        WHERE payment_channel IS NOT NULL
        GROUP BY payment_channel
        ORDER BY transaction_count DESC
    """)

    print_table(con, "Top 5 issuing banks by transaction volume", """
        SELECT
            transaction.ISSUING_BANK              AS issuing_bank,
            COUNT(*)                              AS transaction_count,
            ROUND(SUM(transaction.TRANSACTION_AMOUNT), 2) AS total_amount
        FROM gold
        WHERE transaction.ISSUING_BANK IS NOT NULL
        GROUP BY transaction.ISSUING_BANK
        ORDER BY transaction_count DESC
        LIMIT 5
    """)

    print_table(con, "Average transaction amount by card type and risk score", """
        SELECT
            transaction.CARD_TYPE                  AS card_type,
            risk_score,
            COUNT(*)                                AS transaction_count,
            ROUND(AVG(transaction.TRANSACTION_AMOUNT), 2) AS avg_amount
        FROM gold
        WHERE transaction.CARD_TYPE IS NOT NULL AND risk_score IS NOT NULL
        GROUP BY transaction.CARD_TYPE, risk_score
        ORDER BY card_type, risk_score
    """)


if __name__ == "__main__":
    main()
