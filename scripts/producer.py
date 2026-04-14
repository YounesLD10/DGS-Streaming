#!/usr/bin/env python3
"""
Producer POC — lit le CSV Kaggle et publie sur le topic Kafka 'payments'
Chaque message est chiffré en base64 (simulation AES) et encodé en JSON.

Usage :
  pip install kafka-python pandas cryptography
  python producer.py --csv <fichier.csv> [--bootstrap <host:port>] [--rate <msg/s>]
"""
import argparse, base64, json, sys, time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from cryptography.fernet import Fernet

# Clé de chiffrement fixe pour le POC (à remplacer par KMS en prod)
_FERNET_KEY = b"dGhpcyBpcyBhIDMyLWJ5dGUga2V5AAAAAAAAAAAAA=="  # 32-byte placeholder
_fernet = Fernet(Fernet.generate_key())  # clé aléatoire par run en POC

def encrypt_payload(data: dict) -> str:
    raw = json.dumps(data).encode()
    return base64.b64encode(_fernet.encrypt(raw)).decode()

def build_envelope(row: dict, idx: int) -> dict:
    return {
        "eventId":   f"evt-{idx:08d}",
        "table":     "payments",
        "operation": "INSERT",
        "payload":   encrypt_payload(row),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    }

def main():
    parser = argparse.ArgumentParser(description="Kafka Payment Producer")
    parser.add_argument("--csv",       required=True,           help="Chemin vers le CSV Kaggle")
    parser.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap server")
    parser.add_argument("--topic",     default="payments",      help="Topic Kafka cible")
    parser.add_argument("--rate",      type=float, default=10,  help="Messages par seconde")
    parser.add_argument("--limit",     type=int,   default=0,   help="Limite de lignes (0=toutes)")
    args = parser.parse_args()

    print(f"[Producer] Lecture de {args.csv}")
    df = pd.read_csv(args.csv)
    if args.limit > 0:
        df = df.head(args.limit)
    print(f"[Producer] {len(df)} lignes chargées")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    for i, row in df.iterrows():
        envelope = build_envelope(row.to_dict(), i)
        producer.send(args.topic, value=envelope)
        sent += 1
        if sent % 100 == 0:
            print(f"[Producer] {sent}/{len(df)} messages envoyés")
        if interval > 0:
            time.sleep(interval)

    producer.flush()
    print(f"[Producer] ✓ {sent} messages publiés sur '{args.topic}'")

if __name__ == "__main__":
    main()
