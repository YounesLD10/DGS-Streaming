"""
Fernet symmetric encryption for sensitive payment fields.

In production this would be backed by a KMS / HSM. For the POC, the key is
loaded from an environment variable (FERNET_KEY) provisioned via a Kubernetes
Secret. The same Secret is mounted by both the producer and Job 1.
"""
from __future__ import annotations
import base64
import json
import os
from typing import Any

from cryptography.fernet import Fernet


def get_fernet() -> Fernet:
    key = os.environ.get("FERNET_KEY")
    if not key:
        raise RuntimeError("FERNET_KEY env var not set")
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt_dict(payload: dict[str, Any]) -> str:
    """Serialize → encrypt → base64."""
    f = get_fernet()
    raw = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")
    return base64.b64encode(f.encrypt(raw)).decode("ascii")


def decrypt_dict(token: str) -> dict[str, Any]:
    """base64 → decrypt → deserialize."""
    f = get_fernet()
    cipher = base64.b64decode(token.encode("ascii"))
    raw = f.decrypt(cipher)
    return json.loads(raw.decode("utf-8"))
