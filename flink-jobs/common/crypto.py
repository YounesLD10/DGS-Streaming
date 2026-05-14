"""
Fernet encryption/decryption helpers.

Fernet guarantees authenticated symmetric encryption.
The key must be a 32-byte, url-safe base64-encoded string — typically
generated with cryptography.fernet.Fernet.generate_key().
"""
from cryptography.fernet import Fernet, InvalidToken


def make_fernet(key: str) -> Fernet:
    """Return a Fernet instance from a key string or bytes."""
    raw = key.encode("utf-8") if isinstance(key, str) else key
    return Fernet(raw)


def fernet_decrypt(f: Fernet, token: str) -> str:
    """Decrypt a Fernet token and return the plaintext as a UTF-8 string.

    Raises InvalidToken if the token is malformed or the key is wrong.
    """
    return f.decrypt(token.encode("utf-8")).decode("utf-8")
