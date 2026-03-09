"""AES-256-GCM decryption mirroring app/encryption.py."""

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class EncryptionManager:
    """Decrypt values encrypted by the main BusyBridge app."""

    def __init__(self, key: bytes):
        if len(key) < 32:
            raise ValueError("Encryption key must be at least 32 bytes")
        self._aesgcm = AESGCM(key[:32])

    def decrypt(self, encrypted_data: bytes) -> str:
        """Decrypt bytes (nonce prefix + ciphertext) to string."""
        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]
        return self._aesgcm.decrypt(nonce, ciphertext, None).decode("utf-8")
