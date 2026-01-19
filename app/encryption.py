"""Encryption utilities for securing sensitive data."""

import base64
import os
import secrets
from typing import Union

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class EncryptionManager:
    """Handles encryption and decryption of sensitive data using AES-256-GCM."""

    def __init__(self, key: bytes):
        """Initialize with a 32-byte encryption key."""
        if len(key) < 32:
            raise ValueError("Encryption key must be at least 32 bytes")
        self._key = key[:32]  # Use first 32 bytes
        self._aesgcm = AESGCM(self._key)

    def encrypt(self, plaintext: Union[str, bytes]) -> bytes:
        """
        Encrypt plaintext data.

        Args:
            plaintext: Data to encrypt (string or bytes)

        Returns:
            Encrypted data as bytes (nonce + ciphertext)
        """
        if isinstance(plaintext, str):
            plaintext = plaintext.encode("utf-8")

        # Generate a random 12-byte nonce
        nonce = os.urandom(12)

        # Encrypt the data
        ciphertext = self._aesgcm.encrypt(nonce, plaintext, None)

        # Return nonce + ciphertext
        return nonce + ciphertext

    def decrypt(self, encrypted_data: bytes) -> str:
        """
        Decrypt encrypted data.

        Args:
            encrypted_data: Encrypted bytes (nonce + ciphertext)

        Returns:
            Decrypted string
        """
        if len(encrypted_data) < 12:
            raise ValueError("Invalid encrypted data: too short")

        # Extract nonce and ciphertext
        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]

        # Decrypt
        plaintext = self._aesgcm.decrypt(nonce, ciphertext, None)

        return plaintext.decode("utf-8")

    def encrypt_to_base64(self, plaintext: Union[str, bytes]) -> str:
        """Encrypt and return as base64 string."""
        encrypted = self.encrypt(plaintext)
        return base64.b64encode(encrypted).decode("ascii")

    def decrypt_from_base64(self, encrypted_base64: str) -> str:
        """Decrypt from base64 string."""
        encrypted = base64.b64decode(encrypted_base64)
        return self.decrypt(encrypted)


def generate_encryption_key() -> bytes:
    """Generate a new 32-byte encryption key."""
    return secrets.token_bytes(32)


def key_to_base64(key: bytes) -> str:
    """Convert key to base64 string for display/storage."""
    return base64.b64encode(key).decode("ascii")


def key_from_base64(key_base64: str) -> bytes:
    """Convert base64 string back to key bytes."""
    return base64.b64decode(key_base64)


# Global encryption manager instance (initialized after key is loaded)
_encryption_manager: EncryptionManager | None = None
_encryption_manager_lock = None  # Will be initialized as asyncio.Lock when needed


def _get_lock():
    """Get or create the encryption manager lock."""
    global _encryption_manager_lock
    if _encryption_manager_lock is None:
        import asyncio
        _encryption_manager_lock = asyncio.Lock()
    return _encryption_manager_lock


async def get_encryption_manager_async() -> EncryptionManager:
    """Get the global encryption manager instance (async, thread-safe)."""
    global _encryption_manager
    if _encryption_manager is None:
        async with _get_lock():
            # Double-check after acquiring lock
            if _encryption_manager is None:
                from app.config import get_encryption_key
                key = get_encryption_key()
                _encryption_manager = EncryptionManager(key)
    return _encryption_manager


def get_encryption_manager() -> EncryptionManager:
    """Get the global encryption manager instance (sync version, for backward compatibility)."""
    global _encryption_manager
    if _encryption_manager is None:
        from app.config import get_encryption_key
        key = get_encryption_key()
        _encryption_manager = EncryptionManager(key)
    return _encryption_manager


def init_encryption_manager(key: bytes) -> EncryptionManager:
    """Initialize the global encryption manager with a specific key."""
    global _encryption_manager
    _encryption_manager = EncryptionManager(key)
    return _encryption_manager


def encrypt_value(value: str) -> bytes:
    """Convenience function to encrypt a value."""
    return get_encryption_manager().encrypt(value)


def decrypt_value(encrypted: bytes) -> str:
    """Convenience function to decrypt a value."""
    return get_encryption_manager().decrypt(encrypted)
