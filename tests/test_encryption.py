"""Tests for encryption module."""

import pytest

from app.encryption import (
    EncryptionManager,
    generate_encryption_key,
    key_to_base64,
    key_from_base64,
)


def test_generate_encryption_key():
    """Test encryption key generation."""
    key = generate_encryption_key()
    assert len(key) == 32
    assert isinstance(key, bytes)


def test_key_base64_roundtrip():
    """Test key conversion to/from base64."""
    key = generate_encryption_key()
    b64 = key_to_base64(key)
    recovered = key_from_base64(b64)
    assert key == recovered


def test_encryption_manager_encrypt_decrypt():
    """Test basic encryption and decryption."""
    key = generate_encryption_key()
    manager = EncryptionManager(key)

    plaintext = "Hello, World!"
    encrypted = manager.encrypt(plaintext)
    decrypted = manager.decrypt(encrypted)

    assert decrypted == plaintext
    assert encrypted != plaintext.encode()


def test_encryption_manager_encrypt_bytes():
    """Test encryption of bytes."""
    key = generate_encryption_key()
    manager = EncryptionManager(key)

    plaintext = b"Binary data \x00\x01\x02"
    encrypted = manager.encrypt(plaintext)
    decrypted = manager.decrypt(encrypted)

    assert decrypted == plaintext.decode("utf-8")


def test_encryption_manager_base64():
    """Test base64 encryption methods."""
    key = generate_encryption_key()
    manager = EncryptionManager(key)

    plaintext = "Test data for base64"
    encrypted = manager.encrypt_to_base64(plaintext)
    decrypted = manager.decrypt_from_base64(encrypted)

    assert decrypted == plaintext
    assert isinstance(encrypted, str)


def test_encryption_different_keys():
    """Test that different keys produce different ciphertext."""
    key1 = generate_encryption_key()
    key2 = generate_encryption_key()

    manager1 = EncryptionManager(key1)
    manager2 = EncryptionManager(key2)

    plaintext = "Same message"
    encrypted1 = manager1.encrypt(plaintext)
    encrypted2 = manager2.encrypt(plaintext)

    # Encrypted values should be different
    assert encrypted1 != encrypted2


def test_encryption_manager_invalid_key():
    """Test that short keys are rejected."""
    with pytest.raises(ValueError):
        EncryptionManager(b"short")


def test_encryption_nonce_uniqueness():
    """Test that each encryption uses a unique nonce."""
    key = generate_encryption_key()
    manager = EncryptionManager(key)

    plaintext = "Same message"
    encrypted1 = manager.encrypt(plaintext)
    encrypted2 = manager.encrypt(plaintext)

    # Even same plaintext should produce different ciphertext due to random nonce
    assert encrypted1 != encrypted2

    # But both should decrypt correctly
    assert manager.decrypt(encrypted1) == plaintext
    assert manager.decrypt(encrypted2) == plaintext


def test_decryption_invalid_data():
    """Test decryption of invalid data."""
    key = generate_encryption_key()
    manager = EncryptionManager(key)

    # Too short
    with pytest.raises(ValueError):
        manager.decrypt(b"short")

    # Invalid ciphertext
    with pytest.raises(Exception):
        manager.decrypt(b"x" * 100)
