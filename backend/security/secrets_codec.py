import base64
import ctypes
import getpass
import hashlib
import hmac
import os
import platform
import struct
import sys
import uuid
from ctypes import wintypes

import yaml

_ENC_PREFIX = "enc:v1:"
_DPAPI_PREFIX = f"{_ENC_PREFIX}dpapi:"
_XOR_PREFIX = f"{_ENC_PREFIX}xor:"
_PLACEHOLDER_TOKEN = "YOUR_API_KEY"


def _is_placeholder_secret(value: str) -> bool:
    return _PLACEHOLDER_TOKEN in str(value or "")


def _build_machine_key() -> bytes:
    machine_fingerprint = "|".join(
        [
            platform.node() or "",
            getpass.getuser() or "",
            str(uuid.getnode()),
            sys.platform,
        ]
    )
    return hashlib.pbkdf2_hmac(
        "sha256",
        machine_fingerprint.encode("utf-8"),
        b"silkloom-secret-v1",
        120000,
        dklen=32,
    )


def _keystream(key: bytes, nonce: bytes, length: int) -> bytes:
    chunks = []
    counter = 0
    while sum(len(c) for c in chunks) < length:
        block = hashlib.sha256(key + nonce + struct.pack(">I", counter)).digest()
        chunks.append(block)
        counter += 1
    return b"".join(chunks)[:length]


def _xor_encrypt(plain_text: str) -> str:
    plain = plain_text.encode("utf-8")
    nonce = os.urandom(16)
    key = _build_machine_key()
    stream = _keystream(key, nonce, len(plain))
    cipher = bytes(p ^ s for p, s in zip(plain, stream))
    tag = hmac.new(key, nonce + cipher, hashlib.sha256).digest()
    payload = base64.b64encode(nonce + tag + cipher).decode("ascii")
    return f"{_XOR_PREFIX}{payload}"


def _xor_decrypt(cipher_text: str) -> str:
    raw = base64.b64decode(cipher_text.encode("ascii"))
    if len(raw) < 48:
        raise ValueError("invalid encrypted payload")
    nonce = raw[:16]
    tag = raw[16:48]
    cipher = raw[48:]

    key = _build_machine_key()
    expected_tag = hmac.new(key, nonce + cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expected_tag):
        raise ValueError("integrity check failed")

    stream = _keystream(key, nonce, len(cipher))
    plain = bytes(c ^ s for c, s in zip(cipher, stream))
    return plain.decode("utf-8")


def _to_blob(data: bytes):
    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]

    if data:
        buffer = ctypes.create_string_buffer(data)
        blob = DATA_BLOB(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))
    else:
        buffer = None
        blob = DATA_BLOB(0, None)
    return DATA_BLOB, blob, buffer


def _dpapi_encrypt(plain_text: str) -> str:
    if sys.platform != "win32":
        raise RuntimeError("dpapi is only available on windows")

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    data = plain_text.encode("utf-8")

    DATA_BLOB, in_blob, _in_buffer = _to_blob(data)
    out_blob = DATA_BLOB()

    if not crypt32.CryptProtectData(ctypes.byref(in_blob), "SilkLoom", None, None, None, 0, ctypes.byref(out_blob)):
        raise ctypes.WinError()

    try:
        encrypted = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return f"{_DPAPI_PREFIX}{base64.b64encode(encrypted).decode('ascii')}"
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def _dpapi_decrypt(cipher_text: str) -> str:
    if sys.platform != "win32":
        raise RuntimeError("dpapi is only available on windows")

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    encrypted = base64.b64decode(cipher_text.encode("ascii"))

    DATA_BLOB, in_blob, _in_buffer = _to_blob(encrypted)
    out_blob = DATA_BLOB()

    if not crypt32.CryptUnprotectData(ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)):
        raise ctypes.WinError()

    try:
        decrypted = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return decrypted.decode("utf-8")
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def is_encrypted_secret(value: str) -> bool:
    return isinstance(value, str) and value.startswith(_ENC_PREFIX)


def encrypt_secret(value: str) -> str:
    text = str(value or "").strip()
    if not text or _is_placeholder_secret(text) or is_encrypted_secret(text):
        return text

    if sys.platform == "win32":
        try:
            return _dpapi_encrypt(text)
        except Exception:
            pass

    return _xor_encrypt(text)


def decrypt_secret(value: str) -> str:
    text = str(value or "").strip()
    # Strict mode: only encrypted payload is accepted for persisted secrets.
    if not text or _is_placeholder_secret(text):
        return text

    if not is_encrypted_secret(text):
        return ""

    try:
        if text.startswith(_DPAPI_PREFIX):
            return _dpapi_decrypt(text[len(_DPAPI_PREFIX) :])
        if text.startswith(_XOR_PREFIX):
            return _xor_decrypt(text[len(_XOR_PREFIX) :])
    except Exception:
        return ""

    return ""


def _mask_plain_secret_in_yaml(yaml_text: str) -> str:
    def _strict_transform(api_key: str) -> str:
        if not api_key:
            return api_key
        if _is_placeholder_secret(api_key):
            return api_key
        if is_encrypted_secret(api_key):
            return api_key
        return ""

    return _transform_api_key_in_yaml(yaml_text, _strict_transform)


def decrypt_secret_compat_disabled(value: str) -> str:
    """Backward-compat alias kept for explicit strict usage."""
    return decrypt_secret(value)


def decrypt_config_yaml_strict(yaml_text: str) -> str:
    """Strictly accept encrypted api_key, clear any plaintext value."""
    sanitized = _mask_plain_secret_in_yaml(yaml_text)
    return _transform_api_key_in_yaml(sanitized, decrypt_secret)


def _transform_api_key_in_yaml(yaml_text: str, transform) -> str:
    try:
        config = yaml.safe_load(yaml_text) or {}
    except Exception:
        return yaml_text

    if not isinstance(config, dict):
        return yaml_text

    llm_cfg = config.get("llm")
    if not isinstance(llm_cfg, dict):
        return yaml_text

    api_key = llm_cfg.get("api_key")
    if not isinstance(api_key, str):
        return yaml_text

    llm_cfg["api_key"] = transform(api_key)
    return yaml.safe_dump(config, allow_unicode=True, sort_keys=False)


def encrypt_config_yaml(yaml_text: str) -> str:
    return _transform_api_key_in_yaml(yaml_text, encrypt_secret)


def decrypt_config_yaml(yaml_text: str) -> str:
    return decrypt_config_yaml_strict(yaml_text)
