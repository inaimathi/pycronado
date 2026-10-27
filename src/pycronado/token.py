# src/pycronado/token.py
import os
from os import environ as ENV

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.exceptions import InvalidTokenError

JWT_PRIVKEY = ENV.get("JWT_PRIVKEY")
JWT_PRIVKEY_PASSWORD = ENV.get("JWT_PRIVKEY_PASSWORD")
JWT_PUBKEY = ENV.get("JWT_PUBKEY")
JWT_ISSUER = ENV.get("JWT_ISSUER")


def _password_bytes():
    if JWT_PRIVKEY_PASSWORD is None:
        return None
    if isinstance(JWT_PRIVKEY_PASSWORD, (bytes, bytearray)):
        return JWT_PRIVKEY_PASSWORD
    return JWT_PRIVKEY_PASSWORD.encode("utf-8")


def decodePrivateKey(private_key_pem):
    if isinstance(private_key_pem, str):
        private_key_pem = private_key_pem.encode("utf-8")
    return serialization.load_pem_private_key(
        private_key_pem,
        password=_password_bytes(),
    )


def mkPrivateKey(key_size=4096):
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
    )

    if JWT_PRIVKEY_PASSWORD:
        encryption_algorithm = serialization.BestAvailableEncryption(_password_bytes())
    else:
        encryption_algorithm = serialization.NoEncryption()

    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption_algorithm,
    )
    return pem


def mkPublicKey(private_key_pem):
    priv = decodePrivateKey(private_key_pem)
    pub = priv.public_key()
    pub_pem = pub.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return pub_pem


def ensureKeysFromPEM(private_key_pem):
    global JWT_PRIVKEY
    global JWT_PUBKEY

    JWT_PRIVKEY = decodePrivateKey(private_key_pem)
    JWT_PUBKEY = mkPublicKey(private_key_pem)


def ensureFreshKeys(key_size=4096):
    pem = mkPrivateKey(key_size=key_size)
    ensureKeysFromPEM(pem)
    return pem


def writePrivateKey(private_key_pem, pathname, mode=0o600):
    """Create a private key without overwriting an existing identity."""
    data = private_key_pem.encode() if isinstance(private_key_pem, str) else private_key_pem
    parent = os.path.dirname(pathname)
    if parent:
        os.makedirs(parent, mode=0o700, exist_ok=True)
    fd = os.open(pathname, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def ensureKeysFrom(key_size=4096, file_path=None):
    if file_path is None:
        return ensureFreshKeys(key_size=key_size)
    if not os.path.exists(file_path):
        pem = mkPrivateKey(key_size=key_size)
        try:
            writePrivateKey(pem, file_path)
        except FileExistsError:
            pass  # Another process created it. Never replace it.
    # Invalid, unreadable or unpersistable keys fail startup. No silent rotation.
    with open(file_path, "rb") as fh:
        pem = fh.read()
    ensureKeysFromPEM(pem)
    return pem


def encode(payload_obj):
    assert JWT_PRIVKEY, "No private key in JWT_PRIVKEY"
    return jwt.encode(payload_obj, JWT_PRIVKEY, algorithm="RS256")


def decode(tok, *, allowed_purposes=(None,)):
    assert JWT_PUBKEY, "Need public key in JWT_PUBKEY"
    audience = ENV.get("JWT_AUDIENCE") or None
    claims = jwt.decode(
        tok, JWT_PUBKEY, issuer=JWT_ISSUER, audience=audience,
        algorithms=["RS256"],
        options={"require": ["exp", "iat"], "verify_aud": audience is not None},
    )
    if claims.get("purpose") not in allowed_purposes:
        raise InvalidTokenError("Token purpose is not valid here")
    return claims


def hasValidSig(token):
    try:
        jwt.decode(
            token,
            JWT_PUBKEY,
            algorithms=["RS256"],
            options={"verify_exp": False, "verify_iat": False},
        )
        return True
    except InvalidTokenError:
        return False
