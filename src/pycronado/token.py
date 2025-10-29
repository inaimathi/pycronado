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
    if isinstance(private_key_pem, str):
        data = private_key_pem.encode("utf-8")
    else:
        data = private_key_pem

    parent = os.path.dirname(pathname)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)

    tmp_path = f"{pathname}.tmp"
    with open(tmp_path, "wb") as fh:
        fh.write(data)
    os.replace(tmp_path, pathname)

    try:
        os.chmod(pathname, mode)
    except Exception:
        # best effort; ignore permission failures on weird filesystems
        pass


def ensureKeysFrom(key_size=4096, file_path=None):
    # Ephemeral mode: just generate and don't persist.
    if file_path is None:
        return ensureFreshKeys(key_size=key_size)

    # Persistent mode.
    if os.path.exists(file_path):
        try:
            with open(file_path, "rb") as fh:
                pem_bytes = fh.read()
            # Validate we can actually parse it as a private key. If this fails,
            # we'll fall through to generation.
            decodePrivateKey(pem_bytes)
            ensureKeysFromPEM(pem_bytes)
            return pem_bytes
        except Exception:
            # fall through to generate-and-write
            pass

    # No usable file: generate, write, then load.
    pem_bytes = mkPrivateKey(key_size=key_size)
    try:
        writePrivateKey(pem_bytes, file_path, mode=0o600)
    except Exception:
        # best effort: even if we fail to persist, we can still run with in-memory key
        pass
    ensureKeysFromPEM(pem_bytes)
    return pem_bytes


def encode(payload_obj):
    assert JWT_PRIVKEY, "No private key in JWT_PRIVKEY"
    return jwt.encode(payload_obj, JWT_PRIVKEY, algorithm="RS256")


def decode(tok):
    assert JWT_PUBKEY, "Need public key in JWT_PUBKEY"
    return jwt.decode(
        tok,
        JWT_PUBKEY,
        issuer=JWT_ISSUER,
        algorithms=["RS256"],
    )


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
