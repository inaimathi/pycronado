from os import environ as ENV

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.exceptions import InvalidTokenError

JWT_PRIVKEY = ENV.get("JWT_PRIVKEY")
JWT_PRIVKEY_PASSWORD = ENV.get("JWT_PRIVKEY_PASSWORD")
JWT_PUBKEY = ENV.get("JWT_PUBKEY")
JWT_ISSUER = ENV.get("JWT_ISSUER")


def ensureKeysFromPEM(private_key_pem):
    global JWT_PRIVKEY
    global JWT_PUBKEY
    JWT_PRIVKEY = decodePrivateKey(private_key_pem)
    JWT_PUBKEY = mkPublicKey(private_key_pem)


def ensureFreshKeys(key_size=4096):
    ensureKeysFromPEM(mkPrivateKey())


def decodePrivateKey(private_key_pem):
    return serialization.load_pem_private_key(
        private_key_pem, password=JWT_PRIVKEY_PASSWORD
    )


def mkPrivateKey(key_size=4096):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)

    if JWT_PRIVKEY_PASSWORD:
        encryption_algorithm = serialization.BestAvailableEncryption(
            JWT_PRIVKEY_PASSWORD
        )
    else:
        encryption_algorithm = serialization.NoEncryption()

    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption_algorithm,
    )

    return pem


def mkPublicKey(private_key_pem):
    private_key = decodePrivateKey(private_key_pem)
    public_key = private_key.public_key()
    public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return public_key_pem


def encode(object):
    assert JWT_PRIVKEY, "No private key in JWT_PRIVKEY"
    return jwt.encode(object, JWT_PRIVKEY, algorithm="RS256")


def decode(tok):
    assert JWT_PUBKEY, "Need public key in JWT_PUBKEY"
    return jwt.decode(
        tok,
        JWT_PUBKEY,
        issuer=JWT_ISSUER,
        algorithms=["RS256"],
    )


def hasValidSig(token):
    """
    Checks for a valid signature on the given token, disregards expiration status.
    This is useful for checking incoming JWTs for refreshing, use `decode` for
    """
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
