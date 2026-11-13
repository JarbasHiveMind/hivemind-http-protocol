"""The self-signed certificate the HTTP listener generates for itself.

It is generated once on first start and then trusted by whatever connects, so
what matters is that it parses as a certificate, that its private key is not
readable by anyone else on the box, and that two hosts do not produce the same
serial.
"""
import os
import ssl
import stat

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from hivemind_http_protocol import HiveMindHttpProtocol


def _generate(tmp_path, name="probe"):
    return HiveMindHttpProtocol.create_self_signed_cert(str(tmp_path), name)


def test_certificate_and_key_load(tmp_path):
    cert_path, key_path = _generate(tmp_path)

    with open(cert_path, "rb") as f:
        cert = x509.load_pem_x509_certificate(f.read())
    with open(key_path, "rb") as f:
        serialization.load_pem_private_key(f.read(), password=None)

    assert cert.not_valid_after_utc > cert.not_valid_before_utc

    # the pair is usable as a server credential, which is the whole point
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_path, key_path)


def test_private_key_is_not_world_readable(tmp_path):
    _, key_path = _generate(tmp_path)
    mode = stat.S_IMODE(os.stat(key_path).st_mode)
    assert not mode & (stat.S_IRGRP | stat.S_IROTH | stat.S_IWGRP | stat.S_IWOTH), (
        f"private key is readable beyond its owner: mode {mode:o}"
    )


def test_serial_numbers_do_not_collide(tmp_path):
    """A serial drawn from a small range collides across hosts.

    Certificates are identified by issuer and serial, and every host here
    builds the same issuer, so a repeated serial makes two certificates
    indistinguishable to a store that holds both.
    """
    serials = set()
    for i in range(16):
        d = tmp_path / f"host{i}"
        d.mkdir()
        cert_path, _ = _generate(d)
        with open(cert_path, "rb") as f:
            serials.add(x509.load_pem_x509_certificate(f.read()).serial_number)
    assert len(serials) == 16, f"only {len(serials)} distinct serials from 16 hosts"
    assert max(serials) > 2 ** 32, (
        "serials are drawn from a range small enough to collide in practice"
    )
