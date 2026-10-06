"""Create a local lab CA and separate Kafka identities; never rotate existing trust silently."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import tempfile
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12

ROLES = ('broker', 'admin', 'gateway', 'producer', 'reader', 'probe')

def write(path, data, public=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o750)
    with path.open('xb') as stream:
        stream.write(data if isinstance(data, bytes) else data.encode())
    path.chmod(0o644 if public else 0o640)

def generate(destination):
    destination = Path(destination).resolve()
    if destination.exists():
        manifest = destination / 'manifest.json'
        if not manifest.exists():
            raise ValueError('TLS klasörü zaten var veya yarım kalmış. Otomatik üzerine yazılmaz.')
        report = json.loads(manifest.read_text())
        for name, digest in report['sha256'].items():
            if hashlib.sha256((destination / name).read_bytes()).hexdigest() != digest:
                raise ValueError('TLS dosyası değişmiş/eksik: ' + name)
        for role in ROLES:
            cert = x509.load_pem_x509_certificate((destination / role / 'cert.pem').read_bytes())
            if cert.not_valid_after_utc <= datetime.now(timezone.utc) + timedelta(days=7):
                raise ValueError('Sertifika yenilemesi gerekli: ' + role)
        print('Mevcut TLS dosyaları doğrulandı; anahtarlar korundu.')
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'AHMS Lab CA')])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
        .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=730))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .sign(ca_key, hashes.SHA256()))
    ca_pem = ca.public_bytes(serialization.Encoding.PEM)
    # A failed generation leaves only a temporary sibling; the destination is published atomically.
    with tempfile.TemporaryDirectory(prefix='tls-build-', dir=destination.parent) as scratch:
        root = Path(scratch) / 'tls'
        root.mkdir()
        write(root / 'authority/ca.pem', ca_pem, True)
        write(root / 'authority/ca-key.pem', ca_key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        for role in ROLES:
            key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'kafka' if role == 'broker' else role)])
            builder = (x509.CertificateBuilder().subject_name(name).issuer_name(ca_name)
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=365))
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(x509.KeyUsage(True, False, True, False, False, False, False, False, False), critical=True)
                .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH] +
                    ([ExtendedKeyUsageOID.SERVER_AUTH] if role == 'broker' else [])), critical=False)
                .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False))
            if role == 'broker':
                builder = builder.add_extension(x509.SubjectAlternativeName([
                    x509.DNSName('kafka'), x509.DNSName('localhost'),
                    x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
            cert = builder.sign(ca_key, hashes.SHA256())
            folder = root / role
            write(folder / 'ca.pem', ca_pem, True)
            write(folder / 'cert.pem', cert.public_bytes(serialization.Encoding.PEM), True)
            write(folder / 'key.pem', key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
            password = secrets.token_hex(24).encode()
            filename = 'broker.p12' if role == 'broker' else 'client.p12'
            write(folder / filename, pkcs12.serialize_key_and_certificates(role.encode(), key, cert, [ca],
                serialization.BestAvailableEncryption(password)))
            write(folder / 'truststore.p12', pkcs12.serialize_java_truststore([
                pkcs12.PKCS12Certificate(ca, b'ahms-ca')], serialization.BestAvailableEncryption(password)))
            write(folder / 'store-password', password + b'\n')
            mount = '/etc/kafka/secrets' if role == 'broker' else '/run/tls'
            write(folder / 'client.properties', '\n'.join([
                'security.protocol=SSL', 'ssl.endpoint.identification.algorithm=https',
                'ssl.keystore.type=PKCS12', f'ssl.keystore.location={mount}/{filename}',
                f'ssl.keystore.password={password.decode()}', f'ssl.key.password={password.decode()}',
                'ssl.truststore.type=PKCS12', f'ssl.truststore.location={mount}/truststore.p12',
                f'ssl.truststore.password={password.decode()}', '']) )
        report = {'purpose': 'local laboratory only', 'created_at': now.isoformat(),
            'sha256': {str(file.relative_to(root)).replace(os.sep, '/'): hashlib.sha256(file.read_bytes()).hexdigest()
                for file in root.rglob('*') if file.is_file()}}
        write(root / 'manifest.json', json.dumps(report, indent=2), True)
        root.rename(destination)
    print('TLS kimlikleri oluşturuldu. CA özel anahtarını çalışan servislere bağlamayın.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parents[1] / 'security/tls')
    args = parser.parse_args()
    generate(args.output)
