"""Shared strict Kafka client transport configuration."""
import os

def ssl_options(prefix='KAFKA_SSL_', require=None):
    ca, cert, key = [os.getenv(prefix + name) for name in ('CA', 'CERT', 'KEY')]
    if require is None:
        require = os.getenv('KAFKA_REQUIRE_MTLS', 'false').lower() == 'true'
    if (cert or key or require) and not (ca and cert and key):
        raise ValueError('Kafka mTLS requires CA, CERT and KEY together.')
    if not ca:
        return {}
    result = {'security_protocol': 'SSL', 'ssl_cafile': ca, 'ssl_check_hostname': True}
    if cert:
        result.update(ssl_certfile=cert, ssl_keyfile=key)
    return result
