"""Real Kafka allow/deny checks. Transport failures are never counted as ACL denials."""
import argparse
import json
import os
from pathlib import Path
import socket
import ssl
import time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import for_code
from kafka.errors import TopicAuthorizationFailedError, GroupAuthorizationFailedError

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    tls = Path(os.getenv('TEST_TLS_ROOT', str(ROOT / 'security/tls')))
    brokers = os.getenv('KAFKA_BROKERS', '127.0.0.1:29092').split(',')
    report = []
    def options(role):
        folder = tls / role
        return dict(bootstrap_servers=brokers, security_protocol='SSL',
            ssl_cafile=str(folder / 'ca.pem'), ssl_certfile=str(folder / 'cert.pem'),
            ssl_keyfile=str(folder / 'key.pem'), ssl_check_hostname=True,
            api_version=(3, 0, 0), request_timeout_ms=20000)
    def check(name, action, expected=None):
        try:
            action()
        except Exception as error:
            if expected is None or not isinstance(error, expected):
                raise AssertionError(f'{name}: unexpected {type(error).__name__}') from error
            report.append({'case': name, 'passed': True, 'evidence': type(error).__name__})
        else:
            if expected is not None:
                raise AssertionError(f'{name}: forbidden operation succeeded')
            report.append({'case': name, 'passed': True, 'evidence': 'operation succeeded'})
        print(json.dumps(report[-1]), flush=True)
    def send(role, topic):
        producer = KafkaProducer(acks='all', max_block_ms=7000, retries=0, **options(role))
        try:
            # Deliberately unsigned; the gateway must quarantine it even though Kafka permits raw write.
            producer.send(topic, b'{"security_acl_probe":true}').get(timeout=12)
        finally:
            producer.close(timeout=2)
    def read(role, topic, group=None):
        admin = KafkaAdminClient(**options(role))
        try:
            metadata = admin.describe_topics([topic])[0]
            if metadata['error_code']:
                raise for_code(metadata['error_code'])(topic)
        finally:
            admin.close()
        consumer = KafkaConsumer(group_id=group, enable_auto_commit=False,
            auto_offset_reset='earliest', **options(role))
        try:
            if group:
                consumer.subscribe([topic])
                deadline = time.monotonic() + 15
                records = {}
                while time.monotonic() < deadline:
                    records = consumer.poll(timeout_ms=500)
                    if records:
                        break
                if not consumer.assignment() or not records:
                    raise AssertionError('No verified data; send one normal scenario before this test')
            else:
                partition = TopicPartition(topic, 0)
                consumer.assign([partition])
                consumer.seek(partition, 0)
                # Explicitly request metadata/offsets so a pending metadata refresh cannot be mistaken for success.
                consumer.end_offsets([partition])
                deadline = time.monotonic() + 12
                while time.monotonic() < deadline:
                    if consumer.poll(timeout_ms=500):
                        break
        finally:
            consumer.close()
    check('producer can write raw', lambda: send('producer', 'flight_telemetry_raw'))
    check('producer cannot bypass gateway', lambda: send('producer', 'flight_telemetry_verified'), TopicAuthorizationFailedError)
    check('producer cannot write quarantine', lambda: send('producer', 'flight_telemetry_quarantine'), TopicAuthorizationFailedError)
    check('producer cannot read raw', lambda: read('producer', 'flight_telemetry_raw'), TopicAuthorizationFailedError)
    check('reader can read verified with own group', lambda: read('reader', 'flight_telemetry_verified', 'ahms-verified-reader'))
    check('reader cannot read raw', lambda: read('reader', 'flight_telemetry_raw'), TopicAuthorizationFailedError)
    check('reader cannot write verified', lambda: send('reader', 'flight_telemetry_verified'), TopicAuthorizationFailedError)
    check('reader cannot use foreign group', lambda: read('reader', 'flight_telemetry_verified', 'ahms-forbidden-group'), GroupAuthorizationFailedError)
    check('trusted certificate without ACL cannot write', lambda: send('probe', 'flight_telemetry_raw'), TopicAuthorizationFailedError)
    host, port = brokers[0].rsplit(':', 1)
    def handshake(with_cert, server_hostname):
        context = ssl.create_default_context(cafile=str(tls / 'producer/ca.pem'))
        context.maximum_version = ssl.TLSVersion.TLSv1_2
        if with_cert:
            context.load_cert_chain(str(tls / 'producer/cert.pem'), str(tls / 'producer/key.pem'))
        with socket.create_connection((host, int(port)), timeout=5) as raw:
            with context.wrap_socket(raw, server_hostname=server_hostname):
                pass
    check('client certificate required', lambda: handshake(False, host), ssl.SSLError)
    check('server hostname checked', lambda: handshake(True, 'untrusted.invalid'), ssl.SSLCertVerificationError)
    result = {'transport': 'real Kafka mTLS + identity ACL', 'cases': report}
    if args.output:
        args.output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
