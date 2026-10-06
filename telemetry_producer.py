"""Signed local telemetry simulator. HMAC authenticates content; it does not encrypt it."""
import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import random
import sqlite3
import time
import uuid
from kafka_security import ssl_options

ROOT = Path(__file__).resolve().parent

def sign_payload(payload, key, sequence, timestamp_ms=None, message_id=None):
    envelope = {
        'schema_version': 1, 'key_id': key['key_id'], 'producer_id': key['producer_id'],
        'sequence': sequence, 'timestamp_ms': timestamp_ms if timestamp_ms is not None else time.time_ns() // 1_000_000,
        'message_id': message_id or uuid.uuid4().hex,
        'payload_b64': base64.b64encode(json.dumps(payload, separators=(',', ':'), allow_nan=False).encode()).decode(),
    }
    signed = '\n'.join(str(x) for x in ['AHMSv1', envelope['key_id'], envelope['producer_id'], envelope['sequence'], envelope['timestamp_ms'], envelope['message_id'], envelope['payload_b64']]).encode()
    envelope['mac'] = hmac.new(bytes.fromhex(key['secret_hex']), signed, hashlib.sha256).hexdigest()
    return envelope

def next_sequence(db, key):
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT sequence FROM sequence WHERE producer=? AND key_id=?', (key['producer_id'], key['key_id'])).fetchone()
        sequence = (row[0] if row else 0) + 1
        if sequence > 9007199254740991:
            raise ValueError('Sequence exhausted; rotate the producer key.')
        db.execute('INSERT INTO sequence VALUES(?,?,?) ON CONFLICT(producer,key_id) DO UPDATE SET sequence=excluded.sequence', (key['producer_id'], key['key_id'], sequence))
        db.commit()
        return sequence
    except Exception:
        db.rollback()
        raise

def main():
    from kafka import KafkaProducer
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', choices=['normal', 'anomaly', 'tamper', 'replay'], default='normal')
    parser.add_argument('--count', type=int, default=0, help='0: continuously; otherwise stop after N generated messages')
    parser.add_argument('--interval', type=float, default=1.0)
    args = parser.parse_args()
    if args.count < 0 or args.interval < 0:
        parser.error('count and interval must be non-negative')
    key_path = Path(os.getenv('PRODUCER_KEY_PATH', str(ROOT / 'security/producer-key.json')))
    key = json.loads(key_path.read_text(encoding='utf-8'))
    state_path = Path(os.getenv('PRODUCER_STATE_PATH', str(ROOT / 'data/producer.sqlite')))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(state_path, isolation_level=None)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE IF NOT EXISTS sequence (producer TEXT, key_id TEXT, sequence INTEGER NOT NULL, PRIMARY KEY(producer,key_id))')
    kafka_options = ssl_options()
    producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BROKERS', '127.0.0.1:29092').split(','),
        value_serializer=lambda value: json.dumps(value, separators=(',', ':'), allow_nan=False).encode(),
        key_serializer=lambda value: value.encode(), acks='all', retries=3, request_timeout_ms=10000, **kafka_options)
    count = 0
    try:
        while args.count == 0 or count < args.count:
            count += 1
            payload = {'sensor_id': key['sensor_id'], 'engine_temp_c': round(random.uniform(600, 850), 2),
                'cabin_pressure_psi': round(random.uniform(10.5, 11.5), 2), 'vibration_rpm': random.randint(2500, 3500)}
            if args.scenario == 'anomaly':
                payload['engine_temp_c'] = 5000.0
            envelope = sign_payload(payload, key, next_sequence(db, key))
            if args.scenario == 'tamper':
                payload['engine_temp_c'] = 5000.0
                envelope['payload_b64'] = base64.b64encode(json.dumps(payload, separators=(',', ':')).encode()).decode()
            topic = os.getenv('RAW_TOPIC', 'flight_telemetry_raw')
            producer.send(topic, envelope, key=key['producer_id']).get(timeout=15)
            if args.scenario == 'replay':
                producer.send(topic, envelope, key=key['producer_id']).get(timeout=15)
            print(json.dumps({'scenario': args.scenario, 'sequence': envelope['sequence'], 'message_id': envelope['message_id']}), flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close(timeout=15)
        db.close()

if __name__ == '__main__':
    main()
