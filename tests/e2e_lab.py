"""Runs against an already running local Kafka + gateway + AI lab; never shuts services down."""
import argparse
import json
import os
from pathlib import Path
import time
import urllib.request
import uuid
import sys
from kafka import KafkaProducer, KafkaConsumer
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telemetry_producer import sign_payload, next_sequence
from kafka_security import ssl_options
import sqlite3

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--gateway', default='http://127.0.0.1:3000')
    parser.add_argument('--expect-unknown', action='store_true', help='Use only when AI has intentionally been stopped by the lab operator')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    key = json.loads(Path(os.getenv('PRODUCER_KEY_PATH', str(ROOT / 'security/producer-key.json'))).read_text())
    state_path = Path(os.getenv('PRODUCER_STATE_PATH', str(ROOT / 'data/producer.sqlite')))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(state_path, isolation_level=None)
    db.execute('CREATE TABLE IF NOT EXISTS sequence (producer TEXT,key_id TEXT,sequence INTEGER,PRIMARY KEY(producer,key_id))')
    broker = os.getenv('KAFKA_BROKERS', '127.0.0.1:29092').split(',')
    sender = KafkaProducer(bootstrap_servers=broker, acks='all', value_serializer=lambda value: json.dumps(value).encode(), **ssl_options())
    report = []
    payload = {'sensor_id': key['sensor_id'], 'engine_temp_c': 720.0, 'cabin_pressure_psi': 11.0, 'vibration_rpm': 3000.0}
    cases = []
    def signed(data=payload):
        return sign_payload(data, key, next_sequence(db, key))
    if args.expect_unknown:
        cases.append(('AI_OFFLINE', signed(), 'UNKNOWN', 'QUARANTINE'))
    else:
        cases.append(('NORMAL', signed(), 'NORMAL', 'ACCEPT'))
        cases.append(('ANOMALY', signed({**payload, 'engine_temp_c': 5000.0}), 'ANOMALY', 'ACCEPT'))
        altered = signed(); altered['mac'] = ('0' if altered['mac'][0] != '0' else '1') + altered['mac'][1:]
        cases.append(('TAMPER', altered, 'REJECTED', 'QUARANTINE'))
        replay = signed()
        cases.extend([('REPLAY_ORIGINAL', replay, 'NORMAL', 'ACCEPT'), ('REPLAY_COPY', replay, 'REJECTED', 'QUARANTINE')])
        cases.append(('MALFORMED_SCHEMA', {'unexpected': True}, 'REJECTED', 'QUARANTINE'))
    try:
        for name, message, verdict, decision in cases:
            start = time.perf_counter()
            metadata = sender.send(os.getenv('RAW_TOPIC', 'flight_telemetry_raw'), message, key=key['producer_id'].encode()).get(timeout=15)
            event = None
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                with urllib.request.urlopen(args.gateway + '/api/events', timeout=2) as response:
                    events = json.load(response)
                event = next((e for e in events if e['source']['topic'] == metadata.topic and e['source']['partition'] == metadata.partition and e['source']['offset'] == str(metadata.offset)), None)
                if event:
                    break
                time.sleep(0.05)
            assert event, f'{name}: no durable gateway event'
            assert event['verdict'] == verdict, (name, event)
            assert event['decision'] == decision, (name, event)
            report.append({'case': name, 'verdict': event['verdict'], 'decision': event['decision'], 'reason': event['reason'], 'event_id': event['event_id'], 'observed_ms': round((time.perf_counter() - start) * 1000, 2)})
        # Audit inspection uses a separate operator identity; producer permissions are never widened for tests.
        consumer = KafkaConsumer('flight_telemetry_verified', 'flight_telemetry_quarantine', 'security_events', bootstrap_servers=broker, group_id='ahms-e2e-' + uuid.uuid4().hex, auto_offset_reset='earliest', enable_auto_commit=False, **ssl_options(prefix='AUDIT_SSL_', require=os.getenv('KAFKA_REQUIRE_MTLS') == 'true'))
        routed = {}
        deadline = time.monotonic() + 20
        try:
            while time.monotonic() < deadline:
                for messages in consumer.poll(timeout_ms=500).values():
                    for msg in messages:
                        value = json.loads(msg.value)
                        routed.setdefault(value['event_id'], set()).add(msg.topic)
                if all(len(routed.get(row['event_id'], set())) >= 2 for row in report):
                    break
        finally:
            consumer.close()
        for row in report:
            expected = 'flight_telemetry_verified' if row['decision'] == 'ACCEPT' else 'flight_telemetry_quarantine'
            assert expected in routed.get(row['event_id'], set()), (row, routed)
            assert 'security_events' in routed[row['event_id']], row
            row['kafka_output_verified'] = True
        result = {'transport': 'real Apache Kafka + HTTP FastAPI + gateway + SQLite', 'cases': report,
            'timing_note': 'Includes Kafka send and HTTP event polling; not a latency benchmark.'}
        if args.output:
            args.output.write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps(result, indent=2))
    finally:
        sender.close()
        db.close()

if __name__ == '__main__':
    main()
