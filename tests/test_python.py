import base64
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import sys
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_service import app, load_artifact
from telemetry_producer import sign_payload, next_sequence
from train_model import train

KEY = {'key_id': 'test-key', 'producer_id': 'test-producer', 'sensor_id': 'ENG-01', 'secret_hex': '11' * 32}
PAYLOAD = {'sensor_id': 'ENG-01', 'engine_temp_c': 720.0, 'cabin_pressure_psi': 11.0, 'vibration_rpm': 3000.0}

class PythonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix='ahms-model-')
        cls.folder = Path(cls.directory.name)
        train(cls.folder)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_actual_inference_and_input_validation(self):
        with patch.dict(os.environ, {'MODEL_DIR': str(self.folder)}), TestClient(app) as client:
            self.assertEqual(client.get('/health').status_code, 200)
            response = client.post('/predict', json={k: v for k, v in PAYLOAD.items() if k != 'sensor_id'})
            self.assertEqual(response.status_code, 200)
            self.assertIsInstance(response.json()['is_anomaly'], bool)
            self.assertFalse(response.json()['is_anomaly'])
            anomaly = client.post('/predict', json={'engine_temp_c': 5000.0, 'cabin_pressure_psi': 11.0, 'vibration_rpm': 3000.0})
            self.assertEqual(anomaly.status_code, 200)
            self.assertTrue(anomaly.json()['is_anomaly'])
            self.assertTrue(anomaly.json()['rule_anomaly'])
            # Keep the raw model result visible: an out-of-range value need not cross the learned IF threshold.
            self.assertFalse(anomaly.json()['model_is_anomaly'])
            for data in [{}, {'engine_temp_c': '720', 'cabin_pressure_psi': 11.0, 'vibration_rpm': 3000.0}, {**PAYLOAD}]:
                self.assertEqual(client.post('/predict', json=data).status_code, 422)

    def test_model_checksum_is_checked_before_pickle_load(self):
        with tempfile.TemporaryDirectory(prefix='ahms-corrupt-') as folder:
            target = Path(folder)
            shutil.copy(self.folder / 'model_manifest.json', target)
            (target / 'ai_model.pkl').write_bytes(b'not a pickle')
            with self.assertRaisesRegex(RuntimeError, 'integrity'):
                load_artifact(target)

    def test_sequence_persists_across_connection_restart(self):
        with tempfile.TemporaryDirectory(prefix='ahms-sequence-') as folder:
            path = Path(folder) / 'producer.sqlite'
            db = sqlite3.connect(path, isolation_level=None)
            db.execute('CREATE TABLE sequence (producer TEXT,key_id TEXT,sequence INTEGER,PRIMARY KEY(producer,key_id))')
            self.assertEqual(next_sequence(db, KEY), 1)
            db.close()
            db = sqlite3.connect(path, isolation_level=None)
            self.assertEqual(next_sequence(db, KEY), 2)
            db.close()

    def test_python_signature_is_verified_by_node_without_float_reserialization(self):
        e = sign_payload(PAYLOAD, KEY, 1, timestamp_ms=1700000000000, message_id='a' * 32)
        root = Path(__file__).resolve().parents[1]
        script = "const p=require('./lib/protocol');let x='';process.stdin.on('data',d=>x+=d);process.stdin.on('end',()=>{const input=JSON.parse(x);const result=p.verify(Buffer.from(JSON.stringify(input.envelope)),{keys:[{...input.key,enabled:true}]},1700000000000);console.log(JSON.stringify(result.telemetry));});"
        result = subprocess.run(['node', '-e', script], cwd=root, input=json.dumps({'envelope': e, 'key': KEY}), text=True, capture_output=True, check=True)
        self.assertEqual(json.loads(result.stdout), PAYLOAD)

if __name__ == '__main__':
    unittest.main()
