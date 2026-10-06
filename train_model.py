import argparse
import hashlib
import json
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import IsolationForest

FEATURES = ['engine_temp_c', 'cabin_pressure_psi', 'vibration_rpm']
ROOT = Path(__file__).resolve().parent

def train(output):
    rng = np.random.default_rng(42)
    data = pd.DataFrame({FEATURES[0]: rng.uniform(600, 850, 10000), FEATURES[1]: rng.uniform(10.5, 11.5, 10000), FEATURES[2]: rng.uniform(2500, 3500, 10000)})
    model = IsolationForest(contamination=0.01, random_state=42, n_jobs=1).fit(data)
    output.mkdir(parents=True, exist_ok=True)
    artifact = pickle.dumps(model, protocol=5)
    (output / 'ai_model.pkl').write_bytes(artifact)
    manifest = {'model_version': 'iforest-synthetic-v2', 'sha256': hashlib.sha256(artifact).hexdigest(),
        'features': FEATURES, 'sklearn_version': sklearn.__version__, 'numpy_version': np.__version__, 'pandas_version': pd.__version__, 'seed': 42, 'training_rows': 10000,
        'demo_ranges': {'engine_temp_c': [600, 850], 'cabin_pressure_psi': [10.5, 11.5], 'vibration_rpm': [2500, 3500]}}
    (output / 'model_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps(manifest))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=ROOT / 'models')
    train(parser.parse_args().output)
