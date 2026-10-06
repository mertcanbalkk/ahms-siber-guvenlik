from contextlib import asynccontextmanager
import hashlib
import hmac
import json
import os
from pathlib import Path
import pickle
import pandas as pd
import numpy as np
import sklearn
from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, Field

ROOT = Path(__file__).resolve().parent
FEATURES = ['engine_temp_c', 'cabin_pressure_psi', 'vibration_rpm']

def load_artifact(folder):
    # The manifest must come from a trusted, read-only deployment. A co-located hash is not a signature.
    manifest = json.loads((folder / 'model_manifest.json').read_text(encoding='utf-8'))
    artifact = (folder / 'ai_model.pkl').read_bytes()
    if manifest.get('features') != FEATURES or manifest.get('sklearn_version') != sklearn.__version__ or manifest.get('numpy_version') != np.__version__ or manifest.get('pandas_version') != pd.__version__:
        raise RuntimeError('Model feature schema or runtime version mismatch.')
    ranges = manifest.get('demo_ranges', {})
    if set(ranges) != set(FEATURES) or any(not isinstance(v, list) or len(v) != 2 or not all(isinstance(x, (int, float)) and np.isfinite(x) for x in v) or v[0] >= v[1] for v in ranges.values()):
        raise RuntimeError('Missing or invalid demo range specification.')
    if not isinstance(manifest.get('sha256'), str) or not hmac.compare_digest(hashlib.sha256(artifact).hexdigest(), manifest['sha256']):
        raise RuntimeError('Model integrity mismatch.')
    model = pickle.loads(artifact)
    if not hasattr(model, 'feature_names_in_') or list(model.feature_names_in_) != FEATURES:
        raise RuntimeError('Loaded model schema mismatch.')
    return model, manifest

@asynccontextmanager
async def lifespan(app):
    app.state.model, app.state.manifest = load_artifact(Path(os.getenv('MODEL_DIR', str(ROOT / 'models'))))
    yield

app = FastAPI(lifespan=lifespan)

class SensorData(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)
    engine_temp_c: float = Field(strict=True, ge=-100000, le=100000)
    cabin_pressure_psi: float = Field(strict=True, ge=-100000, le=100000)
    vibration_rpm: float = Field(strict=True, ge=-1000000, le=1000000)

@app.get('/health')
def health():
    return {'status': 'READY', 'model_version': app.state.manifest['model_version']}

@app.post('/predict')
def predict_anomaly(data: SensorData):
    df = pd.DataFrame([[getattr(data, feature) for feature in FEATURES]], columns=FEATURES)
    model = app.state.model
    score = float(model.decision_function(df)[0])
    if not np.isfinite(score):
        raise RuntimeError('Non-finite model score.')
    # These are simulator bounds, not certified aircraft operating limits.
    reasons = [feature for feature in FEATURES if not app.state.manifest['demo_ranges'][feature][0] <= getattr(data, feature) <= app.state.manifest['demo_ranges'][feature][1]]
    return {'is_anomaly': bool(reasons) or score < 0, 'model_is_anomaly': score < 0, 'rule_anomaly': bool(reasons),
        'rule_features': reasons, 'score': score, 'model_version': app.state.manifest['model_version']}
