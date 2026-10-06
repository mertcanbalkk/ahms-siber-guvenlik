"""Independent synthetic comparison; not an aircraft or cyberattack benchmark."""
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from ai_service import load_artifact, FEATURES, ROOT

def evaluate(folder):
    model, manifest = load_artifact(folder)
    rng = np.random.default_rng(2026)
    normal = pd.DataFrame({FEATURES[0]: rng.uniform(600, 850, 10000), FEATURES[1]: rng.uniform(10.5, 11.5, 10000), FEATURES[2]: rng.uniform(2500, 3500, 10000)})
    abnormal = normal.iloc[:1000].copy()
    abnormal[FEATURES[0]] = 5000.0
    rows = pd.concat([normal, abnormal], ignore_index=True)
    truth = np.r_[np.zeros(len(normal), dtype=bool), np.ones(len(abnormal), dtype=bool)]
    learned = model.decision_function(rows) < 0
    rules = np.zeros(len(rows), dtype=bool)
    for feature, (low, high) in manifest['demo_ranges'].items():
        rules |= (rows[feature] < low) | (rows[feature] > high)
    result = {'scope': 'Independent synthetic normal samples and 5000 C injections; no cyberattack classification.', 'seed': 2026, 'normal_samples': len(normal), 'outlier_samples': len(abnormal), 'model_version': manifest['model_version']}
    for name, pred in [('isolation_forest', learned), ('demo_range_rule', rules), ('combined', learned | rules)]:
        tp, fp, fn, tn = (int(np.sum(pred & truth)), int(np.sum(pred & ~truth)), int(np.sum(~pred & truth)), int(np.sum(~pred & ~truth)))
        result[name] = {'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn, 'precision': tp / (tp + fp) if tp + fp else None, 'recall': tp / (tp + fn) if tp + fn else None, 'false_positive_rate': fp / len(normal)}
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--model-dir', type=Path, default=ROOT / 'models')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = evaluate(args.model_dir)
    if args.output:
        args.output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))
