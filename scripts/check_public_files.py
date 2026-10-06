"""Reject local secrets and runtime artifacts in tracked files; never print file contents."""
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
files = subprocess.check_output(['git', 'ls-files', '-z'], cwd=root).decode('utf-8').split('\0')
blocked = []
for name in filter(None, files):
    path = PurePosixPath(name)
    if (any(part in {'security', 'data', 'models', 'node_modules', '.venv', '__pycache__', '.ahms-backups'} for part in path.parts)
        or (path.name.startswith('.env') and path.name != '.env.example')
        or path.suffix.lower() in {'.pem', '.key', '.p12', '.pfx', '.jks', '.crt', '.cer', '.pkl', '.db', '.sqlite', '.pyc', '.log'}
        or path.name == 'store-password'):
        blocked.append(name)
        continue
    if path.suffix.lower() in {'.jpg', '.png'}:
        continue
    text = (root / name).read_text(encoding='utf-8', errors='replace')
    if re.search(r'-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----', text) or re.search(r'\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{30,})\b', text):
        blocked.append(name)
if blocked:
    print('Public repository check failed; remove these tracked files:', *blocked, sep='\n')
    sys.exit(1)
print(f'Public repository check passed: {len(list(filter(None, files)))} tracked files.')
