'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = path.resolve(__dirname, '..');
const folder = path.join(root, 'security');
fs.mkdirSync(folder, { recursive: true, mode: 0o700 });
const gateway = path.join(folder, 'gateway-keys.json');
const producer = path.join(folder, 'producer-key.json');
if (fs.existsSync(gateway) || fs.existsSync(producer)) {
  throw new Error('Anahtar dosyası zaten var. Otomatik üzerine yazılmaz; rotasyon ve replay durumunu birlikte planlayın.');
}
const key = { key_id: 'eng-01-v1', producer_id: 'engine-simulator', sensor_id: 'ENG-01', secret_hex: crypto.randomBytes(32).toString('hex') };
fs.writeFileSync(gateway, JSON.stringify({ keys: [{ ...key, enabled: true }] }, null, 2), { mode: 0o600, flag: 'wx' });
fs.writeFileSync(producer, JSON.stringify(key, null, 2), { mode: 0o600, flag: 'wx' });
console.log('Yerel laboratuvar anahtarları oluşturuldu. Anahtarları sürüm kontrolüne veya dağıtım ZIP dosyasına eklemeyin.');
