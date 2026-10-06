'use strict';
const crypto = require('node:crypto');
const { TextDecoder } = require('node:util');
const ID = /^[A-Za-z0-9_-]{1,64}$/;
const validId = value => typeof value === 'string' && ID.test(value);
const fields = ['schema_version', 'key_id', 'producer_id', 'sequence', 'timestamp_ms', 'message_id', 'payload_b64', 'mac'];
class Rejection extends Error { constructor(reason) { super(reason); this.reason = reason; } }
function reject(reason) { throw new Rejection(reason); }
function exactKeys(value, keys) {
  return value && typeof value === 'object' && !Array.isArray(value) &&
    Object.keys(value).length === keys.length && keys.every(k => Object.hasOwn(value, k));
}
function signingBytes(e) {
  return Buffer.from(['AHMSv1', e.key_id, e.producer_id, e.sequence, e.timestamp_ms, e.message_id, e.payload_b64].join('\n'), 'utf8');
}
function sign(e, secretHex) { return crypto.createHmac('sha256', Buffer.from(secretHex, 'hex')).update(signingBytes(e)).digest('hex'); }
function validateRegistry(registry) {
  if (!registry || !Array.isArray(registry.keys) || registry.keys.length === 0) throw new Error('Anahtar kayıt dosyası boş veya geçersiz.');
  const seen = new Set();
  for (const key of registry.keys) {
    if (!validId(key.key_id) || !validId(key.producer_id) || !validId(key.sensor_id) ||
      !/^[0-9a-f]{64}$/.test(key.secret_hex) || typeof key.enabled !== 'boolean' || seen.has(key.key_id)) {
      throw new Error('Anahtar kaydı geçersiz veya key_id yinelenmiş.');
    }
    seen.add(key.key_id);
  }
  return registry;
}
function verify(raw, registry, now = Date.now(), limits = {}) {
  const maxAgeMs = limits.maxAgeMs ?? 120000, maxFutureMs = limits.maxFutureMs ?? 30000;
  if (!Buffer.isBuffer(raw) || raw.length === 0 || raw.length > 65536) reject('INVALID_MESSAGE_SIZE');
  let e;
  try { e = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(raw)); }
  catch { reject('INVALID_JSON'); }
  if (!exactKeys(e, fields) || e.schema_version !== 1 || !validId(e.key_id) || !validId(e.producer_id) ||
    !Number.isSafeInteger(e.sequence) || e.sequence < 1 || !Number.isSafeInteger(e.timestamp_ms) || e.timestamp_ms < 0 ||
    typeof e.message_id !== 'string' || !/^[0-9a-f]{32}$/.test(e.message_id) ||
    typeof e.payload_b64 !== 'string' || typeof e.mac !== 'string' || !/^[0-9a-f]{64}$/.test(e.mac)) reject('INVALID_ENVELOPE');
  const key = registry.keys.find(k => k.key_id === e.key_id && k.enabled);
  if (!key || key.producer_id !== e.producer_id) reject('UNAUTHORIZED_PRODUCER');
  const expected = Buffer.from(sign(e, key.secret_hex), 'hex');
  if (!crypto.timingSafeEqual(expected, Buffer.from(e.mac, 'hex'))) reject('INVALID_MAC');
  if (e.timestamp_ms < now - maxAgeMs) reject('EXPIRED_MESSAGE');
  if (e.timestamp_ms > now + maxFutureMs) reject('FUTURE_MESSAGE');
  let payload;
  try {
    const bytes = Buffer.from(e.payload_b64, 'base64');
    if (bytes.toString('base64') !== e.payload_b64 || bytes.length > 16384) reject('INVALID_PAYLOAD');
    payload = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  } catch (err) { if (err instanceof Rejection) throw err; reject('INVALID_PAYLOAD'); }
  if (!exactKeys(payload, ['sensor_id', 'engine_temp_c', 'cabin_pressure_psi', 'vibration_rpm']) || payload.sensor_id !== key.sensor_id ||
    !['engine_temp_c', 'cabin_pressure_psi', 'vibration_rpm'].every(f => typeof payload[f] === 'number' && Number.isFinite(payload[f])) ||
    Math.abs(payload.engine_temp_c) > 100000 || Math.abs(payload.cabin_pressure_psi) > 100000 || Math.abs(payload.vibration_rpm) > 1000000) reject('INVALID_TELEMETRY');
  return { envelope: e, telemetry: payload };
}
module.exports = { Rejection, verify, sign, signingBytes, validateRegistry };
