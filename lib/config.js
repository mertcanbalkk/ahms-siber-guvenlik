'use strict';
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
function integer(name, fallback, min, max) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`${name} geçersiz.`);
  return value;
}
function kafkaConfig() {
  const config = { clientId: 'ahms-security-gateway', brokers: (process.env.KAFKA_BROKERS || '127.0.0.1:29092').split(',').map(x => x.trim()), requestTimeout: 10000 };
  const ca = process.env.KAFKA_SSL_CA, cert = process.env.KAFKA_SSL_CERT, key = process.env.KAFKA_SSL_KEY;
  if ((cert || key || process.env.KAFKA_REQUIRE_MTLS === 'true') && !(ca && cert && key)) throw new Error('Kafka mTLS için CA, CERT ve KEY birlikte gerekir.');
  if (ca) config.ssl = { rejectUnauthorized: true, ca: [fs.readFileSync(ca)], ...(cert ? { cert: fs.readFileSync(cert), key: fs.readFileSync(key) } : {}) };
  return config;
}
function loadConfig() {
  const topics = { raw: process.env.RAW_TOPIC || 'flight_telemetry_raw', verified: process.env.VERIFIED_TOPIC || 'flight_telemetry_verified', quarantine: process.env.QUARANTINE_TOPIC || 'flight_telemetry_quarantine', events: process.env.EVENT_TOPIC || 'security_events' };
  if (new Set(Object.values(topics)).size !== 4 || Object.values(topics).some(x => !/^[A-Za-z0-9_.-]{1,120}$/.test(x))) throw new Error('Dört konu adı geçerli ve birbirinden farklı olmalı.');
  const aiUrl = process.env.AI_URL || 'http://127.0.0.1:8000/predict';
  if (!['http:', 'https:'].includes(new URL(aiUrl).protocol)) throw new Error('AI_URL HTTP(S) olmalı.');
  return { topics, aiUrl, aiTimeoutMs: integer('AI_TIMEOUT_MS', 1500, 50, 30000),
    host: process.env.HTTP_HOST || '127.0.0.1', port: integer('HTTP_PORT', 3000, 1, 65535),
    registryPath: path.resolve(root, process.env.KEY_REGISTRY_PATH || 'security/gateway-keys.json'),
    statePath: path.resolve(root, process.env.STATE_PATH || 'data/gateway.sqlite'),
    limits: { maxAgeMs: integer('MAX_AGE_MS', 120000, 1000, 86400000), maxFutureMs: integer('MAX_FUTURE_MS', 30000, 0, 300000) },
    kafka: kafkaConfig(), root };
}
module.exports = { loadConfig, kafkaConfig };
