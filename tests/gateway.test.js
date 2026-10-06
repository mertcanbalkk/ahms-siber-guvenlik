'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');
const { verify, sign, validateRegistry } = require('../lib/protocol');
const { State } = require('../lib/state');
const { processMessage, drainOutbox } = require('../lib/gateway');
const { predict } = require('../lib/ai_client');
const key = { key_id: 'test-key', producer_id: 'test-producer', sensor_id: 'ENG-01', secret_hex: '11'.repeat(32), enabled: true };
const registry = validateRegistry({ keys: [key] });
const topics = { verified: 'verified', quarantine: 'quarantine', events: 'events' };
const telemetry = { sensor_id: 'ENG-01', engine_temp_c: 720, cabin_pressure_psi: 11, vibration_rpm: 3000 };
function envelope(sequence = 1, payload = telemetry, now = Date.now(), messageId = crypto.randomBytes(16).toString('hex')) {
  const e = { schema_version: 1, key_id: key.key_id, producer_id: key.producer_id, sequence, timestamp_ms: now, message_id: messageId, payload_b64: Buffer.from(JSON.stringify(payload)).toString('base64') };
  e.mac = sign(e, key.secret_hex); return e;
}
function run(state, e, offset, prediction = { verdict: 'NORMAL', reason: 'MODEL_NORMAL', score: 0.1, model_version: 'test' }) {
  return processMessage({ state, registry, raw: Buffer.from(typeof e === 'string' ? e : JSON.stringify(e)), source: { topic: 'raw', partition: 0, offset: String(offset) }, topics, predict: async () => prediction });
}
test('valid signed message is accepted; a changed payload and an unsigned source are quarantined', async () => {
  const state = new State(':memory:');
  try {
    assert.equal((await run(state, envelope(), 0)).decision, 'ACCEPT');
    const tampered = envelope(2); tampered.payload_b64 = Buffer.from(JSON.stringify({ ...telemetry, engine_temp_c: 5000 })).toString('base64');
    assert.equal((await run(state, tampered, 1)).reason, 'INVALID_MAC');
    const unknown = envelope(3); unknown.producer_id = 'intruder'; unknown.mac = sign(unknown, key.secret_hex);
    assert.equal((await run(state, unknown, 2)).reason, 'UNAUTHORIZED_PRODUCER');
  } finally { state.close(); }
});
test('replay protection and sequence ordering survive a process restart; same Kafka offset is idempotent', async () => {
  const folder = fs.mkdtempSync(path.join(os.tmpdir(), 'ahms-state-'));
  const filename = path.join(folder, 'state.sqlite'); let state = new State(filename);
  try {
    const e = envelope(10); const first = await run(state, e, 0);
    assert.equal((await run(state, e, 0)).event_id, first.event_id);
    assert.equal(state.pending().length, 2);
    state.close(); state = new State(filename);
    assert.equal((await run(state, e, 1)).reason, 'REPLAY');
    assert.equal((await run(state, envelope(9), 2)).reason, 'REPLAY');
    assert.equal((await run(state, envelope(11, telemetry, Date.now(), e.message_id), 3)).reason, 'REPLAY');
    assert.equal((await run(state, envelope(11), 4)).decision, 'ACCEPT');
  } finally { state.close(); fs.rmSync(folder, { recursive: true }); }
});
test('outbox remains pending when Kafka output fails, then sends after retry without losing audit', async () => {
  const state = new State(':memory:');
  try {
    await run(state, envelope(), 0);
    await assert.rejects(drainOutbox(state, async () => { throw new Error('Kafka down'); }));
    assert.equal(state.stats().pending, 2);
    const delivered = []; await drainOutbox(state, async (topic, id, value) => delivered.push({ topic, id, value }));
    assert.equal(delivered.length, 2); assert.equal(state.stats().pending, 0); assert.equal(state.recent().length, 1);
  } finally { state.close(); }
});
test('bad JSON, null values, missing fields, wrong sensor binding and expired messages are rejected', async () => {
  const state = new State(':memory:');
  try {
    assert.equal((await run(state, '{bad', 0)).reason, 'INVALID_JSON');
    assert.equal((await run(state, 'null', 1)).reason, 'INVALID_ENVELOPE');
    const missing = envelope(); delete missing.producer_id;
    assert.equal((await run(state, missing, 2)).reason, 'INVALID_ENVELOPE');
    assert.equal((await run(state, envelope(2, { ...telemetry, sensor_id: '<b>fake</b>' }), 3)).reason, 'INVALID_TELEMETRY');
    assert.equal((await run(state, envelope(3, telemetry, Date.now() - 121000), 4)).reason, 'EXPIRED_MESSAGE');
    assert.equal((await run(state, envelope(4, telemetry, Date.now() + 31000), 5)).reason, 'FUTURE_MESSAGE');
    assert.equal((await run(state, envelope(5, { ...telemetry, cabin_pressure_psi: '11' }), 6)).reason, 'INVALID_TELEMETRY');
  } finally { state.close(); }
});
test('ML anomaly is observed without automatically disabling a sensor; unknown AI output is quarantined', async () => {
  const state = new State(':memory:');
  try {
    const anomaly = await run(state, envelope(1, { ...telemetry, engine_temp_c: 5000 }), 0, { verdict: 'ANOMALY', reason: 'MODEL_ANOMALY', score: -0.1, model_version: 'test' });
    assert.equal(anomaly.verdict, 'ANOMALY'); assert.equal(anomaly.decision, 'ACCEPT');
    const unknown = await run(state, envelope(2), 1, { verdict: 'UNKNOWN', reason: 'AI_UNAVAILABLE' });
    assert.equal(unknown.verdict, 'UNKNOWN'); assert.equal(unknown.decision, 'QUARANTINE');
  } finally { state.close(); }
});
test('real HTTP client validates 422, 500, JSON schema, malformed JSON and bounded timeout', async () => {
  const server = http.createServer((req, res) => {
    if (req.url === '/timeout') return;
    if (req.url === '/422') { res.writeHead(422); return res.end(JSON.stringify({ detail: 'invalid' })); }
    if (req.url === '/500') { res.writeHead(500); return res.end('server error'); }
    if (req.url === '/bad') return res.end('{bad');
    if (req.url === '/schema') return res.end(JSON.stringify({ is_anomaly: 'false' }));
    if (req.url === '/large') return res.end('x'.repeat(8192));
    res.end(JSON.stringify({ is_anomaly: false, score: 0.1, model_version: 'test' }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    for (const route of ['/422', '/500', '/bad', '/schema', '/large']) {
      assert.equal((await predict(telemetry, { url: base + route })).verdict, 'UNKNOWN');
    }
    const start = Date.now(); const timed = await predict(telemetry, { url: base + '/timeout', timeoutMs: 80 });
    assert.equal(timed.reason, 'AI_TIMEOUT'); assert.ok(Date.now() - start < 1500);
    assert.equal((await predict(telemetry, { url: base + '/ok' })).verdict, 'NORMAL');
  } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});
test('disabled keys and a MAC over changed envelope metadata are rejected', () => {
  const e = envelope();
  assert.throws(() => verify(Buffer.from(JSON.stringify(e)), { keys: [{ ...key, enabled: false }] }), /UNAUTHORIZED_PRODUCER/);
  e.sequence += 1;
  assert.throws(() => verify(Buffer.from(JSON.stringify(e)), registry), /INVALID_MAC/);
});
test('an offset reused by a different Kafka cluster cannot reuse an old accepted decision', async () => {
  const state = new State(':memory:');
  try {
    const first = await run(state, envelope(1), 0);
    const second = await processMessage({ state, registry, raw: Buffer.from(JSON.stringify(envelope(2))), source: { cluster_id: 'different-cluster', topic: 'raw', partition: 0, offset: '0' }, topics, predict: async () => ({ verdict: 'NORMAL', reason: 'MODEL_NORMAL' }) });
    assert.notEqual(first.event_id, second.event_id);
    assert.equal(second.sequence, 2);
    assert.equal(state.recent().length, 2);
  } finally { state.close(); }
});
