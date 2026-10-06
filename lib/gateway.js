'use strict';
const crypto = require('node:crypto');
const { verify, Rejection } = require('./protocol');
function sourceId(source) { return crypto.createHash('sha256').update(JSON.stringify([source.cluster_id ?? 'local-test', source.topic, source.partition, source.offset])).digest('hex'); }
async function processMessage({ raw, source, registry, state, predict, topics, now = Date.now(), limits = {} }) {
  const id = sourceId(source), previous = state.existing(id);
  if (previous) return previous; // Kafka redelivery of the same offset resumes the durable outbox.
  const input = Buffer.isBuffer(raw) ? raw : Buffer.alloc(0);
  let verified, result;
  try {
    verified = verify(input, registry, now, limits);
    if (state.replayed(verified.envelope)) result = { verdict: 'REJECTED', reason: 'REPLAY' };
    else result = await predict(verified.telemetry);
  } catch (err) {
    if (!(err instanceof Rejection)) throw err;
    result = { verdict: 'REJECTED', reason: err.reason };
  }
  const decision = ['REJECTED', 'UNKNOWN'].includes(result.verdict) ? 'QUARANTINE' : 'ACCEPT';
  const event = {
    event_id: id, source, received_at_ms: now,
    producer_id: verified?.envelope.producer_id ?? null, message_id: verified?.envelope.message_id ?? null,
    timestamp_ms: verified?.envelope.timestamp_ms ?? null, sequence: verified?.envelope.sequence ?? null,
    verdict: result.verdict, reason: result.reason, decision,
    telemetry: verified && result.verdict !== 'REJECTED' ? verified.telemetry : null,
    score: result.score ?? null, model_version: result.model_version ?? null,
    model_is_anomaly: result.model_is_anomaly ?? null, rule_anomaly: result.rule_anomaly ?? null,
    raw_sha256: crypto.createHash('sha256').update(input).digest('hex'), raw_size: input.length,
    raw_b64: input.subarray(0, 65536).toString('base64'), raw_truncated: input.length > 65536
  };
  return state.record(id, event, verified && result.verdict !== 'REJECTED' ? verified.envelope : null, topics);
}
async function drainOutbox(state, send, onEvent = () => {}) {
  while (true) {
    const rows = state.pending(); if (!rows.length) return;
    for (const row of rows) {
      await send(row.topic, row.event_id, row.value);
      state.markSent(row.id); // Crash after send may duplicate delivery; consumers deduplicate event_id.
      const event = JSON.parse(row.value);
      onEvent(event, row.topic);
    }
  }
}
module.exports = { processMessage, drainOutbox, sourceId };
