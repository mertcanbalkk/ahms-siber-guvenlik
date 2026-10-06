'use strict';
async function predict(telemetry, { url, timeoutMs = 1500, fetchImpl = fetch }) {
  try {
    const response = await fetchImpl(url, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ engine_temp_c: telemetry.engine_temp_c, cabin_pressure_psi: telemetry.cabin_pressure_psi, vibration_rpm: telemetry.vibration_rpm }),
      signal: AbortSignal.timeout(timeoutMs), redirect: 'error'
    });
    if (!response.ok) { await response.body?.cancel(); return { verdict: 'UNKNOWN', reason: `AI_HTTP_${response.status}` }; }
    // Bound the response body as well as the request; an unbounded JSON response can exhaust memory.
    const reader = response.body.getReader();
    const chunks = []; let bytes = 0;
    try {
      while (true) {
        const item = await reader.read(); if (item.done) break;
        bytes += item.value.length;
        if (bytes > 4096) { await reader.cancel(); return { verdict: 'UNKNOWN', reason: 'AI_INVALID_RESPONSE' }; }
        chunks.push(Buffer.from(item.value));
      }
    } finally { reader.releaseLock(); }
    let result;
    try { result = JSON.parse(Buffer.concat(chunks).toString('utf8')); }
    catch { return { verdict: 'UNKNOWN', reason: 'AI_INVALID_RESPONSE' }; }
    if (!result || typeof result.is_anomaly !== 'boolean' || typeof result.score !== 'number' || !Number.isFinite(result.score) ||
      typeof result.model_version !== 'string' || !/^[A-Za-z0-9_.-]{1,80}$/.test(result.model_version) ||
      (Object.hasOwn(result, 'rule_anomaly') && typeof result.rule_anomaly !== 'boolean') ||
      (Object.hasOwn(result, 'model_is_anomaly') && typeof result.model_is_anomaly !== 'boolean')) {
      return { verdict: 'UNKNOWN', reason: 'AI_INVALID_RESPONSE' };
    }
    return { verdict: result.is_anomaly ? 'ANOMALY' : 'NORMAL', reason: result.rule_anomaly ? 'OUTSIDE_DEMO_RANGE' : (result.is_anomaly ? 'MODEL_ANOMALY' : 'MODEL_NORMAL'), score: result.score, model_version: result.model_version,
      model_is_anomaly: result.model_is_anomaly ?? result.is_anomaly, rule_anomaly: result.rule_anomaly ?? false };
  } catch (err) {
    return { verdict: 'UNKNOWN', reason: ['TimeoutError', 'AbortError'].includes(err.name) ? 'AI_TIMEOUT' : 'AI_UNAVAILABLE' };
  }
}
module.exports = { predict };
