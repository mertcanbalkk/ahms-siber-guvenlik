'use strict';
const el = id => document.getElementById(id);
const socket = io();
const seen = new Set();
let acknowledged = new Set();
try { acknowledged = new Set(JSON.parse(localStorage.getItem('ahms-acknowledged') || '[]')); } catch { /* Browser storage may be unavailable. */ }
let connected = false, health = {}, lastMeasurement = null;
const labels = { NORMAL: 'Normal', ANOMALY: 'Sensör anomalisi', UNKNOWN: 'Değerlendirilemedi', REJECTED: 'Doğrulama reddi' };
function showEvent(event, historical = false) {
  if (seen.has(event.event_id)) return;
  seen.add(event.event_id);
  const row = document.createElement('tr');
  const values = [new Date(event.received_at_ms).toLocaleTimeString('tr-TR'), event.telemetry?.sensor_id || '—', labels[event.verdict] || 'Bilinmiyor', event.decision === 'ACCEPT' ? 'Kabul' : 'Karantina', event.reason];
  for (const value of values) { const cell = document.createElement('td'); cell.textContent = String(value ?? '—'); row.appendChild(cell); }
  row.dataset.eventId = event.event_id;
  el('event-list').prepend(row);
  while (el('event-list').children.length > 100) { const old = el('event-list').lastElementChild; seen.delete(old.dataset.eventId); old.remove(); }
  if (event.decision === 'ACCEPT' && event.telemetry && (!lastMeasurement || event.received_at_ms >= lastMeasurement.received_at_ms)) {
    lastMeasurement = event;
    el('temp').textContent = event.telemetry.engine_temp_c.toFixed(2);
    el('pressure').textContent = event.telemetry.cabin_pressure_psi.toFixed(2);
    el('rpm').textContent = event.telemetry.vibration_rpm.toFixed(0);
  }
  if (event.verdict !== 'NORMAL' && !acknowledged.has(event.event_id)) {
    el('alert').hidden = false;
    el('alert').dataset.eventId = event.event_id;
    el('alert').textContent = `${labels[event.verdict] || 'Bilinmeyen sonuç'} · ${event.reason} · ${event.decision === 'QUARANTINE' ? 'Mesaj karantinaya alındı.' : 'Ölçüm gözlem için kabul edildi.'}`;
  }
  renderHealth();
}
function renderHealth() {
  const now = Date.now();
  el('connection').textContent = connected ? 'Panel bağlı' : 'Panel bağlantısı kesik';
  el('connection').className = `badge ${connected ? 'normal' : 'unknown'}`;
  el('kafka-state').textContent = health.kafka === 'READY' ? 'Bağlı' : (health.kafka || 'Bilinmiyor');
  el('ai-state').textContent = health.ai === 'READY' ? 'Son değerlendirme başarılı' : 'Değerlendirme yok / hata';
  el('pending').textContent = String(health.pending ?? '—');
  const age = lastMeasurement ? Math.max(0, now - lastMeasurement.timestamp_ms) : null;
  el('freshness').textContent = age === null ? 'Henüz yok' : `${Math.floor(age / 1000)} saniye önce${age > 5000 ? ' · GÜNCEL DEĞİL' : ''}`;
  const status = el('measurement-status');
  if (!connected || health.kafka !== 'READY' || health.ai !== 'READY' || age === null || age > 5000) {
    status.className = 'notice unknown'; status.textContent = 'UNKNOWN · Güncel ve tamamlanmış bir değerlendirme yok. Görünen değerler son kabul edilen ölçümdür.';
  } else {
    status.className = `notice ${lastMeasurement.verdict === 'NORMAL' ? 'normal' : 'warning'}`;
    status.textContent = `${labels[lastMeasurement.verdict]} · ${lastMeasurement.telemetry.sensor_id} · Bu sonuç siber saldırı veya uçuş güvenliği kanıtı değildir.`;
  }
}
socket.on('connect', () => { connected = true; renderHealth(); });
socket.on('disconnect', () => { connected = false; renderHealth(); });
socket.on('snapshot', snapshot => { health = snapshot.health; for (const event of snapshot.events) showEvent(event, true); renderHealth(); });
socket.on('health', value => { health = value; renderHealth(); });
socket.on('security_event', event => showEvent(event));
el('acknowledge').addEventListener('click', () => {
  if (el('alert').dataset.eventId) acknowledged.add(el('alert').dataset.eventId);
  try { localStorage.setItem('ahms-acknowledged', JSON.stringify([...acknowledged].slice(-100))); } catch { /* The current session still remembers it. */ }
  el('alert').hidden = true;
});
setInterval(renderHealth, 1000);
