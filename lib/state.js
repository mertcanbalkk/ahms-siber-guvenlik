'use strict';
const { DatabaseSync } = require('node:sqlite');
const fs = require('node:fs');
const path = require('node:path');
class State {
  constructor(filename) {
    if (filename !== ':memory:') fs.mkdirSync(path.dirname(path.resolve(filename)), { recursive: true });
    this.db = new DatabaseSync(filename, { timeout: 5000 });
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS replay (producer TEXT NOT NULL, key_id TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY(producer,key_id));
      CREATE TABLE IF NOT EXISTS ids (producer TEXT NOT NULL, key_id TEXT NOT NULL, message_id TEXT NOT NULL, PRIMARY KEY(producer,key_id,message_id));
      CREATE TABLE IF NOT EXISTS audit (source_id TEXT PRIMARY KEY, event_json TEXT NOT NULL, created_ms INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, topic TEXT NOT NULL, value TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0);
      CREATE INDEX IF NOT EXISTS pending ON outbox(sent,id);`);
  }
  existing(sourceId) { const row = this.db.prepare('SELECT event_json FROM audit WHERE source_id=?').get(sourceId); return row ? JSON.parse(row.event_json) : null; }
  replayed(e) {
    const row = this.db.prepare('SELECT sequence FROM replay WHERE producer=? AND key_id=?').get(e.producer_id, e.key_id);
    return (row && e.sequence <= row.sequence) || Boolean(this.db.prepare('SELECT 1 FROM ids WHERE producer=? AND key_id=? AND message_id=?').get(e.producer_id, e.key_id, e.message_id));
  }
  record(sourceId, event, envelope, topics) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const previous = this.existing(sourceId);
      if (previous) { this.db.exec('COMMIT'); return previous; }
      if (envelope) {
        if (this.replayed(envelope)) {
          event = { ...event, verdict: 'REJECTED', reason: 'REPLAY', decision: 'QUARANTINE', telemetry: null, score: null, model_version: null, model_is_anomaly: null, rule_anomaly: null };
        } else {
          this.db.prepare('INSERT INTO replay VALUES(?,?,?) ON CONFLICT(producer,key_id) DO UPDATE SET sequence=excluded.sequence').run(envelope.producer_id, envelope.key_id, envelope.sequence);
          this.db.prepare('INSERT INTO ids VALUES(?,?,?)').run(envelope.producer_id, envelope.key_id, envelope.message_id);
        }
      }
      this.db.prepare('INSERT INTO audit VALUES(?,?,?)').run(sourceId, JSON.stringify(event), event.received_at_ms);
      const topic = event.decision === 'QUARANTINE' ? topics.quarantine : topics.verified;
      const routed = event.decision === 'QUARANTINE' ? event : { ...event, raw_b64: undefined };
      this.db.prepare('INSERT INTO outbox(event_id,topic,value) VALUES(?,?,?)').run(event.event_id, topic, JSON.stringify(routed));
      this.db.prepare('INSERT INTO outbox(event_id,topic,value) VALUES(?,?,?)').run(event.event_id, topics.events, JSON.stringify({ ...event, raw_b64: undefined }));
      this.db.exec('COMMIT'); return event;
    } catch (err) { this.db.exec('ROLLBACK'); throw err; }
  }
  pending() { return this.db.prepare('SELECT * FROM outbox WHERE sent=0 ORDER BY id LIMIT 100').all(); }
  markSent(id) { this.db.prepare('UPDATE outbox SET sent=1 WHERE id=?').run(id); }
  recent(limit = 100) { return this.db.prepare('SELECT event_json FROM audit ORDER BY created_ms DESC, rowid DESC LIMIT ?').all(limit).map(r => { const e = JSON.parse(r.event_json); delete e.raw_b64; return e; }).reverse(); }
  stats() { return this.db.prepare('SELECT COUNT(*) AS pending FROM outbox WHERE sent=0').get(); }
  counts() { return this.db.prepare("SELECT json_extract(event_json,'$.verdict') AS verdict, COUNT(*) AS total FROM audit GROUP BY verdict").all(); }
  close() { this.db.close(); }
}
module.exports = { State };
