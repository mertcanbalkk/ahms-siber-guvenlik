'use strict';
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { Kafka } = require('kafkajs');
const express = require('express');
const { Server } = require('socket.io');
const { loadConfig } = require('./lib/config');
const { validateRegistry } = require('./lib/protocol');
const { State } = require('./lib/state');
const { predict } = require('./lib/ai_client');
const { processMessage, drainOutbox } = require('./lib/gateway');

async function main() {
  const config = loadConfig();
  const loadRegistry = () => validateRegistry(JSON.parse(fs.readFileSync(config.registryPath, 'utf8')));
  loadRegistry(); // Configuration errors stop startup; never substitute an empty or permissive registry.
  const state = new State(config.statePath);
  const health = { kafka: 'STARTING', ai: 'UNKNOWN', last_received_ms: null, last_verified_ms: null, last_error: null };
  const app = express();
  app.disable('x-powered-by');
  app.use((_req, res, next) => {
    res.set({ 'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'none'", 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store' }); next();
  });
  app.get('/health', (_req, res) => res.status(health.kafka === 'READY' && health.ai === 'READY' ? 200 : 503).json({ ...health, ...state.stats(), now_ms: Date.now() }));
  app.get('/live', (_req, res) => res.status(health.kafka === 'READY' ? 200 : 503).json({ kafka: health.kafka }));
  app.get('/metrics', (_req, res) => {
    const lines = ['# TYPE ahms_events_total counter'];
    for (const row of state.counts()) lines.push(`ahms_events_total{verdict="${row.verdict}"} ${row.total}`);
    lines.push('# TYPE ahms_outbox_pending gauge', `ahms_outbox_pending ${state.stats().pending}`,
      '# TYPE ahms_kafka_ready gauge', `ahms_kafka_ready ${health.kafka === 'READY' ? 1 : 0}`,
      '# TYPE ahms_ai_last_result_ready gauge', `ahms_ai_last_result_ready ${health.ai === 'READY' ? 1 : 0}`);
    res.type('text/plain').send(lines.join('\n') + '\n');
  });
  app.get('/api/events', (_req, res) => res.json(state.recent()));
  app.use(express.static(path.join(config.root, 'public')));
  const server = http.createServer(app);
  const allowedOrigins = new Set([`http://localhost:${config.port}`, `http://127.0.0.1:${config.port}`]);
  const io = new Server(server, { allowRequest: (req, callback) => callback(null, !req.headers.origin || allowedOrigins.has(req.headers.origin)) });
  io.on('connection', socket => socket.emit('snapshot', { events: state.recent(), health: { ...health, ...state.stats(), now_ms: Date.now() } }));
  const kafka = new Kafka(config.kafka);
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  const consumer = kafka.consumer({ groupId: 'ahms-security-gateway-v2', allowAutoTopicCreation: false, sessionTimeout: 45000 });
  const admin = kafka.admin();
  let closed = false;
  const heartbeatTimer = setInterval(() => io.emit('health', { ...health, ...state.stats(), now_ms: Date.now() }), 1000);
  const publish = (topic, key, value) => producer.send({ topic, acks: -1, messages: [{ key, value }] });
  const onEvent = (event, topic) => {
    if (topic === config.topics.events) return;
    delete event.raw_b64; io.emit('security_event', event);
  };
  async function close() {
    if (closed) return; closed = true;
    clearInterval(heartbeatTimer);
    await consumer.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    await admin.disconnect().catch(() => {});
    await new Promise(resolve => io.close(resolve));
    state.close();
  }
  process.once('SIGINT', () => close().then(() => process.exit(0)));
  process.once('SIGTERM', () => close().then(() => process.exit(0)));
  consumer.on(consumer.events.CRASH, ({ payload }) => { health.kafka = 'ERROR'; health.last_error = 'KAFKA_CONSUMER_CRASH'; console.error(payload.error); });
  consumer.on(consumer.events.GROUP_JOIN, () => { health.kafka = 'READY'; health.last_error = null; });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(config.port, config.host, resolve); });
  console.log(`AHMS laboratuvar paneli: http://${config.host}:${config.port}`);
  try {
    await admin.connect();
    const cluster = await admin.describeCluster();
    if (!cluster.clusterId) throw new Error('Kafka cluster identity unavailable.');
    if (process.env.CREATE_TOPICS !== 'false') {
      await admin.createTopics({ waitForLeaders: true, topics: Object.values(config.topics).map(topic => ({ topic, numPartitions: 1, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] })) });
    }
    await admin.disconnect();
    await producer.connect();
    await consumer.connect();
    // Deliver durable pending output from a previous run before consuming new input.
    await drainOutbox(state, publish, onEvent);
    await consumer.subscribe({ topic: config.topics.raw, fromBeginning: true });
    await consumer.run({ eachMessage: async ({ topic, partition, message }) => {
      health.last_received_ms = Date.now();
      const event = await processMessage({ raw: message.value, source: { cluster_id: cluster.clusterId, topic, partition, offset: message.offset }, registry: loadRegistry(), state,
        predict: data => predict(data, { url: config.aiUrl, timeoutMs: config.aiTimeoutMs }), topics: config.topics, limits: config.limits });
      if (event.verdict !== 'REJECTED') health.ai = event.verdict === 'UNKNOWN' ? 'ERROR' : 'READY';
      if (event.decision === 'ACCEPT') health.last_verified_ms = Date.now();
      await drainOutbox(state, publish, onEvent); // Throw on failure: Kafka must not acknowledge an output that was not durably queued/sent.
      console.log(JSON.stringify({ event_id: event.event_id, verdict: event.verdict, reason: event.reason, decision: event.decision }));
    }});
  } catch (err) {
    health.kafka = 'ERROR'; health.last_error = 'STARTUP_OR_OUTPUT_ERROR';
    console.error(err); await close(); throw err;
  }
}
if (require.main === module) main().catch(() => { process.exitCode = 1; });
module.exports = { main };
