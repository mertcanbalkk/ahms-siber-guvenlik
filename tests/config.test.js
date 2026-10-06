'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { kafkaConfig } = require('../lib/config');

test('required mTLS cannot silently downgrade to plaintext or partial credentials', () => {
  const names = ['KAFKA_REQUIRE_MTLS', 'KAFKA_SSL_CA', 'KAFKA_SSL_CERT', 'KAFKA_SSL_KEY'];
  const old = Object.fromEntries(names.map(name => [name, process.env[name]]));
  try {
    for (const name of names) delete process.env[name];
    process.env.KAFKA_REQUIRE_MTLS = 'true';
    assert.throws(kafkaConfig, /mTLS/);
    process.env.KAFKA_SSL_CA = 'missing-ca.pem';
    assert.throws(kafkaConfig, /mTLS/);
    process.env.KAFKA_SSL_CERT = 'missing-cert.pem';
    assert.throws(kafkaConfig, /mTLS/);
  } finally {
    for (const name of names) {
      if (old[name] === undefined) delete process.env[name]; else process.env[name] = old[name];
    }
  }
});
