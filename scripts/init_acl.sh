#!/usr/bin/env bash
set -euo pipefail
broker="${KAFKA_BROKERS:-kafka:9092}"
config=/run/tls/client.properties
for topic in flight_telemetry_raw flight_telemetry_verified flight_telemetry_quarantine security_events; do
  kafka-topics --bootstrap-server "$broker" --command-config "$config" --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1 --config retention.ms=604800000
done
acl() { kafka-acls --bootstrap-server "$broker" --command-config "$config" --add "$@"; }
acl --allow-principal 'User:CN=producer' --operation Write --operation Describe --topic flight_telemetry_raw
acl --allow-principal 'User:CN=gateway' --operation Read --operation Describe --topic flight_telemetry_raw
acl --allow-principal 'User:CN=gateway' --operation Read --group ahms-security-gateway-v2
for topic in flight_telemetry_verified flight_telemetry_quarantine security_events; do
  acl --allow-principal 'User:CN=gateway' --operation Write --operation Describe --topic "$topic"
done
acl --allow-principal 'User:CN=reader' --operation Read --operation Describe --topic flight_telemetry_verified
acl --allow-principal 'User:CN=reader' --operation Read --group ahms-verified-reader
echo 'AHMS konuları ve rol yetkileri hazır.'
