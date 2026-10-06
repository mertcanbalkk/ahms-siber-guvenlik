"""Example AHMS adapter: reads only the verified topic with its own Kafka identity."""
import json
import os
from kafka import KafkaConsumer
from kafka_security import ssl_options

def main():
    consumer = KafkaConsumer('flight_telemetry_verified',
        bootstrap_servers=os.getenv('KAFKA_BROKERS', '127.0.0.1:29092').split(','),
        group_id='ahms-verified-reader', auto_offset_reset='earliest', enable_auto_commit=False,
        **ssl_options(require=True))
    try:
        for record in consumer:
            event = json.loads(record.value)
            if event.get('decision') != 'ACCEPT' or not event.get('event_id'):
                raise ValueError('Unexpected verified event')
            # Real AHMS adapters must atomically deduplicate event_id and store the measurement.
            # This adapter only demonstrates access; it does not write a business database.
            print(json.dumps(event, ensure_ascii=False), flush=True)
            consumer.commit()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
