from kafka import KafkaProducer
import json
from datetime import datetime, timezone

DLQ_TOPIC = "booking-events-dlq"

_dlq_producer = None

def get_dlq_producer():
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return _dlq_producer


def send_to_dlq(original_message: dict, error: str, consumer_name: str):
    """Send a failed message to the dead letter queue with error metadata."""
    dlq_event = {
        "original_event": original_message,
        "error": error,
        "consumer": consumer_name,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    get_dlq_producer().send(DLQ_TOPIC, value=dlq_event)
    print(f"  [DLQ] Sent failed event to {DLQ_TOPIC}: {error}")
