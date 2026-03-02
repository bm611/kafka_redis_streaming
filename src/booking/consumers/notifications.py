from kafka import KafkaConsumer
import json
import redis

from booking.schema import validate_booking_event
from booking.dlq import send_to_dlq

CONSUMER_NAME = "notification-service"

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

consumer = KafkaConsumer(
    'booking-events',
    bootstrap_servers='localhost:9092',
    group_id=CONSUMER_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Notification Service listening...")
for msg in consumer:
    try:
        b = msg.value

        errors = validate_booking_event(b)
        if errors:
            send_to_dlq(b, f"schema validation failed: {errors}", CONSUMER_NAME)
            continue

        # Idempotency check — skip if notification already sent for this booking
        dedup_key = f"notification_sent:{b['booking_id']}"
        if r.exists(dedup_key):
            print(f"  [SKIP] Duplicate — notification already sent for {b['booking_id'][:8]}...")
            continue

        # "Send" the notification
        print(f"  [EMAIL] Booking confirmed for user {b['user_id'][:8]}... "
              f"at {b['hotel_name']} checking in {b['check_in_date']}")

        # Mark as sent (TTL 7 days — long enough to cover any Kafka redelivery)
        r.set(dedup_key, "1", ex=604800)

    except Exception as e:
        send_to_dlq(b if 'b' in dir() else msg.value, str(e), CONSUMER_NAME)
