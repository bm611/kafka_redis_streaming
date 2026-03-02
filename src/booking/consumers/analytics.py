from kafka import KafkaConsumer
import json
import redis

from booking.schema import validate_booking_event
from booking.dlq import send_to_dlq

CONSUMER_NAME = "analytics-service"

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

consumer = KafkaConsumer(
    'booking-events',
    bootstrap_servers='localhost:9092',
    group_id=CONSUMER_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Analytics Service listening...")
for msg in consumer:
    try:
        b = msg.value

        errors = validate_booking_event(b)
        if errors:
            send_to_dlq(b, f"schema validation failed: {errors}", CONSUMER_NAME)
            continue

        # Store revenue in Redis — survives restarts, shared across instances
        r.hincrbyfloat("analytics:hotel_revenue", b['hotel_name'], b['total_price_usd'])
        r.hincrby("analytics:hotel_bookings", b['hotel_name'], 1)

        # Print current snapshot from Redis
        revenue = r.hgetall("analytics:hotel_revenue")
        print(f"  [ANALYTICS] Revenue snapshot: " +
              " | ".join(f"{h}: ${float(v):,.0f}" for h, v in revenue.items()))

    except Exception as e:
        send_to_dlq(b if 'b' in dir() else msg.value, str(e), CONSUMER_NAME)
