import json

import redis
from kafka import KafkaConsumer

from booking.schema import validate_booking_event
from booking.dlq import send_to_dlq

CONSUMER_NAME = "fraud-detection-service"

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

consumer = KafkaConsumer(
    "booking-events",
    bootstrap_servers="localhost:9092",
    group_id=CONSUMER_NAME,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)


def score_booking(b):
    user_id = b["user_id"]
    booking_id = b["booking_id"]

    # Idempotency — skip if already scored
    if r.exists(f"fraud:booking:{booking_id}"):
        return None, None, None

    # Track how many bookings this user has made (sliding window: 1 hour)
    user_key = f"fraud:user:{user_id}:count"
    r.incr(user_key)
    r.expire(user_key, 3600)
    booking_count = int(r.get(user_key))

    # Determine risk
    reasons = []
    if b["total_price_usd"] > 600:
        reasons.append("high_value")
    if booking_count > 3:
        reasons.append("velocity_spike")

    risk = "HIGH" if reasons else "LOW"

    # Store risk score in Redis (expires after 24h)
    r.hset(
        f"fraud:booking:{booking_id}",
        mapping={
            "risk": risk,
            "reasons": json.dumps(reasons),
            "price": b["total_price_usd"],
            "payment_method": b["payment_method"],
            "user_booking_count": booking_count,
        },
    )
    r.expire(f"fraud:booking:{booking_id}", 86400)

    return risk, reasons, booking_count


print("Fraud Detection Service listening...")
for msg in consumer:
    try:
        b = msg.value

        errors = validate_booking_event(b)
        if errors:
            send_to_dlq(b, f"schema validation failed: {errors}", CONSUMER_NAME)
            continue

        risk, reasons, count = score_booking(b)

        if risk is None:
            print(f"  [SKIP] Duplicate — already scored {b['booking_id'][:8]}...")
            continue

        icon = "🚨" if risk == "HIGH" else "✅"
        print(
            f"  [{icon} {risk}] Booking {b['booking_id'][:8]}... "
            f"| ${b['total_price_usd']} via {b['payment_method']} "
            f"| user bookings={count} reasons={reasons}"
        )

    except Exception as e:
        send_to_dlq(b if 'b' in dir() else msg.value, str(e), CONSUMER_NAME)
