import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from booking.schema import validate_booking_event

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

HOTELS = ["Grand Hyatt Amsterdam", "Marriott Barcelona", "Hilton London", "Ibis Paris"]
ROOM_TYPES = ["Standard", "Deluxe", "Suite"]
PAYMENT_METHODS = ["credit_card", "paypal", "debit_card"]
COUNTRIES = ["Netherlands", "Spain", "United Kingdom", "France"]


def generate_booking_event():
    check_in = datetime.now() + timedelta(days=random.randint(1, 60))
    return {
        "booking_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": str(uuid.uuid4()),
        "hotel_name": random.choice(HOTELS),
        "room_type": random.choice(ROOM_TYPES),
        "check_in_date": check_in.strftime("%Y-%m-%d"),
        "check_out_date": (check_in + timedelta(days=random.randint(1, 7))).strftime(
            "%Y-%m-%d"
        ),
        "total_price_usd": round(random.uniform(80, 800), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "country_of_user": random.choice(COUNTRIES),
    }


print("Producing booking events... (Ctrl+C to stop)")
while True:
    event = generate_booking_event()
    errors = validate_booking_event(event)
    if errors:
        print(f"  [REJECTED] Schema validation failed: {errors}")
        continue
    producer.send("booking-events", value=event)
    print(
        f"  Sent: {event['booking_id']} | {event['hotel_name']} | ${event['total_price_usd']}"
    )
    time.sleep(1.5)
