from kafka import KafkaProducer
from faker import Faker
import json, random, time, uuid
from datetime import datetime, timedelta

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

fake = Faker()
HOTELS = ["Grand Hyatt Amsterdam", "Marriott Barcelona", "Hilton London", "Ibis Paris"]

def generate_booking_event():
    check_in = datetime.now() + timedelta(days=random.randint(1, 60))
    return {
        "booking_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": str(uuid.uuid4()),
        "hotel_name": random.choice(HOTELS),
        "room_type": random.choice(["Standard", "Deluxe", "Suite"]),
        "check_in_date": check_in.strftime("%Y-%m-%d"),
        "check_out_date": (check_in + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d"),
        "total_price_usd": round(random.uniform(80, 800), 2),
        "payment_method": random.choice(["credit_card", "paypal", "debit_card"]),
        "country_of_user": fake.country()
    }

print("Producing booking events... (Ctrl+C to stop)")
while True:
    event = generate_booking_event()
    producer.send('booking-events', value=event)
    print(f"  Sent: {event['booking_id']} | {event['hotel_name']} | ${event['total_price_usd']}")
    time.sleep(1.5)
