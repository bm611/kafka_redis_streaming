from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'booking-events',
    bootstrap_servers='localhost:9092',
    group_id='notification-service',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Notification Service listening...")
for msg in consumer:
    b = msg.value
    print(f"  [EMAIL] Booking confirmed for user {b['user_id'][:8]}... "
          f"at {b['hotel_name']} checking in {b['check_in_date']}")
