from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'booking-events',
    bootstrap_servers='localhost:9092',
    group_id='analytics-service',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

hotel_revenue = defaultdict(float)
print("Analytics Service listening...")
for msg in consumer:
    b = msg.value
    hotel_revenue[b['hotel_name']] += b['total_price_usd']
    print(f"  [ANALYTICS] Revenue snapshot: " +
          " | ".join(f"{h}: ${v:.0f}" for h, v in hotel_revenue.items()))
