# Real-Time Hotel Booking Events Pipeline

A Kafka-based event pipeline simulating a booking platform where downstream services react independently to reservation events.

## Architecture

```
Producer (booking service)
    │
    ▼
┌──────────────────┐
│  Kafka Topic:    │
│  booking-events  │
└──────────────────┘
    │         │         │
    ▼         ▼         ▼
Notifications  Fraud    Analytics
 (consumer)   (consumer) (consumer)
                 │
                 ▼
              Redis
         (feature store)
```

## Services

| Service | File | Storage | Purpose |
|---|---|---|---|
| **Producer** | `producer.py` | — | Simulates booking events using Faker |
| **Notifications** | `consumer_notifications.py` | — | Logs confirmation emails |
| **Fraud Detection** | `consumer_fraud.py` | Redis | Scores bookings in real time |
| **Analytics** | `consumer_analytics.py` | — | Tracks revenue per hotel |

## How Redis is Used (Fraud Detection)

Redis serves as a **real-time feature store** for the fraud detection consumer. Two patterns are demonstrated:

### 1. Velocity Tracking (Sliding Window)
```
Key:    fraud:user:{user_id}:count
Type:   String (counter)
TTL:    3600s (1 hour)
```
Each time a user makes a booking, the counter increments. If a user makes more than 3 bookings within an hour, it triggers a `velocity_spike` flag. The TTL means the counter resets automatically — no cleanup needed.

### 2. Risk Score Storage (Hash)
```
Key:    fraud:booking:{booking_id}
Type:   Hash
TTL:    86400s (24 hours)
Fields: risk, reasons, price, payment_method, user_booking_count
```
After scoring a booking, the result is stored so other services (e.g., a dashboard or review queue) can look it up by booking ID without re-processing.

### Why Redis for This?
- **Sub-millisecond reads/writes** — fraud scoring must not add latency to the booking flow
- **Built-in TTL** — ephemeral data (counters, scores) expires automatically
- **Atomic operations** — `INCR` is atomic, safe under concurrent writes from multiple consumer instances

## Quick Start

### 1. Start Infrastructure
```bash
cd docker
docker compose up -d
```
This starts Postgres, Kafka (with Zookeeper), and Redis.

### 2. Install Dependencies
```bash
pip install kafka-python faker redis
# or with uv:
uv sync
```

### 3. Run the Pipeline
Open 4 terminals:

```bash
# Terminal 1 — Producer
python producer.py

# Terminal 2 — Notifications
python consumer_notifications.py

# Terminal 3 — Fraud Detection (writes to Redis)
python consumer_fraud.py

# Terminal 4 — Analytics
python consumer_analytics.py

# Terminal 5 — Warehouse (batch writes to SQLite)
python consumer_warehouse.py
```

### 4. Query the Warehouse
Once the warehouse consumer has flushed a few batches:
```bash
python query_warehouse.py
```

### 5. Inspect Redis Data
```bash
# Connect to Redis CLI
docker exec -it $(docker ps -qf "ancestor=redis:7-alpine") redis-cli

# See all fraud keys
KEYS fraud:*

# Check a user's booking velocity
GET fraud:user:<user_id>:count

# Inspect a booking's risk score
HGETALL fraud:booking:<booking_id>
```

## Key Concepts

| Concept | Implementation |
|---|---|
| **Kafka Topic** | `booking-events` — single stream consumed by all services |
| **Consumer Groups** | Each service has a unique `group_id` so they read independently |
| **Redis as Feature Store** | Fraud consumer writes/reads real-time signals (velocity, risk scores) |
| **TTL-based Expiry** | Counters and scores auto-expire — no manual cleanup |
| **Batch Processing** | Warehouse consumer buffers events and flushes to SQLite every 10 messages |
| **Aggregate Tables** | Pre-computed stats (hotel revenue, payment methods, countries) refreshed on each flush |
| **Decoupling** | Adding a new consumer requires zero changes to the producer |
