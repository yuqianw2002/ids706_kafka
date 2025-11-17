import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_ride():
    ride_id = random.randint(100000, 999999)
    driver_id = random.randint(1, 50)
    distance_km = round(random.uniform(0.5, 25.0), 2)
    fare_usd = round(2.5 + distance_km * random.uniform(0.8, 2.0), 2)
    status = random.choice(["ongoing", "completed"])

    return {
        "ride_id": ride_id,
        "timestamp": datetime.utcnow().isoformat(),
        "pickup_lat": round(random.uniform(40.5, 40.9), 5),
        "pickup_lng": round(random.uniform(-74.2, -73.7), 5),
        "dropoff_lat": round(random.uniform(40.5, 40.9), 5),
        "dropoff_lng": round(random.uniform(-74.2, -73.7), 5),
        "distance_km": distance_km,
        "fare_usd": fare_usd,
        "driver_id": driver_id,
        "status": status,
    }

while True:
    ride = generate_ride()
    producer.send("rides", ride)
    print("[Producer] Sent ride:", ride)
    time.sleep(1)  # 1 event per second
