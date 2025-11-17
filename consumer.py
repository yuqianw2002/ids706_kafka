import json
import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """Consumes ride events from Kafka and inserts them into PostgreSQL."""
    try:
        # --- Kafka connection ---
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "rides",  # topic for rides
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        # --- PostgreSQL connection ---
        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # --- Ensure table exists ---
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rides (
                ride_id       BIGINT,
                ts_utc        TIMESTAMP,
                pickup_lat    DOUBLE PRECISION,
                pickup_lng    DOUBLE PRECISION,
                dropoff_lat   DOUBLE PRECISION,
                dropoff_lng   DOUBLE PRECISION,
                distance_km   DOUBLE PRECISION,
                fare_usd      DOUBLE PRECISION,
                driver_id     INT,
                status        TEXT
            );
            """
        )
        conn.commit()
        print("[Consumer] ✓ Table 'rides' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        # --- Consume loop ---
        for msg in consumer:
            r = msg.value  # this must be a dict with keys matching below

            try:
                cur.execute(
                    """
                    INSERT INTO rides (
                        ride_id, ts_utc, pickup_lat, pickup_lng,
                        dropoff_lat, dropoff_lng,
                        distance_km, fare_usd, driver_id, status
                    ) VALUES (
                        %(ride_id)s, %(timestamp)s, %(pickup_lat)s, %(pickup_lng)s,
                        %(dropoff_lat)s, %(dropoff_lng)s,
                        %(distance_km)s, %(fare_usd)s, %(driver_id)s, %(status)s
                    )
                    """,
                    r,
                )
                conn.commit()
                print(f"[Consumer] ✓ Saved ride: {r.get('ride_id')}")
            except Exception as e_row:
                print(f"[Consumer] ⚠ Failed to insert ride: {r}")
                print(f"[Consumer] Row error: {e_row}")

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
