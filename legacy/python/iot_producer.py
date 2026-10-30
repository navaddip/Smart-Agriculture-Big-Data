"""
IoT sensor-network simulator -> Kafka.

Emulates a field sensor network streaming soil-moisture and temperature
readings in real time. Each message is one reading from one field, published
to the 'iot_sensors' Kafka topic. This is the dense, high-frequency ground
modality that the reliability-aware fusion pairs with the sparse satellite feed.

Run:
    pip install kafka-python
    python iot_producer.py --bootstrap localhost:9092 --rate 5
"""
import argparse, json, time, random, math
from datetime import datetime, timezone


def make_sensors(processed="results", n=20):
    """Anchor virtual sensors on real grid cells so readings map to fields."""
    try:
        import pandas as pd
        grid = pd.read_parquet(f"{processed}/soilgrids.parquet")
        cells = grid.sample(min(n, len(grid)), random_state=1)
        return [dict(sensor_id=f"S{i:03d}", tile=r.tile,
                     cell_row=int(r.cell_row), cell_col=int(r.cell_col),
                     lat=float(r.lat), lon=float(r.lon))
                for i, (_, r) in enumerate(cells.iterrows())]
    except Exception:
        # fallback synthetic sensors if the grid file isn't present
        return [dict(sensor_id=f"S{i:03d}", tile="T43PGQ", cell_row=i, cell_col=i,
                     lat=12.9 + i*0.01, lon=77.5 + i*0.01) for i in range(n)]


def reading(sensor, t):
    """Synthesize a plausible soil-moisture + temperature reading."""
    hour = datetime.now(timezone.utc).hour
    diurnal = math.sin((hour - 6) / 24 * 2 * math.pi)      # warm afternoons
    temp = 26 + 6 * diurnal + random.gauss(0, 0.6)
    soil_moisture = max(0.05, 0.25 + 0.05 * math.sin(t / 50) + random.gauss(0, 0.01))
    return dict(sensor_id=sensor["sensor_id"], tile=sensor["tile"],
                cell_row=sensor["cell_row"], cell_col=sensor["cell_col"],
                lat=sensor["lat"], lon=sensor["lon"],
                timestamp=datetime.now(timezone.utc).isoformat(),
                soil_moisture=round(soil_moisture, 4),
                temperature_c=round(temp, 2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="iot_sensors")
    ap.add_argument("--rate", type=float, default=5, help="messages per second")
    ap.add_argument("--dry_run", action="store_true", help="print instead of sending")
    args = ap.parse_args()

    sensors = make_sensors()
    print(f"Simulating {len(sensors)} sensors -> topic '{args.topic}'")

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    t = 0
    while True:
        s = random.choice(sensors)
        msg = reading(s, t)
        if producer:
            producer.send(args.topic, msg)
        else:
            print(json.dumps(msg))
        t += 1
        time.sleep(1.0 / args.rate)


if __name__ == "__main__":
    main()
