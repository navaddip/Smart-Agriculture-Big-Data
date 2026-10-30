"""
Consume the IoT sensor stream and land it as micro-batch Parquet.

Demonstrates the streaming sink: reads 'iot_sensors' from Kafka and every
BATCH messages writes a Parquet file (the tabular form the analytics use).
This is the Python stand-in for the Spark Structured Streaming job described
in the paper; the production version uses flatMapGroupsWithState for the
stateful Kalman step.

Run:
    pip install kafka-python pandas pyarrow
    python iot_consumer.py --bootstrap localhost:9092
"""
import argparse, json
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="iot_sensors")
    ap.add_argument("--outdir", default="results/iot_stream")
    ap.add_argument("--batch", type=int, default=50, help="messages per Parquet file")
    args = ap.parse_args()
    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        args.topic, bootstrap_servers=args.bootstrap,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")))
    print(f"Listening on '{args.topic}' ... (Ctrl+C to stop)")

    buf, batch_no = [], 0
    for msg in consumer:
        buf.append(msg.value)
        if len(buf) >= args.batch:
            df = pd.DataFrame(buf)
            path = Path(args.outdir) / f"iot_batch_{batch_no:04d}.parquet"
            df.to_parquet(path, index=False)
            print(f"wrote {len(buf)} readings -> {path}")
            buf, batch_no = [], batch_no + 1


if __name__ == "__main__":
    main()
