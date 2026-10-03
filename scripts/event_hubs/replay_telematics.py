import json
import argparse
import os
from itertools import islice
import time
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from pathlib import Path

from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import ConnectError, ConnectionLostError
from dotenv import load_dotenv
import pyarrow.parquet as pq


# ============================================
# CONFIGURATION
# ============================================

EVENTHUB_NAMESPACE = "smart-claims-streaming"
EVENTHUB_NAME = "telematics"
REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKPOINT_PATH = REPO_ROOT / ".producer-checkpoint"


def json_value(value):
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def records(data_dir):
    files = sorted(data_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {data_dir}")

    for path in files:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=1024):
            yield from batch.to_pylist()


def send_record(producer, payload):
    delay = 5
    while True:
        try:
            batch = producer.create_batch()
            batch.add(EventData(payload))
            producer.send_batch(batch)
            return
        except (ConnectError, ConnectionLostError) as exc:
            print(f"Connection interrupted ({type(exc).__name__}); retrying in {delay}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60)


def main():
    parser = argparse.ArgumentParser(description="Send Parquet rows to Azure Event Hubs one at a time")
    parser.add_argument("--data-dir", type=Path, default=REPO_ROOT / "data" / "telematics")
    parser.add_argument("--interval-seconds", type=float, default=0, help="Delay between records (default: 0)")
    parser.add_argument("--limit", type=int, help="Stop after this many records")
    parser.add_argument("--skip", type=int, default=0, help="Skip this many source records before sending")
    parser.add_argument("--resume", action="store_true", help="Resume after the source record in .producer-checkpoint")
    parser.add_argument("--dry-run", action="store_true", help="Print records without sending them")
    args = parser.parse_args()

    if args.interval_seconds < 0 or args.limit is not None and args.limit < 1 or args.skip < 0:
        parser.error("--interval-seconds and --skip must be nonnegative; --limit must be positive")
    if args.resume:
        if args.skip:
            parser.error("Use either --resume or --skip, not both")
        if not CHECKPOINT_PATH.exists():
            parser.error("No .producer-checkpoint file exists")
        args.skip = int(CHECKPOINT_PATH.read_text(encoding="utf-8"))

    producer = None
    sent = 0
    try:
        if not args.dry_run:
            load_dotenv(REPO_ROOT / ".env")
            connection_string = os.environ.get("EVENTHUB_CONNECTION_STRING")
            if not connection_string:
                parser.error("Set EVENTHUB_CONNECTION_STRING in .env or the environment")
            producer = EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=EVENTHUB_NAME,
            )

        for record in islice(records(args.data_dir), args.skip, None):
            payload = json.dumps(record, default=json_value, ensure_ascii=False)
            if args.dry_run:
                print(payload)
            else:
                send_record(producer, payload)

            sent += 1
            if not args.dry_run:
                CHECKPOINT_PATH.write_text(str(args.skip + sent), encoding="utf-8")
            if not args.dry_run and (sent == 1 or sent % 1000 == 0):
                print(f"Sent through source record {args.skip + sent}", flush=True)
            if args.limit is not None and sent >= args.limit:
                break
            if args.interval_seconds:
                time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        if producer is not None:
            producer.close()

    print(f"Processed {sent} records")


if __name__ == "__main__":
    main()
