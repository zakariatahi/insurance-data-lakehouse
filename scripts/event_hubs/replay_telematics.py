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


def retry_connection(operation):
    delay = 5
    while True:
        try:
            return operation()
        except (ConnectError, ConnectionLostError) as exc:
            print(f"Connection interrupted ({type(exc).__name__}); retrying in {delay}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60)


def replay_records(producer, source, *, skip, limit, dry_run, batch_size, interval_seconds, checkpoint_path):
    processed = 0
    acknowledged = 0
    batch = None
    buffered = 0

    def flush_batch():
        nonlocal batch, buffered, acknowledged
        if not buffered:
            return
        retry_connection(lambda: producer.send_batch(batch))
        previous = acknowledged
        acknowledged += buffered
        checkpoint_path.write_text(str(skip + acknowledged), encoding="utf-8")
        if previous == 0 or acknowledged // 1000 > previous // 1000:
            print(f"Sent through source record {skip + acknowledged}", flush=True)
        batch = None
        buffered = 0

    try:
        for record in source:
            payload = json.dumps(record, default=json_value, ensure_ascii=False)
            if dry_run:
                print(payload)
            else:
                if batch is None:
                    batch = retry_connection(producer.create_batch)
                event = EventData(payload)
                try:
                    batch.add(event)
                except ValueError:
                    if not buffered:
                        raise ValueError("A single event exceeds the Event Hubs batch size limit") from None
                    flush_batch()
                    batch = retry_connection(producer.create_batch)
                    try:
                        batch.add(event)
                    except ValueError:
                        raise ValueError("A single event exceeds the Event Hubs batch size limit") from None
                buffered += 1
                if buffered >= batch_size:
                    flush_batch()

            processed += 1
            if limit is not None and processed >= limit:
                break
            if interval_seconds:
                time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Stopped by user")
    else:
        flush_batch()

    return processed if dry_run else acknowledged


def main():
    parser = argparse.ArgumentParser(description="Replay Parquet rows to Azure Event Hubs")
    parser.add_argument("--data-dir", type=Path, default=REPO_ROOT / "data" / "telematics")
    parser.add_argument("--interval-seconds", type=float, default=0, help="Delay between records (default: 0)")
    parser.add_argument("--batch-size", type=int, default=100, help="Maximum events per send (default: 100)")
    parser.add_argument("--limit", type=int, help="Stop after this many records")
    parser.add_argument("--skip", type=int, default=0, help="Skip this many source records before sending")
    parser.add_argument("--resume", action="store_true", help="Resume after the source record in .producer-checkpoint")
    parser.add_argument("--dry-run", action="store_true", help="Print records without sending them")
    args = parser.parse_args()

    if args.interval_seconds < 0 or args.limit is not None and args.limit < 1 or args.skip < 0 or args.batch_size < 1:
        parser.error("--interval-seconds and --skip must be nonnegative; --limit and --batch-size must be positive")
    if args.resume:
        if args.skip:
            parser.error("Use either --resume or --skip, not both")
        if not CHECKPOINT_PATH.exists():
            parser.error("No .producer-checkpoint file exists")
        args.skip = int(CHECKPOINT_PATH.read_text(encoding="utf-8"))

    producer = None
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

        sent = replay_records(
            producer,
            islice(records(args.data_dir), args.skip, None),
            skip=args.skip,
            limit=args.limit,
            dry_run=args.dry_run,
            batch_size=1 if args.interval_seconds else args.batch_size,
            interval_seconds=args.interval_seconds,
            checkpoint_path=CHECKPOINT_PATH,
        )
    finally:
        if producer is not None:
            producer.close()

    print(f"Processed {sent} records")


if __name__ == "__main__":
    main()
