import os
from pathlib import Path
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus

REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

SQL_SERVER = os.getenv("SQL_SERVER", "insurance-project.database.windows.net")
SQL_DATABASE = os.getenv("SQL_DATABASE", "insurance_db")
SQL_USER = os.getenv("SQL_USER", "sqladmin")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
if not SQL_PASSWORD:
    raise RuntimeError("Set SQL_PASSWORD in the environment or .env")

engine = create_engine(
    f"mssql+pymssql://{SQL_USER}:{quote_plus(SQL_PASSWORD)}@{SQL_SERVER}/{SQL_DATABASE}"
)

SCRIPT_URL = "https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql"
LOCAL_SCRIPT = REPO_ROOT / "utility_script.sql"


def download_script():
    """Download the Lakeflow utility objects SQL script."""
    if not os.path.exists(LOCAL_SCRIPT):
        print(f"Downloading utility script from {SCRIPT_URL}...")
        resp = requests.get(SCRIPT_URL)
        resp.raise_for_status()
        with open(LOCAL_SCRIPT, "w", encoding="utf-8") as f:
            f.write(resp.text)
        print(f"Saved to {LOCAL_SCRIPT}")
    else:
        print(f"Using existing {LOCAL_SCRIPT}")


def execute_script():
    """Execute the utility script against Azure SQL using GO-batch splitting."""
    with open(LOCAL_SCRIPT, "r", encoding="utf-8") as f:
        script = f.read()

    # SQL scripts use GO as a batch separator — split on it
    # Match GO on its own line (case-insensitive)
    import re
    batches = re.split(r'^\s*GO\s*$', script, flags=re.MULTILINE | re.IGNORECASE)

    print(f"\nExecuting utility script ({len(batches)} batch(es))...")
    print(f"Target: {SQL_SERVER} / {SQL_DATABASE}\n")

    with engine.connect() as conn:
        for i, batch in enumerate(batches, 1):
            batch = batch.strip()
            if not batch:
                continue
            try:
                conn.execute(text(batch))
                conn.commit()
                print(f"[OK] Batch {i} executed successfully")
            except Exception as e:
                print(f"[WARN] Batch {i} -- {e}")

    print("\n--- Utility objects script execution complete ---")


def setup_ct_and_cdc():
    """Run lakeflowSetupChangeTracking and lakeflowSetupChangeDataCapture
    for the ingestion user (sqladmin in this case)."""

    with engine.connect() as conn:
        # Setup Change Tracking with DDL support objects
        try:
            conn.execute(text(
                "EXEC dbo.lakeflowSetupChangeTracking "
                "@Tables = 'dbo.claims, dbo.customers, dbo.policies', "
                "@User = 'sqladmin', "
                "@CreateDdlSupportingObjects = 1"
            ))
            conn.commit()
            print("[OK] lakeflowSetupChangeTracking completed")
        except Exception as e:
            print(f"[WARN] lakeflowSetupChangeTracking -- {e}")

        # Setup CDC
        try:
            conn.execute(text(
                "EXEC dbo.lakeflowSetupChangeDataCapture "
                "@Tables = 'dbo.claims, dbo.customers, dbo.policies', "
                "@User = 'sqladmin'"
            ))
            conn.commit()
            print("[OK] lakeflowSetupChangeDataCapture completed")
        except Exception as e:
            print(f"[WARN] lakeflowSetupChangeDataCapture -- {e}")


def verify_installation():
    """Verify the utility objects were installed correctly."""
    with engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT dbo.lakeflowUtilityVersion() AS Version"))
            row = result.fetchone()
            print(f"\n[OK] Lakeflow Utility Version: {row[0]}")
        except Exception as e:
            print(f"[ERROR] Could not verify version -- {e}")

        try:
            result = conn.execute(text("SELECT dbo.lakeflowDetectPlatform() AS Platform"))
            row = result.fetchone()
            print(f"[OK] Detected Platform: {row[0]}")
        except Exception as e:
            print(f"[ERROR] Could not detect platform -- {e}")


if __name__ == "__main__":
    download_script()
    execute_script()
    setup_ct_and_cdc()
    verify_installation()
