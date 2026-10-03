import os
from pathlib import Path
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

# ── Phase 1: ALTER DATABASE must run with autocommit (no transaction) ──
autocommit_statements = [
    ("Enable Change Tracking on DB",
     "ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON "
     "(CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)"),
]

# ── Phase 2: Table-level statements (can run in normal transaction) ────
table_statements = [
    ("Enable Change Tracking on claims",
     "ALTER TABLE dbo.claims ENABLE CHANGE_TRACKING"),
    ("Enable Change Tracking on customers",
     "ALTER TABLE dbo.customers ENABLE CHANGE_TRACKING"),
    ("Enable Change Tracking on policies",
     "ALTER TABLE dbo.policies ENABLE CHANGE_TRACKING"),

    ("Enable CDC on DB",
     "EXEC sys.sp_cdc_enable_db"),

    ("Enable CDC on claims",
     "EXEC sys.sp_cdc_enable_table "
     "@source_schema = N'dbo', "
     "@source_name = N'claims', "
     "@role_name = NULL, "
     "@supports_net_changes = 1"),
    ("Enable CDC on customers",
     "EXEC sys.sp_cdc_enable_table "
     "@source_schema = N'dbo', "
     "@source_name = N'customers', "
     "@role_name = NULL, "
     "@supports_net_changes = 1"),
    ("Enable CDC on policies",
     "EXEC sys.sp_cdc_enable_table "
     "@source_schema = N'dbo', "
     "@source_name = N'policies', "
     "@role_name = NULL, "
     "@supports_net_changes = 1"),
]

# Phase 1: run ALTER DATABASE with autocommit
for label, sql in autocommit_statements:
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(sql))
        print(f"[OK] {label}")
    except Exception as e:
        print(f"[WARN] {label} -- {e}")

# Phase 2: run table-level statements
with engine.connect() as conn:
    for label, sql in table_statements:
        try:
            conn.execute(text(sql))
            conn.commit()
            print(f"[OK] {label}")
        except Exception as e:
            print(f"[WARN] {label} -- {e}")

print("\nDone.")
