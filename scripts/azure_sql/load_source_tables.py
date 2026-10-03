import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

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

DATA_DIR = REPO_ROOT / "data"
TABLE_KEYS = {"claims": "claim_no", "customers": "customer_id", "policies": "POLICY_NO"}


def test_connection():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1")).fetchone()
    print(f"Connected to Azure SQL: {SQL_SERVER} / {SQL_DATABASE}")


def ensure_table(table_name, pk, df):
    """Create a missing table, or verify an existing table has the expected key."""
    inspector = inspect(engine)
    if inspector.has_table(table_name, schema="dbo"):
        columns = inspector.get_pk_constraint(table_name, schema="dbo").get("constrained_columns") or []
        if [column.lower() for column in columns] != [pk.lower()]:
            raise RuntimeError(f"dbo.{table_name} must have a primary key on {pk} before loading")
        return

    with engine.begin() as conn:
        df.head(0).to_sql(table_name, conn, if_exists="fail", index=False, schema="dbo")
        conn.execute(text(
            f"ALTER TABLE dbo.[{table_name}] ALTER COLUMN [{pk}] NVARCHAR(255) NOT NULL"
        ))
        conn.execute(text(
            f"ALTER TABLE dbo.[{table_name}] ADD CONSTRAINT PK_{table_name} PRIMARY KEY ([{pk}])"
        ))
    print(f"Created dbo.{table_name} with primary key {pk}")


def load_data():
    """Insert CSV records absent from Azure SQL without replacing tables."""
    for table_name, pk in TABLE_KEYS.items():
        file_path = DATA_DIR / f"{table_name}.csv"
        df = pd.read_csv(file_path)
        if df[pk].isna().any():
            raise ValueError(f"{file_path} contains null {pk} values")
        if df[pk].astype(str).str.len().gt(255).any():
            raise ValueError(f"{file_path} contains {pk} values longer than 255 characters")
        df = df.drop_duplicates(subset=[pk])

        ensure_table(table_name, pk, df)
        with engine.connect() as conn:
            existing_ids = pd.read_sql(
                text(f"SELECT [{pk}] FROM dbo.[{table_name}]"), conn
            )
        known = set(existing_ids[pk].dropna().astype(str))
        new_rows = df[~df[pk].astype(str).isin(known)]

        print(f"dbo.{table_name}: {len(new_rows)} new rows from {file_path.name}")
        if not new_rows.empty:
            new_rows.to_sql(table_name, engine, if_exists="append", index=False, schema="dbo")


if __name__ == "__main__":
    test_connection()
    load_data()
