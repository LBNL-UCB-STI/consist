import pytest
import pandas as pd
import numpy as np
from typing import Optional
from sqlmodel import SQLModel, Field, select, func
from consist.core.tracker import Tracker
import consist


# 1. Define the Schema (The Contract)
# We set schema="global_tables" so SQLAlchemy knows where to find it in DuckDB
class CensusRecord(SQLModel, table=True):
    __tablename__ = "census_strict"
    __table_args__ = {"schema": "global_tables"}

    # Primary Key isn't strictly necessary for OLAP, but good for ID
    person_id: int = Field(primary_key=True)
    age: int
    income: float
    mode: str

    # Optional fields (handling Schema Drift via defaults if needed)
    city: Optional[str] = None


@pytest.mark.heavy
def test_typed_analysis_workflow(tmp_path):
    run_dir = tmp_path / "typed_run"
    db_path = str(tmp_path / "typed.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # =========================================================================
    # 1. GENERATE & INGEST (With Strict Schema)
    # =========================================================================
    print("\n[Step 1] Generating data...")
    N = 100_000
    df = pd.DataFrame(
        {
            "person_id": np.arange(N),
            "age": np.random.randint(18, 90, size=N),
            "income": np.random.normal(60000, 20000, size=N),
            "mode": np.random.choice(["car", "transit", "walk"], size=N),
            "city": "San Francisco",
        }
    )

    with tracker.start_run("run_strict_01", model="asim"):
        # We explicitly pass the SQLModel class to ingest()
        # This tells Consist/dlt: "Ensure the table matches this class exactly"
        art = consist.log_artifact("census.parquet", key="census_strict")
        df.to_parquet(run_dir / "census.parquet")

        consist.ingest(artifact=art, data=df, schema=CensusRecord)  # <--- STRICT MODE

    # =========================================================================
    # 2. ANALYSIS (The "Pandas-like" SQL Way)
    # =========================================================================
    print("[Step 2] performing Typed Analysis (No SQL Strings)...")

    # We use the tracker's session to execute SQLAlchemy expressions
    # This is efficient: It generates the SQL query, sends it to DuckDB,
    # and only fetches the aggregated results (not 100k objects).

    from sqlmodel import Session

    with Session(tracker.engine) as session:
        # Query: "Group by Mode, calculate Avg Income and Count"
        # Look how Pythonic this is:
        statement = (
            select(
                CensusRecord.mode,
                func.count(CensusRecord.person_id).label("count"),
                func.avg(CensusRecord.income).label("avg_income"),
            )
            .where(CensusRecord.age > 30)  # easy filtering
            .group_by(CensusRecord.mode)
            .order_by(func.avg(CensusRecord.income).desc())
        )

        results = session.exec(statement).all()

        print(f"\n{'Mode':<10} {'Count':<10} {'Avg Income':<15}")
        print("-" * 35)
        for row in results:
            # Row matches the select() structure
            mode, count, avg_inc = row
            print(f"{mode:<10} {count:<10} ${avg_inc:,.2f}")

            assert count > 0
            assert avg_inc > 0

    # =========================================================================
    # 3. BONUS: Into DataFrame
    # =========================================================================
    # If you really want a DataFrame back for plotting:
    print("\n[Step 3] Fetching aggregated result as DataFrame...")

    with tracker.engine.connect() as conn:
        # You can pass the SQLAlchemy statement directly to read_sql
        df_agg = pd.read_sql(statement, conn)

    print(df_agg)
    assert len(df_agg) == 3
