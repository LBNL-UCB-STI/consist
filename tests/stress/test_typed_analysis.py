"""
Consist "Typed Analysis" Workflow Stress Tests

This module contains stress tests for Consist's "typed analysis" workflow.
This workflow leverages `SQLModel` for schema definition, and `dlt` for
strict ingestion. It demonstrates how Consist can enforce data contracts
and enable Pythonic, type-safe querying of large datasets via SQLAlchemy
expressions, effectively bridging the gap between Python data science
and SQL analytics.

These tests validate the end-to-end process of defining a schema,
ingesting data according to that schema, and then querying the data
using Python objects rather than raw SQL strings, all while handling
large volumes of data efficiently.
"""

import logging
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
    """
    SQLModel class representing a strict schema for census records.

    This model serves as a data contract, defining the expected types and structure
    of census data when ingested into Consist. By specifying `__table_args__` and
    `__tablename__`, it maps directly to the global Consist data model for "census_strict"
    records within the DuckDB database.

    Attributes
    ----------
    person_id : int
        Unique identifier for each person, serving as the primary key.
    age : int
        Age of the person.
    income : float
        Income of the person.
    mode : str
        Primary mode of transport (e.g., "car", "transit", "walk").
    city : Optional[str]
        Optional field for the city of residence.
    """

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
    """
    Tests an end-to-end "typed analysis" workflow using Consist, SQLModel, and `dlt`'s
    strict ingestion capabilities.

    This test demonstrates how Consist allows users to define explicit data schemas
    using `SQLModel`, strictly ingest data according to those schemas, and then
    perform Pythonic, type-safe analytical queries directly against the data in DuckDB
    using SQLAlchemy expressions, without writing raw SQL strings. This approach
    combines the benefits of type safety with the performance of a columnar database.

    What happens:
    1. A `Tracker` is initialized.
    2. **Generate & Ingest**: A large Pandas DataFrame (`df`) is generated with
       synthetic census data (100,000 rows). This DataFrame is then ingested into
       Consist using `consist.ingest`, with `CensusRecord` provided as a strict
       schema. This ensures the incoming data conforms to the defined types and structure.
    3. **Pythonic Analysis**: A Pythonic analytical query is constructed using
       SQLAlchemy expressions (e.g., `select(CensusRecord.mode, func.count(...))`)
       to group data by mode, calculate the count of persons, and their average income,
       filtered by age. This query is executed via the `tracker.engine`.
    4. **Results as DataFrame**: The results of the SQLAlchemy query are also fetched
       directly as a Pandas DataFrame for further Python-based analysis.

    What's checked:
    - The ingestion process successfully loads data into the `global_tables.census_strict`
      table, adhering to the `CensusRecord` schema.
    - The Pythonic SQLAlchemy query correctly translates into SQL and retrieves accurate
      aggregated results from the DuckDB database.
    - The printed results confirm that `count` and `avg_income` values are positive
      for each mode.
    - The conversion of aggregated results back into a Pandas DataFrame (`df_agg`) is
      successful, and its length matches the number of unique modes.
    """
    run_dir = tmp_path / "typed_run"
    db_path = str(tmp_path / "typed.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    tracker.identity.hashing_strategy = "fast"

    # =========================================================================
    # 1. GENERATE & INGEST (With Strict Schema)
    # =========================================================================
    logging.info("\n[Step 1] Generating data...")
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
    logging.info("[Step 2] performing Typed Analysis (No SQL Strings)...")

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

        logging.info(f"\n{'Mode':<10} {'Count':<10} {'Avg Income':<15}")
        logging.info("-" * 35)
        for row in results:
            # Row matches the select() structure
            mode, count, avg_inc = row
            logging.info(f"{mode:<10} {count:<10} ${avg_inc:,.2f}")

            assert count > 0
            assert avg_inc > 0

    # =========================================================================
    # 3. BONUS: Into DataFrame
    # =========================================================================
    # If you really want a DataFrame back for plotting:
    logging.info("\n[Step 3] Fetching aggregated result as DataFrame...")

    with tracker.engine.connect() as conn:
        # You can pass the SQLAlchemy statement directly to read_sql
        df_agg = pd.read_sql(statement, conn)

    logging.info(df_agg)
    assert len(df_agg) == 3
