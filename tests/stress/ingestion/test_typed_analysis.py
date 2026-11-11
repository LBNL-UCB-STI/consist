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
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel
import pytest
from sqlmodel import Field, SQLModel

import consist
from consist.core.tracker import Tracker


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


class CensusModeSummary(BaseModel):
    mode: str
    count: int
    avg_income: float


@pytest.mark.heavy
def test_typed_analysis_workflow(tmp_path):
    """
    Tests an end-to-end "typed analysis" workflow using Consist, SQLModel/Pydantic, and `dlt`'s
    strict ingestion capabilities.
    """
    run_dir = tmp_path / "typed_run"
    db_path = str(tmp_path / "typed.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    try:
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
            # Log + ingest in one step via the convenience helper.
            consist.log_dataframe(
                df,
                key="census_strict",
                schema=CensusRecord,  # strict schema enforcement
                tracker=tracker,
                path=run_dir / "census.parquet",
            )

        # =========================================================================
        # 2. ANALYSIS (Typed Rows via StoreLedger)
        # =========================================================================
        logging.info("[Step 2] performing Typed Analysis...")
        tracker.create_view("v_census_strict", "census_strict")

        agg_sql = """
            SELECT
                mode,
                COUNT(person_id) AS count,
                AVG(income) AS avg_income
            FROM v_census_strict
            WHERE age > ?
            GROUP BY mode
            ORDER BY avg_income DESC
        """
        results = tracker.query_rows(agg_sql, [30], model=CensusModeSummary)

        logging.info(f"\n{'Mode':<10} {'Count':<10} {'Avg Income':<15}")
        logging.info("-" * 35)
        for row in results:
            logging.info(f"{row.mode:<10} {row.count:<10} ${row.avg_income:,.2f}")

            assert row.count > 0
            assert row.avg_income > 0

        # =========================================================================
        # 3. BONUS: Into DataFrame
        # =========================================================================
        logging.info("\n[Step 3] Fetching aggregated result as DataFrame...")

        df_agg = tracker.query_df(agg_sql, [30])

        logging.info(df_agg)
        assert len(df_agg) == 3

        # Extra check: confirm run persisted with completed status via top-level helper
        run_meta = consist.find_run(tracker=tracker, id="run_strict_01")
        assert run_meta is not None
        assert run_meta.status == "completed"
    finally:
        tracker.close()
