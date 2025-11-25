# src/consist/core/views.py

import os
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING
from sqlmodel import select, Session, text  # <--- Added text

from consist.models.artifact import Artifact
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class ViewFactory:
    def __init__(self, tracker: "Tracker"):
        self.tracker = tracker

    def create_hybrid_view(self, view_name: str, concept_key: str, driver_filter: List[str] = None):
        """
        Creates a DuckDB VIEW that creates a union of:
        1. 'Hot' data in global_tables.{concept_key}
        2. 'Cold' data from Artifacts (Parquet/CSV) with matching key
        """
        if not self.tracker.engine:
            raise RuntimeError("Cannot create views: No database engine configured.")

        # 1. Identify 'Hot' Data availability
        hot_table_exists = self._check_table_exists(f"global_tables.{concept_key}")

        # 2. Identify 'Cold' Artifacts
        cold_sqls = self._generate_cold_queries(concept_key, driver_filter)

        # 3. Construct the Union
        parts = []

        if hot_table_exists:
            # Select everything from the materialized table
            parts.append(f"SELECT * FROM global_tables.{concept_key}")

        parts.extend(cold_sqls)

        if not parts:
            # Fallback: Create an empty view with a generic schema
            query = "SELECT 1 as _empty WHERE 1=0"
        else:
            # UNION ALL BY NAME matches columns by name, nulling out missing ones.
            query = "\nUNION ALL BY NAME\n".join(parts)

        # 4. Execute View Creation
        # Use begin() to ensure the DDL is committed
        with self.tracker.engine.begin() as conn:
            sql = f"CREATE OR REPLACE VIEW {view_name} AS \n{query}"
            conn.execute(text(sql))

        return True

    def _check_table_exists(self, table_path: str) -> bool:
        """Checks information_schema for table existence."""
        if "." in table_path:
            schema, table = table_path.split(".", 1)
        else:
            schema = "main"
            table = table_path

        # Use text() for SQLAlchemy 2.0 compatibility
        sql = text("""
        SELECT count(*) FROM information_schema.tables 
        WHERE table_schema = :schema AND table_name = :table
        """)

        with self.tracker.engine.connect() as conn:
            result = conn.execute(sql, {"schema": schema, "table": table}).scalar()

        return result > 0

    def _generate_cold_queries(self, concept_key: str, driver_filter: List[str] = None) -> List[str]:
        """
        Finds artifacts and generates SQL SELECT statements with injected system columns.
        """
        drivers = driver_filter or ["parquet", "csv"]

        with Session(self.tracker.engine) as session:
            # Query Artifacts + Join Run to get Year/Iteration info
            statement = (
                select(Artifact, Run)
                .join(Run, Artifact.run_id == Run.id)
                .where(Artifact.key == concept_key)
                .where(Artifact.driver.in_(drivers))
            )
            results = session.exec(statement).all()

        sqls = []
        for artifact, run in results:
            # Resolve URI to absolute path for DuckDB
            abs_path = self.tracker.resolve_uri(artifact.uri)
            if not os.path.exists(abs_path):
                # Skip missing files to prevent View runtime errors
                print(f"[Consist Warning] Skipping missing artifact in View: {abs_path}")
                continue

            # Determine reader function
            if artifact.driver == "parquet":
                reader = f"read_parquet('{abs_path}')"
            elif artifact.driver == "csv":
                reader = f"read_csv_auto('{abs_path}')"
            else:
                continue

            # Inject Context Columns (The Consist Protocol)
            # We cast strict types to match the Global Table schema
            cols = [
                "*",
                f"'{run.id}'::VARCHAR as consist_run_id",
                f"'{artifact.id}'::VARCHAR as consist_artifact_id",
                f"{run.year or 'NULL'}::INTEGER as consist_year",
                f"{run.iteration or 'NULL'}::INTEGER as consist_iteration"
            ]

            sqls.append(f"SELECT {', '.join(cols)} FROM {reader}")

        return sqls