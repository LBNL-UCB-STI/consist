# src/consist/core/views.py

import logging
import os
from typing import List, Optional, TYPE_CHECKING
from sqlmodel import select, Session, text

from consist.models.artifact import Artifact
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class ViewFactory:
    """
    A factory class responsible for generating **"Hybrid Views"** in DuckDB, acting as
    Consist's **"The Virtualizer"** component.

    Hybrid Views combine data from materialized tables (often ingested via dlt)
    with data directly from file-based artifacts (e.g., Parquet, CSV),
    providing a unified SQL interface to query both "hot" and "cold" data
    transparently. This approach is central to Consist's flexible data access strategy.
    """

    def __init__(self, tracker: "Tracker"):
        """
        Initializes the ViewFactory with a reference to the main Tracker.

        Args:
            tracker (Tracker): An instance of the Consist Tracker, which provides
                               access to the database engine, artifact resolution,
                               and other run-time context.
        """
        self.tracker = tracker

    def create_hybrid_view(
        self,
        view_name: str,
        concept_key: str,
        driver_filter: Optional[List[str]] = None,
    ) -> bool:
        """
        Creates or replaces a DuckDB SQL VIEW that combines "hot" and "cold" data for a given concept.

        This method generates a **"Hybrid View"** which allows transparent querying across
        different data storage types. It implements **"View Optimization"** by leveraging
        DuckDB's capabilities for vectorized reads from files. The resulting view uses
        `UNION ALL BY NAME` to gracefully handle **"Schema Evolution"** (different columns
        across runs or data sources) by nulling out missing columns.

        "Hot" data refers to records already materialized into a DuckDB table (e.g., via ingestion).
        "Cold" data refers to records still residing in file-based artifacts (e.g., Parquet, CSV).

        Args:
            view_name (str): The name to assign to the newly created or replaced SQL view.
            concept_key (str): The semantic key identifying the data concept (e.g., "households").
                               Artifacts and materialized tables matching this key will be included.
            driver_filter (Optional[List[str]]): An optional list of artifact drivers (e.g., "parquet", "csv")
                                                 to include when querying "cold" data. If None, "parquet"
                                                 and "csv" drivers are considered by default.

        Returns:
            bool: True if the view creation was attempted (even if empty), False otherwise.

        Raises:
            RuntimeError: If the Tracker's database engine is not configured.
        """
        if not self.tracker.engine:
            raise RuntimeError("Cannot create views: No database engine configured.")

        # 1. Identify 'Hot' Data
        hot_table_exists = self._check_table_exists(f"global_tables.{concept_key}")

        # 2. Identify 'Cold' Artifacts (Optimized)
        cold_sql = self._generate_cold_query_optimized(concept_key, driver_filter)

        # 3. Construct the Union
        parts = []

        if hot_table_exists:
            # Select everything from the materialized table
            parts.append(f"SELECT * FROM global_tables.{concept_key}")

        if cold_sql:
            parts.append(cold_sql)

        if not parts:
            # Fallback: Create an empty view with a generic schema
            query = "SELECT 1 as _empty WHERE 1=0"
        else:
            # UNION ALL BY NAME matches columns by name, nulling out missing ones.
            query = "\nUNION ALL BY NAME\n".join(parts)

        # 4. Execute
        with self.tracker.engine.begin() as conn:
            sql = f"CREATE OR REPLACE VIEW {view_name} AS \n{query}"
            conn.execute(text(sql))

        return True

    def _check_table_exists(self, table_path: str) -> bool:
        """
        Checks if a specified table exists in the DuckDB information schema.

        Args:
            table_path (str): The full path to the table (e.g., "global_tables.my_table" or "my_table").

        Returns:
            bool: True if the table exists, False otherwise.
        """
        if "." in table_path:
            schema, table = table_path.split(".", 1)
        else:
            schema = "main"
            table = table_path

        # Use text() for SQLAlchemy 2.0 compatibility
        sql = text(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table"
        )
        with self.tracker.engine.connect() as conn:
            return conn.execute(sql, {"schema": schema, "table": table}).scalar() > 0

    def _generate_cold_query_optimized(
        self, concept_key: str, driver_filter: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Generates a single optimized SQL query for all "cold" artifacts of a given `concept_key`.

        This method is central to **"View Optimization"** by employing **"Vectorization"**:
        it uses DuckDB's `read_parquet` or `read_csv_auto` functions with a list of file paths
        for efficient, single-pass reads. It also dynamically injects run-specific metadata
        (e.g., `consist_run_id`, `consist_year`) using a Common Table Expression (CTE)
        to allow unified querying with hot data.

        Args:
            concept_key (str): The semantic key for which to generate the cold data query.
            driver_filter (Optional[List[str]]): List of artifact drivers to include.

        Returns:
            Optional[str]: A SQL query string for the cold data, or None if no cold data is found.
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

        if not results:
            return None

        # Group artifacts by driver to vectorize reads
        # (e.g. read_parquet can't read CSVs, so we group them)
        grouped = {}
        for artifact, run in results:
            if artifact.meta and artifact.meta.get("is_ingested"):
                continue

            abs_path = self.tracker.resolve_uri(artifact.uri)
            if not os.path.exists(abs_path):
                # Skip missing files to prevent View runtime errors
                logging.warning(
                    f"[Consist Warning] Skipping missing artifact in View: {abs_path}"
                )
                continue

            if artifact.driver not in grouped:
                grouped[artifact.driver] = []

            grouped[artifact.driver].append(
                {
                    "path": abs_path,
                    "run_id": run.id,
                    "art_id": str(artifact.id),
                    "year": run.year,
                    "iter": run.iteration,
                }
            )

        union_parts = []

        for driver, items in grouped.items():
            if not items:
                continue

            # --- Optimization: Vectorized Read + Join ---
            # We construct a CTE (Common Table Expression) that maps filenames to metadata

            # 1. Build Metadata Map (VALUES list)
            # DuckDB allows matching on 'filename' returned by reader
            meta_rows = []
            path_list = []

            for item in items:
                # Escape single quotes in paths for SQL safety
                safe_path = item["path"].replace("'", "''")
                path_list.append(f"'{safe_path}'")

                # Meta row: (path, run_id, art_id, year, iter)
                row = (
                    f"'{safe_path}'",
                    f"'{item['run_id']}'",
                    f"'{item['art_id']}'",
                    f"{item['year'] or 'NULL'}",
                    f"{item['iter'] or 'NULL'}",
                )
                meta_rows.append(f"({', '.join(row)})")

            # 2. Define Reader
            if driver == "parquet":
                # union_by_name=True handles schema drift across files!
                reader_func = f"read_parquet([{', '.join(path_list)}], union_by_name=true, filename=true)"
            elif driver == "csv":
                # normalize_names=true helps with CSV header inconsistencies (case/spacing)
                reader_func = f"read_csv_auto([{', '.join(path_list)}], union_by_name=true, filename=true, normalize_names=true)"
            else:
                continue  # Unknown driver fallback

            # 3. Construct Query with CTE
            # The CTE 'file_meta' acts as our lookup table
            cte_values = ",\n        ".join(meta_rows)

            # Using a unique alias for the CTE to prevent collisions if multiple drivers exist
            cte_name = f"meta_{driver}"

            query = f"""
            SELECT 
                data.* EXCLUDE (filename), 
                {cte_name}.run_id as consist_run_id,
                {cte_name}.art_id as consist_artifact_id,
                CAST({cte_name}.year AS INTEGER) as consist_year,
                CAST({cte_name}.iter AS INTEGER) as consist_iteration
            FROM {reader_func} data
            JOIN (
                VALUES {cte_values}
            ) as {cte_name}(fpath, run_id, art_id, year, iter)
            ON data.filename = {cte_name}.fpath
            """
            union_parts.append(query)

        if not union_parts:
            return None

        return "\nUNION ALL BY NAME\n".join(union_parts)
