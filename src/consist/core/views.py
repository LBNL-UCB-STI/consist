"""
Consist Core Views Module

This module defines the `ViewFactory`, which is a central component of Consist's
data virtualization layer. It enables the creation of "Hybrid Views" in DuckDB,
allowing users to query data seamlessly regardless of whether it's stored
in materialized database tables ("hot" data) or directly in file-based
artifacts ("cold" data like Parquet or CSV).

The module implements strategies for:
-   **Hybrid Data Access**: Unifying "hot" and "cold" data sources under a single SQL interface.
-   **View Optimization**: Leveraging DuckDB's native capabilities for efficient
    vectorized reads from files to ensure query performance.
-   **Schema Evolution**: Handling schema differences across various data sources
    and runs through `UNION ALL BY NAME` clauses.
"""

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

    Attributes
    ----------
    tracker : Tracker
        An instance of the Consist `Tracker`, which provides access to the database
        engine, artifact resolution, and other run-time context necessary for
        view creation.
    """

    def __init__(self, tracker: "Tracker") -> None:
        """
        Initializes the ViewFactory with a reference to the main Tracker.

        Parameters
        ----------
        tracker : Tracker
            An instance of the Consist `Tracker`, which provides access to the
            database engine, artifact resolution, and other run-time context
            required for creating and managing views.
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

        Parameters
        ----------
        view_name : str
            The name to assign to the newly created or replaced SQL view. This is the name
            you will use in your SQL queries to access the combined data.
        concept_key : str
            The semantic key identifying the data concept (e.g., "households", "transactions").
            Artifacts and materialized tables matching this key will be included in the view.
        driver_filter : Optional[List[str]], optional
            An optional list of artifact drivers (e.g., "parquet", "csv") to include
            when querying "cold" data. If `None`, "parquet" and "csv" drivers are considered
            by default.

        Returns
        -------
        bool
            True if the view creation was attempted (even if the view ends up empty), False otherwise.

        Raises
        ------
        RuntimeError
            If the `Tracker`'s database engine is not configured (i.e., `db_path` was not
            provided during `Tracker` initialization).
        """
        if not self.tracker.engine:
            raise RuntimeError("Cannot create views: No database engine configured.")

        # 1. Identify 'Hot' Data
        # Explicitly check for the table in global_tables schema
        hot_table_exists = self._check_table_exists("global_tables", concept_key)

        # 2. Identify 'Cold' Artifacts
        cold_sql = self._generate_cold_query_optimized(concept_key, driver_filter)

        parts = []

        if hot_table_exists:
            parts.append(f"SELECT * FROM global_tables.{concept_key}")

        if cold_sql:
            parts.append(cold_sql)

        if not parts:
            # FIX: Create an empty dummy view so queries don't crash
            # DuckDB allows typed empty selects, but simplest is a generic empty set
            query = "SELECT 1 AS _empty_marker WHERE 1=0"
        else:
            query = "\nUNION ALL BY NAME\n".join(parts)

        with self.tracker.engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            sql = f"CREATE VIEW {view_name} AS \n{query}"
            conn.execute(text(sql))

        return True

    def _check_table_exists(self, schema: str, table: str) -> bool:
        """
        Robustly checks if a table exists in a specific schema.
        """
        sql = text(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name = :table"
        )
        try:
            with self.tracker.engine.connect() as conn:
                count = conn.execute(sql, {"schema": schema, "table": table}).scalar()
                return count > 0
        except Exception as e:
            logging.warning(f"Failed to check table existence: {e}")
            return False

    def _generate_cold_query_optimized(
        self, concept_key: str, driver_filter: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Generates a single optimized SQL query for all "cold" artifacts of a given `concept_key`.

        This method is central to **"View Optimization"** by employing **"Vectorization"**:
        it uses DuckDB's `read_parquet` or `read_csv_auto` functions with a list of file paths
        for efficient, single-pass reads. It also dynamically injects run-specific metadata
        (e.g., `consist_run_id`, `consist_year`) into the loaded data using a Common Table Expression (CTE)
        to allow unified querying with hot data. This approach significantly reduces query
        complexity and improves performance when dealing with numerous file-based artifacts.

        Parameters
        ----------
        concept_key : str
            The semantic key for which to generate the cold data query.
        driver_filter : Optional[List[str]], optional
            An optional list of artifact drivers (e.g., "parquet", "csv") to include
            when querying "cold" data. If `None`, "parquet" and "csv" drivers are considered
            by default.

        Returns
        -------
        Optional[str]
            A SQL query string that, when executed, will return the combined data
            from all matching "cold" artifacts with injected provenance metadata.
            Returns `None` if no cold data artifacts are found or eligible.
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
                # Quote the path for the list
                path_list.append(f"'{safe_path}'")

                # Meta row
                row = (
                    f"'{safe_path}'",
                    f"'{item['run_id']}'",
                    f"'{item['art_id']}'",
                    f"{item['year'] or 'NULL'}",
                    f"{item['iter'] or 'NULL'}",
                )
                meta_rows.append(f"({', '.join(row)})")

            if driver == "parquet":
                # union_by_name=True handles schema drift
                reader_func = f"read_parquet([{', '.join(path_list)}], union_by_name=true, filename=true)"
            elif driver == "csv":
                reader_func = f"read_csv_auto([{', '.join(path_list)}], union_by_name=true, filename=true, normalize_names=true)"
            else:
                continue

            cte_name = f"meta_{driver}_{concept_key}"
            cte_values = ",\n        ".join(meta_rows)

            # NOTE: We simply match on filename.
            # DuckDB's read_parquet usually returns the path provided in the list.
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
