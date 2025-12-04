import logging
import os
import random
import uuid
import functools
import inspect
from pathlib import Path
from time import sleep
from typing import Dict, Optional, List, Any, Type, Iterable, Union, Callable, Tuple
from datetime import datetime, timezone
UTC = timezone.utc
from contextlib import contextmanager

import pandas as pd

from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm.exc import ConcurrentModificationError
from sqlmodel import create_engine, Session, select, SQLModel

from sqlalchemy.pool import NullPool
from pydantic import BaseModel

from consist.core.views import ViewFactory
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink, ConsistRecord
from consist.core.identity import IdentityManager
from consist.core.context import push_tracker, pop_tracker


class OutputCapture:
    """
    A helper object to temporarily hold artifacts captured during a `capture_outputs` block.

    This class provides a convenient way for users to access the artifacts that were
    automatically logged by the system immediately after a `capture_outputs` context manager
    finishes execution.

    Attributes:
        artifacts (List[Artifact]): A list of `Artifact` objects that were
                                    captured and logged during the `capture_outputs` block.
    """

    def __init__(self) -> None:
        """
        Initializes the OutputCapture object.

        This constructor sets up an empty list to store `Artifact` objects that
        will be captured during a `capture_outputs` context.
        """
        self.artifacts: List[Artifact] = []


class Tracker:
    """
    The central orchestrator for Consist, managing the lifecycle of a Run and its associated Artifacts.

    The Tracker is responsible for:
    1.  Initiating and managing the state of individual "Runs" (e.g., model executions, data processing steps).
    2.  Logging "Artifacts" (input files, output data, etc.) and their relationships to runs.
    3.  Implementing a **dual-write mechanism**, logging provenance to both human-readable JSON files (`consist.json`)
        and an analytical DuckDB database (`provenance.duckdb`).
    4.  Providing **path virtualization** to make runs portable across different environments,
        as described in the "Path Resolution & Mounts" architectural section.
    5.  Facilitating **smart caching** based on a Merkle DAG strategy, enabling "run forking" and "hydration"
        of previously computed results.
    """

    def __init__(
        self,
        run_dir: Path,
        db_path: Optional[str] = None,
        mounts: Dict[str, str] = None,
        project_root: str = ".",
        hashing_strategy: str = "full",
    ):
        """
        Initializes the Consist Tracker.

        Sets up the directory for run logs, configures path virtualization mounts,
        and optionally initializes the DuckDB database connection.

        Args:
            run_dir (Path): The root directory where run-specific logs (e.g., `consist.json`)
                            and potentially other run outputs will be stored. This directory
                            will be created if it does not exist.
            db_path (Optional[str]): The file path to the DuckDB database. If provided,
                                     the tracker will persist run and artifact metadata
                                     to this database as part of the **dual-write mechanism**.
                                     If None, database features are disabled.
            mounts (Optional[Dict[str, str]]): A dictionary mapping scheme names (e.g., "inputs", "outputs")
                                             to absolute file system paths. These mounts are used for
                                             **path virtualization**, allowing Consist to store portable URIs
                                             instead of absolute paths, making runs reproducible across environments.
                                             Defaults to an empty dictionary if None.
            project_root (str): The root directory of the project, used by the `IdentityManager`
                                to compute relative paths for code hashing and artifact virtualization.
                                Defaults to the current working directory ".".
        """
        # Force absolute resolve on run_dir to prevent /var vs /private/var mismatches
        self.run_dir = Path(run_dir).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = mounts or {}
        self.db_path = db_path
        self.identity = IdentityManager(
            project_root=project_root, hashing_strategy=hashing_strategy
        )

        self.engine = None
        if db_path:
            # Using NullPool ensures the file lock is released when the session closes
            self.engine = create_engine(f"duckdb:///{db_path}", poolclass=NullPool)

            # WRAP: Schema creation in retry logic
            self._execute_with_retry(
                lambda: SQLModel.metadata.create_all(
                    self.engine,
                    tables=[
                        Run.__table__,
                        Artifact.__table__,
                        RunArtifactLink.__table__,
                    ],
                ),
                operation_name="init_schema",
            )

        # In-Memory State (The Source of Truth)
        self.current_consist: Optional[ConsistRecord] = None

        # Introspection State (Last completed run)
        self._last_consist: Optional[ConsistRecord] = None

        # Active run tracking (for imperative begin_run/end_run pattern)
        self._active_run_cache_mode: Optional[str] = None

        # Event Hooks (for extensibility)
        self._on_run_start_hooks: List[Callable[[Run], None]] = []
        self._on_run_complete_hooks: List[Callable[[Run, List[Artifact]], None]] = []
        self._on_run_failed_hooks: List[Callable[[Run, Exception], None]] = []

    # --- Event Hook Registration ---

    def on_run_start(self, callback: Callable[[Run], None]) -> Callable[[Run], None]:
        """
        Register a callback to be invoked when a run starts.

        The callback receives the `Run` object after it has been initialized
        but before any user code executes. This is useful for external integrations
        like OpenLineage event emission, logging, or notifications.

        Parameters
        ----------
        callback : Callable[[Run], None]
            A function that takes a `Run` object as its only argument.

        Returns
        -------
        Callable[[Run], None]
            The same callback, allowing use as a decorator.

        Example
        -------
        ```python
        @tracker.on_run_start
        def log_start(run):
            print(f"Starting run: {run.id}")

        # Or without decorator:
        tracker.on_run_start(my_callback_function)
        ```
        """
        self._on_run_start_hooks.append(callback)
        return callback

    def on_run_complete(
        self, callback: Callable[[Run, List[Artifact]], None]
    ) -> Callable[[Run, List[Artifact]], None]:
        """
        Register a callback to be invoked when a run completes successfully.

        The callback receives the `Run` object and a list of output `Artifact` objects.
        This is useful for external integrations like OpenLineage event emission,
        post-processing, notifications, or cleanup tasks.

        Parameters
        ----------
        callback : Callable[[Run, List[Artifact]], None]
            A function that takes a `Run` object and a list of `Artifact` objects.

        Returns
        -------
        Callable[[Run, List[Artifact]], None]
            The same callback, allowing use as a decorator.

        Example
        -------
        ```python
        @tracker.on_run_complete
        def log_complete(run, outputs):
            print(f"Run {run.id} completed with {len(outputs)} outputs")
            print(f"Duration: {run.duration_seconds:.2f}s")
        ```
        """
        self._on_run_complete_hooks.append(callback)
        return callback

    def on_run_failed(
        self, callback: Callable[[Run, Exception], None]
    ) -> Callable[[Run, Exception], None]:
        """
        Register a callback to be invoked when a run fails with an exception.

        The callback receives the `Run` object and the `Exception` that caused the failure.
        This is useful for error reporting, alerting, or cleanup tasks.

        Parameters
        ----------
        callback : Callable[[Run, Exception], None]
            A function that takes a `Run` object and an `Exception`.

        Returns
        -------
        Callable[[Run, Exception], None]
            The same callback, allowing use as a decorator.

        Example
        -------
        ```python
        @tracker.on_run_failed
        def alert_failure(run, error):
            send_alert(f"Run {run.id} failed: {error}")
        ```
        """
        self._on_run_failed_hooks.append(callback)
        return callback

    def _emit_run_start(self, run: Run) -> None:
        """Invoke all registered on_run_start hooks."""
        for hook in self._on_run_start_hooks:
            try:
                hook(run)
            except Exception as e:
                logging.warning(f"[Consist] on_run_start hook failed: {e}")

    def _emit_run_complete(self, run: Run, outputs: List[Artifact]) -> None:
        """Invoke all registered on_run_complete hooks."""
        for hook in self._on_run_complete_hooks:
            try:
                hook(run, outputs)
            except Exception as e:
                logging.warning(f"[Consist] on_run_complete hook failed: {e}")

    def _emit_run_failed(self, run: Run, error: Exception) -> None:
        """Invoke all registered on_run_failed hooks."""
        for hook in self._on_run_failed_hooks:
            try:
                hook(run, error)
            except Exception as e:
                logging.warning(f"[Consist] on_run_failed hook failed: {e}")

    @property
    def last_run(self) -> Optional[ConsistRecord]:
        """
        Returns the record of the most recently completed run (successful or failed).
        Useful for debugging and introspection in notebooks.
        """
        return self._last_consist

    def history(
        self, limit: int = 10, tags: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Returns a Pandas DataFrame of recent runs from the database.
        Useful for quickly verifying run status and provenance.

        Parameters
        ----------
        limit : int, default 10
            Maximum number of runs to return.
        tags : Optional[List[str]], optional
            If provided, filter runs to only those containing ALL specified tags.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing run information including timing and duration.
        """
        if not self.engine:
            return pd.DataFrame()

        query = f"""
            SELECT id, model_name, status, tags,
                   started_at, ended_at,
                   CASE
                       WHEN ended_at IS NOT NULL AND started_at IS NOT NULL
                       THEN EXTRACT(EPOCH FROM (ended_at - started_at))
                       ELSE NULL
                   END as duration_seconds,
                   config_hash, git_hash, input_hash,
                   created_at
            FROM run
            ORDER BY created_at DESC
            LIMIT {limit}
        """
        try:
            df = pd.read_sql(query, self.engine)
            # Filter by tags if specified (post-query filtering for JSON column compatibility)
            if tags and not df.empty:

                def has_all_tags(run_tags):
                    if run_tags is None:
                        return False
                    return all(t in run_tags for t in tags)

                df = df[df["tags"].apply(has_all_tags)]
            return df
        except Exception as e:
            logging.warning(f"Failed to fetch history: {e}")
            return pd.DataFrame()

    @property
    def is_cached(self) -> bool:
        """
        Returns True if the current run is a valid Cache Hit.
        This means a `cached_run` (a previously completed `Run` with a matching signature)
        was found and its outputs were successfully validated (e.g., files exist or data
        is in the database, also known as "Ghost Mode").
        """
        return self.current_consist and self.current_consist.cached_run is not None

    def find_matching_run(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> Optional[Run]:
        """
        Attempts to find a previously "completed" run that matches the given
        `config_hash`, `input_hash`, and `git_hash`. This method is central
        to Consist's **Smart Caching Strategy** and performs the **Signature Lookup**
        to identify potential cache hits.

        Args:
            config_hash (str): The hash of the run's configuration.
            input_hash (str): The hash derived from the run's input artifacts' provenance IDs.
            git_hash (str): The Git commit hash of the code version.

        Returns:
            Optional[Run]: The most recent matching `Run` object if a cache hit is found,
                           otherwise None.
        """
        if not self.engine:
            return None

        def _query():
            with Session(self.engine) as session:
                statement = (
                    select(Run)
                    .where(Run.status == "completed")
                    .where(Run.config_hash == config_hash)
                    .where(Run.input_hash == input_hash)
                    .where(Run.git_hash == git_hash)
                    .order_by(Run.created_at.desc())
                    .limit(1)
                )
                return session.exec(statement).first()

        try:
            return self._execute_with_retry(_query, operation_name="cache_lookup")
        except Exception as e:
            # If it still fails after retries, log warning and return None (Cache Miss)
            logging.warning(f"[Consist Warning] Cache lookup failed after retries: {e}")
            return None

    def _validate_run_outputs(self, run: Run) -> bool:
        """
        Verifies that all output artifacts of a cached run are still accessible.

        This method is crucial for implementing **"Ghost Mode"** (where data in DB
        allows cache hits even if files are missing) and **"Self-Healing"** (forcing
        a re-run if data is missing from both disk and DB). It checks if each output
        artifact associated with the given run exists on disk or is marked as ingested
        in the database.

        Parameters
        ----------
        run : Run
            The cached `Run` object whose outputs need validation.

        Returns
        -------
        bool
            True if all outputs are accessible (either on disk or ingested), False otherwise.
        """
        if not self.engine:
            return True

        with Session(self.engine) as session:
            statement = (
                select(Artifact)
                .join(RunArtifactLink, Artifact.id == RunArtifactLink.artifact_id)
                .where(RunArtifactLink.run_id == run.id)
                .where(RunArtifactLink.direction == "output")
            )
            outputs = session.exec(statement).all()

            for art in outputs:
                resolved_path = self.resolve_uri(art.uri)
                on_disk = Path(resolved_path).exists()
                in_db = art.meta.get("is_ingested", False)

                if not on_disk and not in_db:
                    logging.warning(
                        f"⚠️ [Consist] Cache Validation Failed. Missing: {art.uri}"
                    )
                    return False
            return True

    def _execute_with_retry(
        self, func: Callable, operation_name: str = "db_op", retries: int = 20
    ) -> Any:
        """
        Executes a given function with retry logic and exponential backoff for database lock errors.

        This method is crucial for handling concurrent access to the DuckDB database,
        especially when multiple processes or threads might try to write simultaneously.
        It specifically catches `OperationalError` and `DatabaseError` which often
        indicate locking issues in DuckDB/SQLite.

        Parameters
        ----------
        func : Callable
            The function to execute. This function should contain the database operation
            that might fail due to locking.
        operation_name : str, default "db_op"
            A descriptive name for the operation being performed, used in logging messages.
        retries : int, default 20
            The maximum number of times to retry the function execution before giving up.

        Returns
        -------
        Any
            The result of the executed function `func`.

        Raises
        ------
        ConcurrentModificationError
            If the function fails to execute successfully after all retries due to a
            persistent database lock.
        Exception
            Any other exception raised by `func` that is not related to database locking
            will be re-raised immediately.
        """
        for i in range(retries):
            try:
                return func()
            except (OperationalError, DatabaseError) as e:
                # Check for DuckDB lock strings
                msg = str(e)
                # Broader check for lock messages
                if (
                    "Conflicting lock" in msg
                    or "IO Error" in msg
                    or "Could not set lock" in msg
                    or "database is locked" in msg  # common in sqlite/duckdb
                ):
                    if i == retries - 1:
                        logging.error(
                            f"[Consist] DB Lock Timeout on {operation_name} after {retries} attempts. Last error: {msg}"
                        )
                        raise e

                    # Exponential Backoff with Jitter
                    # slightly increased backoff factor to handling higher contention
                    sleep_time = (0.1 * (1.5**i)) + random.uniform(0.05, 0.2)
                    sleep_time = min(sleep_time, 2.0)  # Cap at 2 seconds
                    sleep(sleep_time)
                else:
                    raise e
            except Exception as e:
                raise e
        raise ConcurrentModificationError("Concurrency problem")

    def task(
        self,
        cache_mode: str = "reuse",
        depends_on: Optional[List[Union[str, Path, Artifact]]] = None,
        capture_dir: Optional[Union[str, Path]] = None,
        capture_pattern: str = "*",
        **run_kwargs: Any,
    ) -> Callable:
        """
        Decorator to register a function as a Consist task, enabling provenance tracking and caching.

        This decorator wraps a function, automatically managing its execution within a
        `Consist` run context. It handles:
        -   **Argument Resolution**: Automatically detects `Artifact` objects passed as arguments
            or within `depends_on` and logs them as inputs.
        -   **Configuration Hashing**: Computes a configuration hash from the function's
            arguments and `run_kwargs`.
        -   **Cache Management**: Based on `cache_mode`, it performs cache lookups and
            either reuses previous results (cache hit) or executes the function (cache miss).
        -   **Output Capture**: If `capture_dir` is specified, it automatically logs new
            or modified files in that directory as output artifacts.
        -   **Return Value Handling**: Logs the function's return value (if it's a path or a dict of paths)
            as an output artifact.

        Parameters
        ----------
        cache_mode : str, default "reuse"
            Strategy for caching.
            "reuse": Attempts to find and reuse a previously cached run.
            "overwrite": Executes the task and updates the cache with new results.
            "readonly": Executes the task but does not save new results to the cache.
        depends_on : Optional[List[Union[str, Path, Artifact]]], optional
            A list of explicit dependencies for the task. These can be file paths (str or Path)
            or `Artifact` objects. Files will be hashed as inputs. If a glob pattern is provided
            for a path, all matching files will be included as dependencies.
        capture_dir : Optional[Union[str, Path]], optional
            If provided, Consist will automatically log any new or modified files
            within this directory as output artifacts after the decorated function
            completes. The function decorated with `capture_dir` must return `None`.
        capture_pattern : str, default "*"
            A glob pattern to filter files captured by `capture_dir`.
        **run_kwargs : Any
            Additional keyword arguments passed directly to the `start_run` method,
            such as `year`, `iteration`, or custom metadata.

        Returns
        -------
        Callable
            The decorated function, which, when called, will execute within a Consist run context.

        Raises
        ------
        ValueError
            If `capture_dir` is used and the decorated function returns a non-None value.
            If a dictionary is returned, but its values are not `Path` objects.
        FileNotFoundError
            If the task returns a path that does not exist.
        TypeError
            If the task returns an unsupported type.

        Notes
        -----
        The decorated function's arguments can be `Artifact` objects directly,
        which will automatically be logged as inputs.
        """

        def decorator(func: Callable) -> Callable:
            """
            The actual decorator function that wraps the user's task function.

            Parameters
            ----------
            func : Callable
                The user-defined function to be wrapped as a Consist task.

            Returns
            -------
            Callable
                A wrapped function that includes provenance tracking, caching, and output management.
            """

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                """
                The wrapper function that executes the Consist task with provenance and caching.

                This function intercepts the call to the original task function, sets up
                the Consist run context, handles input/output logging, cache lookup,
                and output capture, and then executes the original function if necessary.

                Parameters
                ----------
                *args : Any
                    Positional arguments passed to the original task function.
                **kwargs : Any
                    Keyword arguments passed to the original task function.

                Returns
                -------
                Any
                    The return value of the original task function, potentially
                    wrapped as an `Artifact` or a dictionary of `Artifact`s if
                    `capture_dir` is not used. If `capture_dir` is used, returns
                    a list of captured `Artifact`s.

                Raises
                ------
                ValueError
                    If `capture_dir` is specified and the decorated function returns a non-None value.
                FileNotFoundError
                    If the task returns a path that does not exist.
                TypeError
                    If the task returns an unsupported type.
                """
                sig = inspect.signature(func)
                try:
                    bound = sig.bind(*args, **kwargs)
                except TypeError as e:
                    logging.warning(
                        f"[Consist] Signature binding failed for @task {func.__name__}: {e}"
                    )
                    bound = None

                config = {}
                inputs = []

                if bound:
                    bound.apply_defaults()
                    raw_args = bound.arguments
                    for k, v in raw_args.items():
                        if isinstance(v, Artifact):
                            inputs.append(v)
                            config[k] = v.uri
                        elif isinstance(v, list) and v and isinstance(v[0], Artifact):
                            inputs.extend(v)
                            config[k] = [a.uri for a in v]
                        else:
                            config[k] = v

                if depends_on:
                    for dep in depends_on:
                        if isinstance(dep, Artifact):
                            inputs.append(dep)
                            config[f"dep_{dep.key}"] = dep.uri
                        elif isinstance(dep, (str, Path)):
                            # Check for Glob Patterns
                            dep_str = str(dep)
                            if "*" in dep_str or "?" in dep_str or "[" in dep_str:
                                # Expand Glob
                                # Note: Glob is relative to CWD unless absolute
                                p = Path(dep_str)
                                root = Path(".").resolve()
                                if p.is_absolute():
                                    root = p.anchor
                                    matcher = p.relative_to(root)
                                else:
                                    matcher = p

                                found_any = False
                                for match in root.glob(str(matcher)):
                                    if match.exists():
                                        found_any = True
                                        inputs.append(str(match.resolve()))

                                if not found_any:
                                    logging.warning(
                                        f"[Consist] No files found matching dependency pattern: {dep}"
                                    )

                            else:
                                # Normal Path (File or Directory)
                                p = Path(dep).resolve()
                                if p.exists():
                                    inputs.append(str(p))
                                else:
                                    logging.warning(
                                        f"[Consist] Dependency not found: {dep}"
                                    )

                model_name = func.__name__
                run_id = f"{model_name}_{uuid.uuid4().hex[:8]}"

                with self.start_run(
                    run_id=run_id,
                    model=model_name,
                    config=config,
                    inputs=inputs,
                    cache_mode=cache_mode,
                    **run_kwargs,
                ):
                    if self.is_cached:
                        outputs = self.current_consist.outputs
                        if capture_dir:
                            return outputs
                        if not outputs:
                            return None
                        if len(outputs) == 1:
                            return outputs[0]
                        return outputs

                    if capture_dir:
                        with self.capture_outputs(
                            capture_dir, pattern=capture_pattern
                        ) as cap:
                            result = func(*args, **kwargs)
                        if result is not None:
                            raise ValueError(
                                f"Task '{model_name}' defines 'capture_dir', so it must return None."
                            )
                        return cap.artifacts

                    else:
                        result = func(*args, **kwargs)
                        if isinstance(result, (str, Path)):
                            p = Path(result)
                            if not p.exists():
                                raise FileNotFoundError(
                                    f"Task returned path {p} which does not exist."
                                )
                            return self.log_artifact(p, key=p.stem)
                        elif isinstance(result, dict):
                            logged_dict = {}
                            for key, val in result.items():
                                if not isinstance(val, (str, Path)):
                                    raise ValueError(
                                        "Task dictionary return values must be Paths."
                                    )
                                logged_dict[key] = self.log_artifact(val, key=key)
                            return logged_dict
                        elif result is None:
                            return None
                        else:
                            raise TypeError(
                                f"Task '{model_name}' returned unsupported type {type(result)}."
                            )

            return wrapper

        return decorator

    @contextmanager
    def capture_outputs(
        self, directory: Union[str, Path], pattern: str = "*", recursive: bool = False
    ) -> OutputCapture:
        """
        A context manager to automatically capture and log new or modified files in a directory.

        This context manager is used within a `@task` function or `start_run` block
        to monitor a specified directory. Any files created or modified within this
        directory during the execution of the `with` block will be automatically
        logged as output artifacts of the current run.

        Parameters
        ----------
        directory : Union[str, Path]
            The path to the directory to monitor for new or modified files.
        pattern : str, default "*"
            A glob pattern (e.g., "*.csv", "data_*.parquet") to filter which files
            are captured within the specified directory. Defaults to all files.
        recursive : bool, default False
            If True, the capture will recursively scan subdirectories within `directory`.

        Yields
        ------
        OutputCapture
            An `OutputCapture` object containing a list of `Artifact` objects that were
            captured and logged after the `with` block finishes.

        Raises
        ------
        RuntimeError
            If `capture_outputs` is used outside of an active `start_run` context.
        """
        if not self.current_consist:
            raise RuntimeError(
                "capture_outputs must be used within a start_run context."
            )

        dir_path = Path(directory).resolve()
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)

        def _scan():
            files = {}
            iterator = dir_path.rglob(pattern) if recursive else dir_path.glob(pattern)
            for f in iterator:
                if f.is_file():
                    files[f] = f.stat().st_mtime_ns
            return files

        before_state = _scan()
        capture_result = OutputCapture()

        try:
            yield capture_result
        finally:
            after_state = _scan()
            for f_path, mtime in after_state.items():
                is_new = f_path not in before_state
                is_modified = f_path in before_state and mtime > before_state[f_path]

                if is_new or is_modified:
                    key = f_path.stem
                    try:
                        art = self.log_artifact(
                            str(f_path),
                            key=key,
                            direction="output",
                            captured_automatically=True,
                        )
                        capture_result.artifacts.append(art)
                        logging.info(f"📸 [Consist] Captured output: {f_path.name}")
                    except Exception as e:
                        logging.error(f"[Consist] Failed to auto-capture {f_path}: {e}")

    @contextmanager
    def start_run(
        self,
        run_id: str,
        model: str,
        config: Union[Dict[str, Any], BaseModel, None] = None,
        inputs: Optional[List[Union[str, Artifact]]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
        cache_mode: str = "reuse",
        **kwargs: Any,
    ) -> "Tracker":
        """
        Context manager to initiate and manage the lifecycle of a Consist run.

        This is the primary entry point for defining a reproducible and observable unit
        of work. It wraps the imperative `begin_run()`/`end_run()` methods to provide
        automatic cleanup and exception handling.

        Parameters
        ----------
        run_id : str
            A unique identifier for the current run.
        model : str
            A descriptive name for the model or process being executed.
        config : Union[Dict[str, Any], BaseModel, None], optional
            Configuration parameters for this run, hashed to form part of run identity.
        inputs : Optional[List[Union[str, Artifact]]], optional
            Input paths or Artifact objects that this run depends on.
        tags : Optional[List[str]], optional
            String labels for categorization and filtering.
        description : Optional[str], optional
            Human-readable description of the run's purpose.
        cache_mode : str, default "reuse"
            Strategy for caching:
            - "reuse": Attempts to find a matching run in the cache. If found and valid,
                       the current run becomes a cache hit and its outputs are hydrated.
                       Otherwise, the run executes.
            - "overwrite": The run will always execute, and its results will overwrite
                           any existing cache entry for its signature.
            - "readonly": The run will perform a cache lookup. If a hit, outputs are hydrated.
                          If a miss, the run executes but its results are NOT saved to the cache.
        **kwargs : Any
            Additional metadata. Special keywords `year` and `iteration` can be used.

        Yields
        ------
        Tracker
            The current `Tracker` instance for use within the `with` block.

        Raises
        ------
        Exception
            Any exception raised within the `with` block will be caught, the run
            marked as "failed", and then re-raised after cleanup.

        See Also
        --------
        begin_run : Imperative alternative for starting runs.
        end_run : Imperative alternative for ending runs.
        """
        self.begin_run(
            run_id=run_id,
            model=model,
            config=config,
            inputs=inputs,
            tags=tags,
            description=description,
            cache_mode=cache_mode,
            **kwargs,
        )
        try:
            yield self
            self.end_run(status="completed")
        except Exception as e:
            self.end_run(status="failed", error=e)
            raise

    def begin_run(
        self,
        run_id: str,
        model: str,
        config: Union[Dict[str, Any], BaseModel, None] = None,
        inputs: Optional[List[Union[str, Artifact]]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
        cache_mode: str = "reuse",
        **kwargs: Any,
    ) -> Run:
        """
        Start a run imperatively (without context manager).

        Use this when run start and end are in separate methods, or when integrating
        with frameworks that have their own lifecycle management. Returns the Run object.
        Call end_run() when complete.

        This provides an alternative to the context manager pattern when you need more
        control over the run lifecycle, such as in PILATES-like integrations where
        start_model_run() and complete_model_run() are separate method calls.

        Parameters
        ----------
        run_id : str
            A unique identifier for the current run.
        model : str
            A descriptive name for the model or process being executed.
        config : Union[Dict[str, Any], BaseModel, None], optional
            Configuration parameters for this run.
        inputs : Optional[List[Union[str, Artifact]]], optional
            A list of input paths or Artifact objects.
        tags : Optional[List[str]], optional
            A list of string labels for categorization and filtering.
        description : Optional[str], optional
            A human-readable description of the run's purpose.
        cache_mode : str, default "reuse"
            Strategy for caching: "reuse", "overwrite", or "readonly".
        **kwargs : Any
            Additional metadata. Special keywords `year` and `iteration` can be used.

        Returns
        -------
        Run
            The Run object representing the started run.

        Raises
        ------
        RuntimeError
            If there is already an active run.

        Example
        -------
        ```python
        run = tracker.begin_run("run_001", "urbansim", config={...})
        try:
            tracker.log_artifact(input_file, direction="input")
            # ... do work ...
            tracker.log_artifact(output_file, direction="output")
            tracker.end_run("completed")
        except Exception as e:
            tracker.end_run("failed", error=e)
            raise
        ```
        """
        if self.current_consist is not None:
            raise RuntimeError(
                f"Cannot begin_run: A run is already active (id={self.current_consist.run.id}). "
                "Call end_run() first."
            )

        if config is None:
            config = {}
        elif isinstance(config, BaseModel):
            config = config.model_dump()

        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)

        # Compute core identity hashes early
        config_hash = self.identity.compute_config_hash(config)
        git_hash = self.identity.get_code_version()

        now = datetime.now(UTC)
        run = Run(
            id=run_id,
            model_name=model,
            description=description,
            year=year,
            iteration=iteration,
            tags=tags or [],
            status="running",
            config_hash=config_hash,
            git_hash=git_hash,
            meta=kwargs,
            started_at=now,
            created_at=now,
        )

        push_tracker(self)
        self.current_consist = ConsistRecord(run=run, config=config)
        self._active_run_cache_mode = cache_mode

        # Process Inputs & Auto-Forking
        if inputs:
            for item in inputs:
                if isinstance(item, Artifact):
                    self.log_artifact(item, direction="input")
                else:
                    key = Path(item).stem
                    self.log_artifact(item, key=key, direction="input")

        # Detect Parent Lineage
        parent_candidates = [
            a.run_id for a in self.current_consist.inputs if a.run_id is not None
        ]
        if parent_candidates:
            run.parent_run_id = parent_candidates[-1]

        # Identity Completion: Compute Merkle DAG signature
        try:
            input_hash = self.identity.compute_input_hash(
                self.current_consist.inputs, path_resolver=self.resolve_uri
            )
            run.input_hash = input_hash
            run.signature = self.identity.calculate_run_signature(
                code_hash=git_hash, config_hash=config_hash, input_hash=input_hash
            )
        except Exception as e:
            logging.warning(
                f"[Consist Warning] Failed to compute inputs hash for run {run_id}: {e}"
            )
            run.input_hash = "error"
            run.signature = "error"

        # Cache Lookup (Smart Caching)
        if cache_mode == "reuse":
            cached_run = self.find_matching_run(
                config_hash=run.config_hash,
                input_hash=run.input_hash,
                git_hash=run.git_hash,
            )
            if cached_run:
                if self._validate_run_outputs(cached_run):
                    self.current_consist.cached_run = cached_run
                    logging.info(
                        f"✅ [Consist] Cache HIT! Matching run: {cached_run.id}"
                    )
                    # HYDRATE OUTPUTS
                    with Session(self.engine) as session:
                        statement = (
                            select(Artifact)
                            .join(
                                RunArtifactLink,
                                Artifact.id == RunArtifactLink.artifact_id,
                            )
                            .where(RunArtifactLink.run_id == cached_run.id)
                            .where(RunArtifactLink.direction == "output")
                        )
                        cached_outputs = session.exec(statement).all()
                        for art in cached_outputs:
                            session.expunge(art)
                            art.abs_path = self.resolve_uri(art.uri)
                            self.current_consist.outputs.append(art)
                else:
                    logging.info(
                        "🔄 [Consist] Cache Miss (Data Missing or Invalidated). Re-running..."
                    )
        elif cache_mode == "overwrite":
            logging.warning(
                "⚠️ [Consist] Cache lookup skipped (Mode: Overwrite). Run will execute and update cache."
            )
        elif cache_mode == "readonly":
            logging.info(
                "👁️ [Consist] Cache lookup in read-only mode. New results will not be saved."
            )

        # Dual-Write Logging
        self._flush_json()
        self._sync_run_to_db(run)

        # Emit on_run_start hooks
        self._emit_run_start(run)

        return run

    def end_run(
        self,
        status: str = "completed",
        error: Optional[Exception] = None,
    ) -> Run:
        """
        End the current run started with begin_run().

        This method finalizes the run, persists the final state to JSON and database,
        and emits lifecycle hooks. It is idempotent - calling it multiple times
        on an already-ended run will log a warning but not raise an error.

        Parameters
        ----------
        status : str, default "completed"
            The final status of the run. Typically "completed" or "failed".
        error : Optional[Exception], optional
            The exception that caused the failure, if status is "failed".
            The error message will be stored in the run's metadata.

        Returns
        -------
        Run
            The completed Run object.

        Raises
        ------
        RuntimeError
            If there is no active run to end.

        Example
        -------
        ```python
        run = tracker.begin_run("run_001", "urbansim")
        try:
            # ... do work ...
            tracker.end_run("completed")
        except Exception as e:
            tracker.end_run("failed", error=e)
            raise
        ```
        """
        if not self.current_consist:
            raise RuntimeError("No active run to end. Call begin_run() first.")

        run = self.current_consist.run
        cache_mode = self._active_run_cache_mode or "reuse"

        # Update run status
        if cache_mode != "readonly":
            run.status = status
        else:
            run.status = "skipped_save" if status == "completed" else status

        if error:
            run.meta["error"] = str(error)

        # Clean up global tracker context
        pop_tracker()

        # Snapshot the result for introspection
        self._last_consist = self.current_consist

        # Set timing fields
        end_time = datetime.now(UTC)
        run.ended_at = end_time
        run.updated_at = end_time

        # Persist final state
        self._flush_json()
        if cache_mode != "readonly":
            self._sync_run_to_db(run)

        # Emit lifecycle hooks
        if error is not None:
            self._emit_run_failed(run, error)
        elif run.status == "completed":
            outputs = self.current_consist.outputs if self.current_consist else []
            self._emit_run_complete(run, outputs)

        # Clear current run context
        self.current_consist = None
        self._active_run_cache_mode = None

        return run

    def log_artifact(
        self,
        path: Union[str, Artifact],
        key: Optional[str] = None,
        direction: str = "output",
        schema: Optional[Type[SQLModel]] = None,
        driver: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Logs an artifact (file or data reference) within the current run context.

        This method supports:
        -   **Automatic Input Discovery**: If an input `path` matches a previously
            logged output artifact, Consist automatically links them, building the
            provenance graph. This is a key part of **"Auto-Forking"**.
        -   **Path Virtualization**: Converts absolute file system paths to portable URIs
            (e.g., `inputs://data.csv`) using configured mounts, adhering to
            **"Path Resolution & Mounts"**.
        -   **Schema Metadata Injection**: Embeds schema information (if provided) into the
            artifact's metadata, useful for later "Strict Mode" validation or introspection.

        Parameters
        ----------
        path : Union[str, Artifact]
            The file path (str) or an existing `Artifact` object to be logged.
        key : Optional[str], optional
            A semantic, human-readable name for the artifact (e.g., "households").
            Required if `path` is a string.
        direction : str, default "output"
            Specifies whether the artifact is an "input" or "output" for the
            current run. Defaults to "output".
        schema : Optional[Type[SQLModel]], optional
            An optional SQLModel class that defines the expected schema for the artifact's data.
            Its name will be stored in artifact metadata.
        driver : Optional[str], optional
            Explicitly specify the driver (e.g., 'h5_table').
            If None, the driver is inferred from the file extension.
        **meta : Any
            Additional key-value pairs to store in the artifact's flexible `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        ValueError
            If `key` is not provided when `path` is a string.
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        artifact_obj = None
        resolved_abs_path = None

        # --- Logic Branch A: Artifact Object Passed (Explicit Chaining/Re-logging) ---
        if isinstance(path, Artifact):
            artifact_obj = path
            # Resolve absolute path from existing artifact URI or _abs_path if already set
            resolved_abs_path = artifact_obj.abs_path or self.resolve_uri(
                artifact_obj.uri
            )
            if key is None:
                key = artifact_obj.key  # Use existing key if not overridden
            if driver:
                artifact_obj.driver = driver
            if meta:
                artifact_obj.meta.update(meta)

        # --- Logic Branch B: String Path Passed (New or Discovered Artifact) ---
        else:
            if key is None:
                raise ValueError("Argument 'key' required when 'path' is a string.")

            # Resolve to absolute path and then virtualize for storage
            resolved_abs_path = str(Path(path).resolve())
            uri = self._virtualize_path(resolved_abs_path)

            # 1. Lineage Discovery (Automatic Input Discovery)
            # For input artifacts, check if an artifact with this URI was previously output by another run.
            if direction == "input" and self.engine:
                try:
                    with Session(self.engine) as session:
                        # Find the most recent artifact created at this location with a run_id
                        statement = (
                            select(Artifact)
                            .where(Artifact.uri == uri)
                            .where(Artifact.run_id.is_not(None))
                            .order_by(Artifact.created_at.desc())
                            .limit(1)
                        )
                        parent = session.exec(statement).first()

                        if parent:
                            # LINEAGE FOUND! Reuse the existing artifact object to link to its creator run.
                            # Detach from session so we can use it in our current flow without binding issues
                            session.expunge(parent)
                            artifact_obj = parent
                            if driver:
                                artifact_obj.driver = driver
                            if meta:
                                artifact_obj.meta.update(meta)
                except Exception as e:
                    logging.warning(
                        f"[Consist Warning] Lineage discovery failed for {uri}: {e}"
                    )

            # 2. If no parent artifact found or it's an output, create a fresh Artifact object
            if artifact_obj is None:
                # Infer driver if not provided
                if driver is None:
                    driver = Path(path).suffix.lstrip(".").lower() or "unknown"

                # Compute content hash for the artifact
                content_hash = None
                try:
                    content_hash = self.identity._compute_file_checksum(
                        resolved_abs_path
                    )
                except Exception as e:
                    logging.warning(
                        f"[Consist Warning] Failed to compute hash for {path}: {e}"
                    )

                artifact_obj = Artifact(
                    key=key,
                    uri=uri,
                    driver=driver,
                    hash=content_hash,
                    run_id=(
                        self.current_consist.run.id if direction == "output" else None
                    ),
                    meta=meta,
                )

        # --- Common Logic (Applies to both branches) ---

        # Schema Metadata Injection
        if schema:
            artifact_obj.meta["schema_name"] = schema.__name__
            artifact_obj.meta["has_strict_schema"] = True

        # Attach Runtime Absolute Path (Vital for chaining and external tools)
        # This is a PrivateAttr and not persisted to the DB, part of "Artifact Chaining (Runtime vs Persistence)"
        artifact_obj.abs_path = resolved_abs_path

        # Update In-Memory State (The ConsistRecord being built for the current run)
        if direction == "input":
            self.current_consist.inputs.append(artifact_obj)
        else:
            self.current_consist.outputs.append(artifact_obj)

        self._flush_json()
        self._sync_artifact_to_db(artifact_obj, direction)

        return artifact_obj

    def log_artifacts(
        self,
        paths: List[Union[str, Path]],
        direction: str = "output",
        driver: Optional[str] = None,
        **shared_meta: Any,
    ) -> List[Artifact]:
        """
        Log multiple artifacts in a single call for efficiency.

        This is a convenience method for bulk artifact logging, particularly useful
        when a model produces many output files or when registering multiple inputs.
        Each path is logged as a separate artifact, with the filename stem used as the key.

        Parameters
        ----------
        paths : List[Union[str, Path]]
            A list of file paths to log as artifacts.
        direction : str, default "output"
            Specifies whether the artifacts are "input" or "output" for the current run.
        driver : Optional[str], optional
            Explicitly specify the driver for all artifacts. If None, driver is inferred
            from each file's extension individually.
        **shared_meta : Any
            Metadata key-value pairs to apply to ALL logged artifacts.
            Useful for tagging a batch of related files.

        Returns
        -------
        List[Artifact]
            A list of the created `Artifact` objects, in the same order as the input paths.

        Raises
        ------
        RuntimeError
            If called outside an active run context.

        Example
        -------
        ```python
        # Log all CSV files from a directory
        csv_files = list(Path("./outputs").glob("*.csv"))
        artifacts = tracker.log_artifacts(csv_files, direction="output", batch="run_001")

        # Log specific input files
        inputs = tracker.log_artifacts(
            ["data/train.parquet", "data/test.parquet"],
            direction="input",
            dataset_version="v2"
        )
        ```
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifacts outside of a run context.")

        artifacts = []
        for path in paths:
            path_obj = Path(path)
            key = path_obj.stem
            art = self.log_artifact(
                str(path_obj),
                key=key,
                direction=direction,
                driver=driver,
                **shared_meta,
            )
            artifacts.append(art)

        return artifacts

    def log_input(
        self,
        path: Union[str, Artifact],
        key: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Log an input artifact. Convenience wrapper for log_artifact(direction='input').

        Parameters
        ----------
        path : Union[str, Artifact]
            The file path (str) or an existing `Artifact` object to be logged.
        key : Optional[str], optional
            A semantic, human-readable name for the artifact.
        **meta : Any
            Additional key-value pairs to store in the artifact's `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.
        """
        return self.log_artifact(path, key=key, direction="input", **meta)

    def log_output(
        self,
        path: Union[str, Artifact],
        key: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Log an output artifact. Convenience wrapper for log_artifact(direction='output').

        Parameters
        ----------
        path : Union[str, Artifact]
            The file path (str) or an existing `Artifact` object to be logged.
        key : Optional[str], optional
            A semantic, human-readable name for the artifact.
        **meta : Any
            Additional key-value pairs to store in the artifact's `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.
        """
        return self.log_artifact(path, key=key, direction="output", **meta)

    def log_h5_container(
        self,
        path: Union[str, Path],
        key: Optional[str] = None,
        direction: str = "output",
        discover_tables: bool = True,
        table_filter: Optional[Union[Callable[[str], bool], List[str]]] = None,
        **meta: Any,
    ) -> Tuple[Artifact, List[Artifact]]:
        """
        Log an HDF5 file and optionally discover its internal tables.

        This method provides first-class HDF5 container support, automatically
        discovering and logging internal tables as child artifacts. This is
        particularly useful for PILATES-like workflows that extensively use
        HDF5 files containing multiple tables.

        Parameters
        ----------
        path : Union[str, Path]
            Path to the HDF5 file.
        key : Optional[str], optional
            Semantic name for the container. If not provided, uses the file stem.
        direction : str, default "output"
            Whether this is an "input" or "output" artifact.
        discover_tables : bool, default True
            If True, scan the file and create child artifacts for each table/dataset.
        table_filter : Optional[Union[Callable[[str], bool], List[str]]], optional
            Filter which tables to log. Can be:
            - A callable that takes a table name and returns True to include
            - A list of table names to include (exact match)
            If None, all tables are included.
        **meta : Any
            Additional metadata for the container artifact.

        Returns
        -------
        Tuple[Artifact, List[Artifact]]
            A tuple of (container_artifact, list_of_table_artifacts).

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        ImportError
            If h5py is not installed and discover_tables is True.

        Example
        -------
        ```python
        # Log HDF5 file with auto-discovery of all tables
        container, tables = tracker.log_h5_container("data.h5", key="urbansim_data")
        print(f"Logged {len(tables)} tables from container")

        # Filter tables by callable
        container, tables = tracker.log_h5_container(
            "data.h5",
            key="urbansim_data",
            table_filter=lambda name: name.startswith("/2025/")
        )

        # Filter tables by list of names
        container, tables = tracker.log_h5_container(
            "data.h5",
            key="urbansim_data",
            table_filter=["households", "persons", "buildings"]
        )
        ```
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        path_obj = Path(path)
        if key is None:
            key = path_obj.stem

        # Log the container artifact
        container = self.log_artifact(
            str(path_obj),
            key=key,
            direction=direction,
            driver="h5",
            is_container=True,
            **meta,
        )

        table_artifacts: List[Artifact] = []

        if discover_tables:
            try:
                import h5py
            except ImportError:
                logging.warning(
                    "[Consist] h5py not installed. Cannot discover HDF5 tables. "
                    "Install with: pip install h5py"
                )
                return container, table_artifacts

            # Build filter function
            if table_filter is None:
                filter_fn = lambda name: True
            elif isinstance(table_filter, list):
                # Convert list to set for O(1) lookup
                filter_set = set(table_filter)
                filter_fn = (
                    lambda name: name in filter_set or name.lstrip("/") in filter_set
                )
            else:
                filter_fn = table_filter

            try:
                with h5py.File(str(path_obj), "r") as f:

                    def visit_datasets(name: str, obj: Any) -> None:
                        """Visitor function to find all datasets in the HDF5 file."""
                        import h5py as h5

                        if isinstance(obj, h5.Dataset):
                            if filter_fn(name):
                                # Create a unique key for this table
                                table_key = f"{key}_{name.replace('/', '_')}"

                                table_art = self.log_artifact(
                                    str(path_obj),
                                    key=table_key,
                                    direction=direction,
                                    driver="h5_table",
                                    parent_id=str(container.id),
                                    table_path=name,
                                    shape=list(obj.shape),
                                    dtype=str(obj.dtype),
                                )
                                table_artifacts.append(table_art)

                    f.visititems(visit_datasets)

            except Exception as e:
                logging.warning(f"[Consist] Failed to discover HDF5 tables: {e}")

        # Update container metadata with table info
        container.meta["table_count"] = len(table_artifacts)
        container.meta["table_ids"] = [str(t.id) for t in table_artifacts]

        # Re-sync the container with updated metadata
        self._flush_json()
        self._sync_artifact_to_db(container, direction)

        return container, table_artifacts

    def ingest(
        self,
        artifact: Artifact,
        data: Optional[Union[Iterable[Dict[str, Any]], Any]] = None,
        schema: Optional[Type[SQLModel]] = None,
        run: Optional[Run] = None,
    ) -> Any:
        """
        Ingests data associated with an `Artifact` into the Consist DuckDB database.

        This method is central to Consist's **"Hot Data Strategy"**, where data is
        materialized into the database for faster query performance and easier sharing.
        It leverages the `dlt` (Data Load Tool) integration for efficient and robust
        data loading, including support for schema inference and evolution.

        Parameters
        ----------
        artifact : Artifact
            The artifact object representing the data being ingested. Its metadata
            might include schema information.
        data : Optional[Union[Iterable[Dict[str, Any]], Any]], optional
            An iterable (e.g., list of dicts, generator) where each item represents a
            row of data to be ingested. If `data` is omitted, Consist attempts to
            stream it directly from the artifact's file URI, resolving the path.
            Can also be other data types that `dlt` can handle directly (e.g., Pandas DataFrame).
        schema : Optional[Type[SQLModel]], optional
            An optional SQLModel class that defines the expected schema for the ingested data.
            If provided, `dlt` will use this for strict validation.
        run : Optional[Run], optional
            If provided, tags data with this run's ID (Offline Mode).
            If None, uses the currently active run (Online Mode).

        Returns
        -------
        Any
            The result information from the `dlt` ingestion process.

        Raises
        ------
        RuntimeError
            If no database is configured (`db_path` was not provided during
            Tracker initialization) or if `ingest` is called outside of
            an active run context.
        Exception
            Any exception raised by the underlying `dlt` ingestion process.
        """
        if not self.db_path:
            raise RuntimeError("Cannot ingest data: No database configured.")
        target_run = run or (self.current_consist.run if self.current_consist else None)
        if not target_run:
            raise RuntimeError("Cannot ingest data: No active run context.")

        if self.engine:
            self.engine.dispose()
        from consist.integrations.dlt_loader import ingest_artifact

        # Auto-Resolve Data if None
        data_to_pass = data
        if data_to_pass is None:
            # If no data provided, we assume we should read from the artifact's file
            # We resolve the URI to an absolute path string
            data_to_pass = self.resolve_uri(artifact.uri)

        try:
            info, resource_name = ingest_artifact(
                artifact=artifact,
                run_context=target_run,
                db_path=self.db_path,
                data_iterable=data_to_pass,
                schema_model=schema,
            )

            # FORCE Metadata update
            # We must wrap this in retry logic because dlt might be releasing the lock
            def _update_metadata():
                if self.engine:
                    with Session(self.engine) as session:
                        # Re-fetch or merge to ensure attached to session
                        # We modify the object first
                        new_meta = dict(artifact.meta)
                        new_meta["is_ingested"] = True
                        new_meta["dlt_table_name"] = resource_name
                        artifact.meta = new_meta

                        session.merge(artifact)
                        session.commit()

            # Execute with retry to handle lock contention from dlt teardown
            self._execute_with_retry(
                _update_metadata, operation_name="ingest_metadata_update"
            )

            # Persist metadata update to DB
            if self.engine:
                with Session(self.engine) as session:
                    session.merge(artifact)
                    session.commit()

            return info

        except Exception as e:
            raise e
        # Note: We don't need to explicitly reconnect self.engine.
        # SQLAlchemy will automatically reconnect the next time it's used.

    def get_artifact(self, key_or_id: Union[str, uuid.UUID]) -> Optional[Artifact]:
        """
        Retrieves an Artifact by its semantic key or UUID.

        This method provides a flexible way to locate artifacts, first checking
        the in-memory context of the current run, and then querying the database
        for persistent records.

        Parameters
        ----------
        key_or_id : Union[str, uuid.UUID]
            The artifact's 'key' (e.g., "households") or its unique UUID.
            When a string is provided, the most recently created artifact matching
            that key is returned.

        Returns
        -------
        Optional[Artifact]
            The found `Artifact` object, or `None` if no matching artifact is found.
        """
        # 1. Check In-Memory Context (Current Run)
        if self.current_consist:
            for art in reversed(self.current_consist.outputs):
                if art.key == key_or_id or art.id == key_or_id:
                    return art
            for art in self.current_consist.inputs:
                if art.key == key_or_id or art.id == key_or_id:
                    return art

        # 2. Check Database with Retry
        if self.engine:

            def _query():
                with Session(self.engine) as session:
                    if isinstance(key_or_id, uuid.UUID):
                        return session.get(Artifact, key_or_id)
                    else:
                        statement = (
                            select(Artifact)
                            .where(Artifact.key == key_or_id)
                            .order_by(Artifact.created_at.desc())
                            .limit(1)
                        )
                        return session.exec(statement).first()

            try:
                return self._execute_with_retry(_query, operation_name="get_artifact")
            except Exception:
                return None

        return None

    def get_artifact_by_uri(self, uri: str) -> Optional[Artifact]:
        """
        Find an artifact by its URI.

        Useful for checking if a specific file has been logged,
        or for retrieving artifact metadata by path.

        Parameters
        ----------
        uri : str
            The portable URI to search for (e.g., "inputs://households.csv").

        Returns
        -------
        Optional[Artifact]
            The found `Artifact` object, or `None` if no matching artifact is found.
        """
        # 1. Check In-Memory Context (Current Run)
        if self.current_consist:
            for art in self.current_consist.inputs + self.current_consist.outputs:
                if art.uri == uri:
                    return art

        # 2. Check Database
        if self.engine:

            def _query():
                with Session(self.engine) as session:
                    statement = (
                        select(Artifact)
                        .where(Artifact.uri == uri)
                        .order_by(Artifact.created_at.desc())
                        .limit(1)
                    )
                    return session.exec(statement).first()

            try:
                return self._execute_with_retry(
                    _query, operation_name="get_artifact_by_uri"
                )
            except Exception:
                return None

        return None

    def create_view(self, view_name: str, concept_key: str) -> Any:
        """
        Creates a hybrid SQL view that consolidates data from both materialized tables
        in DuckDB and raw file-based artifacts (e.g., Parquet, CSV).

        This view is a core component of Consist's **"View Factory"** and **"Hybrid Views"**
        strategy, allowing transparent querying of data regardless of its underlying
        storage mechanism (hot/cold data).

        Parameters
        ----------
        view_name : str
            The desired name for the generated SQL view. This will be the name by which
            you query the combined data.
        concept_key : str
            The semantic key (e.g., "households", "transactions")
            that identifies the logical concept this view represents.
            The view will union all artifacts with this key.

        Returns
        -------
        Any
            The result of the `ViewFactory.create_hybrid_view` method, typically a
            representation of the created view or confirmation of its creation.
        """
        factory = ViewFactory(self)
        return factory.create_hybrid_view(view_name, concept_key)

    def resolve_uri(self, uri: str) -> str:
        """
        Converts a portable Consist URI back into an absolute file system path.

        This is the inverse operation of `_virtualize_path`, crucial for **"Path Resolution & Mounts"**.
        It uses the configured `mounts` and the `run_dir` to reconstruct the local
        absolute path to an artifact, making runs portable across different environments.

        Parameters
        ----------
        uri : str
            The portable URI (e.g., "inputs://file.csv", "./output/data.parquet")
            to resolve.

        Returns
        -------
        str
            The absolute file system path corresponding to the given URI.
            If the URI cannot be fully resolved (e.g., scheme not mounted),
            it returns the most resolved path or the original URI after
            attempting to make it absolute.
        """
        path_str = uri
        # 1. Check schemes (mounts)
        if "://" in uri:
            scheme, rel_path = uri.split("://", 1)
            if scheme in self.mounts:
                path_str = str(Path(self.mounts[scheme]) / rel_path)
            elif scheme == "file":
                path_str = rel_path
        elif uri.startswith("./"):
            path_str = str(self.run_dir / uri[2:])

        # Ensure we always return absolute, resolved paths
        return str(Path(path_str).resolve())

    def _virtualize_path(self, path: str) -> str:
        """
        Converts an absolute file system path into a portable Consist URI.

        This method is a key part of **"Path Resolution & Mounts"**, attempting to
        replace parts of the absolute path with scheme-based URIs (e.g., "inputs://")
        if a matching mount is configured, or makes it relative to the `run_dir`
        if possible. This ensures artifact paths stored in the provenance are portable
        across different execution environments, making Consist runs reproducible.

        Parameters
        ----------
        path : str
            The absolute file system path to virtualize.

        Returns
        -------
        str
            A portable URI representation of the path (e.g., "inputs://file.csv",
            "./output/data.parquet"). If no virtualization is possible, the original
            absolute path is returned.
        """
        abs_path = str(Path(path).resolve())

        # Check mounts longest-match first
        for name, root in sorted(
            self.mounts.items(), key=lambda x: len(x[1]), reverse=True
        ):
            root_abs = str(Path(root).resolve())
            if abs_path.startswith(root_abs):
                rel = os.path.relpath(abs_path, root_abs)
                return f"{name}://{rel}"

        # Fallback: Relative to run_dir if possible, else strict absolute
        try:
            rel = os.path.relpath(abs_path, self.run_dir)
            if not rel.startswith(".."):
                return f"./{rel}"
        except ValueError:
            pass

        return abs_path

    def _flush_json(self) -> None:
        """
        Writes the current `ConsistRecord` (in-memory state of the run) to a `consist.json` file.

        This operation is performed using an atomic write pattern (write to a temporary file,
        then rename) to ensure data integrity and prevent corruption, even if the process
        is interrupted.

        This is a critical part of the **"Dual-Write Safety"** strategy: the JSON file
        is always flushed first, ensuring that a human-readable record of the run exists
        even if the subsequent database synchronization fails.
        """
        if not self.current_consist:
            return
        json_str = self.current_consist.model_dump_json(indent=2)

        target = self.run_dir / "consist.json"
        # Write temp then rename to ensure no corrupted files
        tmp = target.with_suffix(".tmp")
        with open(tmp, "w") as f:
            f.write(json_str)
        tmp.rename(target)

    def _sync_run_to_db(self, run: Run) -> None:
        """
        Synchronizes the state of a `Run` object to the DuckDB database.

        This method either updates an existing run record or inserts a new one,
        ensuring that the database reflects the most current status and metadata
        of the run. It uses a **"Clone and Push"** strategy to avoid binding the
        live run object to the session, which helps prevent potential ORM issues.

        As part of the **"Dual-Write Safety"** mechanism, this method
        tolerates database failures (logs a warning instead of crashing),
        prioritizing the completion of the user's run.

        Parameters
        ----------
        run : Run
            The `Run` object whose state needs to be synchronized with the database.
        """
        if not self.engine:
            return

        def _do_sync():
            with Session(self.engine) as session:
                # "Clone and Push"
                db_run = session.get(Run, run.id)
                if db_run:
                    # Update existing DB row explicitly
                    db_run.status = run.status
                    db_run.description = run.description
                    db_run.updated_at = run.updated_at
                    db_run.meta = run.meta
                    db_run.config_hash = run.config_hash
                    db_run.git_hash = run.git_hash
                    db_run.input_hash = run.input_hash
                    db_run.signature = run.signature
                    db_run.parent_run_id = run.parent_run_id
                    db_run.tags = run.tags
                    db_run.started_at = run.started_at
                    db_run.ended_at = run.ended_at
                    session.add(db_run)
                else:
                    # Insert new: Create a fresh copy for the DB
                    # This ensures the original 'run' variable stays pure/detached
                    run_data = run.model_dump()
                    new_run = Run(**run_data)
                    session.add(new_run)

                session.commit()

        try:
            self._execute_with_retry(_do_sync, operation_name="sync_run")
        except Exception as e:
            # Dual-Write Safety: If DB fails after retries, log warning but don't crash run
            logging.warning(
                f"[Consist Warning] Database sync failed for run {run.id}: {e}"
            )

    def _sync_artifact_to_db(self, artifact: Artifact, direction: str) -> None:
        """
        Synchronizes an `Artifact` object and its `RunArtifactLink` to the DuckDB database.

        This method merges the artifact (either creating it or updating an existing one)
        into the database and creates a `RunArtifactLink` entry. This link explicitly
        associates the artifact with the current run and its role (input or output).

        As part of the **"Dual-Write Safety"** mechanism, this method
        tolerates database failures (logs a warning instead of crashing),
        prioritizing the completion of the user's run.

        Parameters
        ----------
        artifact : Artifact
            The `Artifact` object to synchronize.
        direction : str
            The direction of the artifact relative to the current run
            ("input" or "output").
        """
        if not self.engine or not self.current_consist:
            return

        def _do_sync():
            with Session(self.engine) as session:
                # Merge artifact (create or update)
                db_artifact = session.merge(artifact)

                # Create Link
                link = RunArtifactLink(
                    run_id=self.current_consist.run.id,
                    artifact_id=db_artifact.id,
                    direction=direction,
                )
                session.merge(link)
                session.commit()

        try:
            self._execute_with_retry(_do_sync, operation_name="sync_artifact")
        except Exception as e:
            logging.warning(f"[Consist Warning] Artifact sync failed: {e}")

    def get_run(self, run_id: str) -> Optional[Run]:
        """
        Retrieves a single Run by its ID from the database.

        Args:
            run_id (str): The unique identifier of the run to retrieve.

        Returns:
            Optional[Run]: The found Run object, or None if not found.
        """
        if not self.engine:
            return None

        def _query():
            with Session(self.engine) as session:
                return session.get(Run, run_id)

        try:
            return self._execute_with_retry(_query, operation_name="get_run")
        except Exception:
            return None

    def get_artifacts_for_run(
        self, run_id: str
    ) -> List[Tuple[Artifact, str]]:
        """
        Retrieves all artifacts associated with a given run, indicating their direction.

        Args:
            run_id (str): The ID of the run to retrieve artifacts for.

        Returns:
            List[Tuple[Artifact, str]]: A list of tuples, where each tuple contains
                                        an Artifact object and its direction ("input" or "output").
        """
        if not self.engine:
            return []

        def _query() -> List[Tuple[Artifact, str]]:
            with Session(self.engine) as session:
                statement = (
                    select(Artifact, RunArtifactLink.direction)
                    .join(
                        RunArtifactLink, Artifact.id == RunArtifactLink.artifact_id
                    )
                    .where(RunArtifactLink.run_id == run_id)
                )
                results = session.exec(statement).all()
                return results

        try:
            return self._execute_with_retry(
                _query, operation_name="get_artifacts_for_run"
            )
        except Exception as e:
            logging.warning(
                f"[Consist Warning] Failed to get artifacts for run {run_id}: {e}"
            )
            return []

    def get_artifact_lineage(
        self, artifact_key_or_id: Union[str, uuid.UUID]
    ) -> Optional[Dict[str, Any]]:
        """
        Recursively builds a lineage tree for a given artifact.

        The tree shows the run that produced the artifact, and recursively, the
        input artifacts for that run and the runs that produced them.

        Args:
            artifact_key_or_id (Union[str, uuid.UUID]): The key or ID of the artifact to trace.

        Returns:
            A dictionary representing the lineage tree, or None if the artifact is not found.
            Example structure:
            {
                "artifact": <Artifact>,
                "producing_run": {
                    "run": <Run>,
                    "inputs": [
                        {"artifact": <Artifact>, "producing_run": ...},
                        ...
                    ]
                }
            }
        """
        if not self.engine:
            return None

        # 1. Get the starting artifact
        start_artifact = self.get_artifact(key_or_id=artifact_key_or_id)
        if not start_artifact:
            return None

        # 2. Recursive function to build the tree
        def _trace(
            artifact: Artifact, visited_runs: set
        ) -> Dict[str, Any]:
            lineage_node: Dict[str, Any] = {"artifact": artifact, "producing_run": None}

            # Find the run that produced this artifact
            producing_run_id = artifact.run_id
            if not producing_run_id or producing_run_id in visited_runs:
                return lineage_node

            visited_runs.add(producing_run_id)
            producing_run = self.get_run(producing_run_id)
            if not producing_run:
                return lineage_node

            # Get the inputs for that run
            input_artifacts_with_direction = self.get_artifacts_for_run(
                producing_run.id
            )

            run_node: Dict[str, Any] = {"run": producing_run, "inputs": []}
            for input_artifact, direction in input_artifacts_with_direction:
                if direction == "input":
                    # Recurse on each input
                    run_node["inputs"].append(
                        _trace(input_artifact, visited_runs.copy())
                    )

            lineage_node["producing_run"] = run_node
            return lineage_node

        return _trace(start_artifact, set())

    def log_meta(self, **kwargs: Any) -> None:
        """
        Updates the metadata for the current run.

        This method allows logging additional key-value pairs to the `meta` field
        of the currently active `Run` object. This is particularly useful for
        recording runtime metrics (e.g., accuracy, loss, F1-score), tags, or
        any other arbitrary information generated during the run's execution.
        The metadata is immediately flushed to both the JSON log and the database.

        Parameters
        ----------
        **kwargs : Any
            Arbitrary key-value pairs to merge into the `meta` dictionary of
            the current run. Existing keys will be updated, and new keys will be added.
        """
        if not self.current_consist:
            logging.warning("[Consist] Cannot log_meta: No active run.")
            return

        # 1. Update In-Memory
        # Ensure 'meta' is a dict (SQLModel sometimes initializes defaults oddly depending on version)
        if self.current_consist.run.meta is None:
            self.current_consist.run.meta = {}

        self.current_consist.run.meta.update(kwargs)

        # 2. Persist
        self._flush_json()
        # We also sync to DB immediately so external monitors can see progress/tags
        self._sync_run_to_db(self.current_consist.run)