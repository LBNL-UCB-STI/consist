# Troubleshooting Guide

This guide covers common errors, their root causes, and solutions.

---

## Cache & Provenance Issues

### "Relation leak warnings"

**Symptom:** Warning: `Consist has N active DuckDB relations...`

**Root Cause:** Relations returned by `consist.load(...)` (tabular artifacts) keep a
DuckDB connection open until you close them.

**Solution:**

- Prefer `consist.load_df(...)` if you only need a pandas DataFrame.
- Use `consist.load_relation(...)` as a context manager to ensure connections are closed.
- If you're intentionally holding many Relations, increase the warning threshold:
  `CONSIST_RELATION_WARN_THRESHOLD=500`.

### "Old DBs no longer load after the Relation-first refactor"

**Symptom:** Errors when reading artifacts or querying the DB after upgrading.

**Root Cause:** The artifact schema changed:
- `Artifact.uri` → `Artifact.container_uri`
- `Artifact.table_path` added (nullable) for container formats (HDF5 tables)
- `Artifact.array_path` added (nullable) for array formats
- `meta["table_path"]` is no longer used

**Solution:**

Reset your Consist database(s) and re-run workflows:

```bash
rm ./provenance.duckdb
rm ./test_db.duckdb
```

Then update any code that referenced `artifact.uri` or `artifact.meta["table_path"]`:

```python
# Before
artifact.uri
artifact.meta.get("table_path")

# After
artifact.container_uri
artifact.table_path
```

---

### "Cache hit but output files are missing"

**Symptom:** `cache_hit=True` but `artifact.path` doesn't exist on disk.

**Root Cause:** Consist returned a cache hit but didn't materialize the files to disk.

Why this happens: Consist defaults to metadata-only cache hits to keep cache checks fast and avoid duplicating large files. You explicitly opt in to file copying via hydration/materialization when you need bytes on disk.

**Solution:**

Use cache hydration to copy files:

```python
result = consist.run(
    fn=my_function,
    inputs={...},
    cache_hydration="outputs-all",  # Copy all cached outputs
    ...
)
```

Or materialize manually:

```python
result = consist.run(...)
if result.cache_hit:
    from consist.core.materialize import materialize_artifacts
    artifacts_to_load = [(artifact, artifact.path) for artifact in result.outputs.values()]
    materialize_artifacts(tracker, artifacts_to_load)
```

---

### "Same inputs/config but cache not found"

**Symptom:** Code hasn't changed, inputs haven't changed, but run re-executes instead of hitting cache.

**Root Cause:** Signature mismatch. Something in the cache key changed.

**Solution:**

Debug the signature:

```python
import json
from consist.core.identity import IdentityManager

identity = tracker.identity
code_hash = identity.code_hash()
config_hash = identity.compute_json_hash({"param": value})
input_hash = identity.compute_file_checksum(Path("input.csv"))

print(f"Code: {code_hash}")
print(f"Config: {config_hash}")
print(f"Inputs: {input_hash}")

# Check if these match a prior run
prior_runs = tracker.find_runs()
for run in prior_runs:
    print(f"Run {run.id}: signature={run.signature}")
```

**Common causes:**
- **Code changed:** Check git status, function definitions
- **Config changed:** Check parameter types (0 vs 0.0, "0" vs 0)
- **Input file changed:** Check file modification time, content hash
- **Python version changed:** Code hash includes Python version (can invalidate cache)
- **Dependencies changed:** Installed package versions can affect behavior

---

### "How do I clear/reset cache?"

**Solution:**

Delete the database file:

```bash
rm ./provenance.duckdb
```

This clears all run history and cache. Next run will re-execute everything.

To keep history but force re-execution:

```python
tracker.begin_run(..., cache_mode="overwrite")
result = consist.run(...)
tracker.end_run()
```

---

### "Database locked" error

**Symptom:** `database is locked` or similar error when running multiple Consist processes.

**Root Cause:** DuckDB locks the database during writes. Concurrent write attempts fail.

**Solution:**

1. **Run sequentially** (recommended):
   ```bash
   python workflow1.py
   python workflow2.py
   ```

2. **Use separate databases per process:**
   ```python
   tracker = Tracker(
       run_dir="./runs",
       db_path=f"./provenance_{process_id}.duckdb",  # Unique per process
   )
   ```

3. **Increase lock timeout:**
   ```python
   import duckdb
   conn = duckdb.connect("provenance.duckdb", timeout=30)
   ```

---

## Mount & Path Issues

### "Mount not resolving" (Container integration)

**Symptom:** Container runs but `/inputs` is empty or doesn't exist.

**Root Cause:** Volume mount paths don't exist or are incorrect.

**Solution:**

1. **Check paths exist on host:**
   ```python
   from pathlib import Path
   for host_path in volumes.keys():
       assert Path(host_path).exists(), f"Missing: {host_path}"
   ```

2. **Use absolute paths:**
   ```python
   # DON'T:
   volumes={"./data": "/inputs"}

   # DO:
   volumes={str(Path("./data").resolve()): "/inputs"}
   ```

3. **Check permissions:**
   ```bash
   ls -la ./data
   # Must be readable by your user (and by Docker if using docker-in-docker)
   ```

4. **Debug mount:**
   ```bash
   docker run -it -v ./data:/inputs my-image ls -la /inputs
   ```

---

### "URI resolution failed"

**Symptom:** Error like `Cannot resolve URI: outputs://key/file.csv`

**Root Cause:** URI scheme not recognized or mount not registered.

**Solution:**

Use absolute paths instead of URI schemes for file operations:

```python
# DON'T:
artifact_uri = "outputs://key/result.csv"
df = pd.read_csv(artifact_uri)  # Fails

# DO:
artifact = consist.log_artifact(result_path, key="key")
df = pd.read_csv(artifact.path)  # Use .path property
```

Or resolve URI explicitly:

```python
resolved_path = tracker.resolve_uri("outputs://key/result.csv")
df = pd.read_csv(resolved_path)
```

---

### "Working directory changed between runs"

**Symptom:** File paths work in first run but fail in second run (re-run from different directory).

**Root Cause:** Relative paths depend on current working directory.

**Solution:**

Use absolute paths everywhere:

```python
# DON'T:
output_file = "results.csv"  # Relative to cwd

# DO:
output_file = Path(tracker.run_dir) / "results.csv"  # Absolute
```

Or use artifact URIs:

```python
consist.log_artifact(result, key="output")
# Later, access via:
artifact = tracker.get_artifacts_for_run("run_id").outputs["output"]
print(artifact.path)  # Absolute path
```

---

## Data & Schema Issues

### "Schema mismatch during ingestion"

**Symptom:** Error like `Column 'age' expected int, got str`

**Root Cause:** DataFrame column type doesn't match schema definition.

**Solution:**

Convert DataFrame types before ingestion:

```python
from consist.models.artifact import MySchema

# Check types
print(df.dtypes)

# Convert if needed
df = df.astype({
    "age": "int64",
    "income": "float64",
    "name": "object",
})

consist.log_dataframe(df, key="data", schema=MySchema)
```

Or use Pandas casting:

```python
df["age"] = pd.to_numeric(df["age"], errors="coerce")  # Convert with fallback
```

---

### "Null in non-optional field"

**Symptom:** Warning like `Null value in non-optional field 'age'`

**Root Cause:** DataFrame has NaN/None in a field that schema requires non-null.

**Solution:**

1. **Drop nulls:**
   ```python
   df = df.dropna(subset=["age"])
   ```

2. **Fill nulls:**
   ```python
   df["age"] = df["age"].fillna(0)  # Default value
   ```

3. **Make field optional:**
   ```python
   from typing import Optional

   class MySchema(SQLModel, table=True):
       age: Optional[int]  # Can be None
   ```

---

### "Duplicate primary keys"

**Symptom:** Error like `Primary key violation: duplicate ID`

**Root Cause:** DataFrame has duplicate values in the primary key column.

**Solution:**

Deduplicate before ingestion:

```python
# Keep last occurrence (or "first")
df = df.drop_duplicates(subset=["id"], keep="last")

# Or remove all duplicates
df = df[~df.duplicated(subset=["id"], keep=False)]

consist.log_dataframe(df, key="data", schema=MySchema)
```

---

### "Can't query across runs"

**Symptom:** `tracker.views.MySchema` doesn't exist or returns empty results.

**Root Cause:** Schema not registered or data not ingested with schema.

**Solution:**

1. **Register schema on Tracker creation:**
   ```python
   tracker = Tracker(
       run_dir="./runs",
       db_path="./provenance.duckdb",
       schemas=[Person, Trip],  # Register here
   )
   ```

2. **Ingest with schema:**
   ```python
   consist.log_dataframe(df, key="persons", schema=Person)
   ```

3. **Verify schema exists:**
   ```python
   print(tracker.views.Person)  # Should not raise AttributeError
   ```

---

## Container Execution Issues

### "Container execution failed"

**Symptom:** Error: `RuntimeError: Container execution failed`

**Root Cause:** Container exited with non-zero code.

**Solution:**

1. **Test container manually:**
   ```bash
   docker run -it -v ./data:/inputs my-image python script.py
   ```

2. **Check logs:**
   ```bash
   docker logs <container_id>
   ```

3. **Add verbose output:**
   ```python
   result = run_container(
       ...
       environment={"DEBUG": "1"},  # Enable debug output in container
   )
   ```

4. **Verify input paths:**
   ```python
   from pathlib import Path
   for input_path in inputs:
       assert Path(input_path).exists(), f"Missing: {input_path}"
   ```

---

### "Output files not found after container"

**Symptom:** Warning: `Expected output not found: ./outputs/result.csv`

**Root Cause:** Container didn't create output at expected location.

**Solution:**

1. **Verify container creates outputs:**
   ```bash
   docker run -it -v ./outputs:/outputs my-image sh -c "ls -la /outputs && echo 'done'"
   ```

2. **Check output paths in container:**
   ```python
   run_container(
       ...
       command=["python", "script.py"],  # Ensure script creates output
   )
   ```

3. **Use correct host paths:**
   ```python
   output_dir = Path("./outputs").mkdir(parents=True, exist_ok=True)
   result = run_container(
       ...
       outputs=[str(output_dir / "result.csv")],
   )
   ```

---

### "Image pull failed"

**Symptom:** Error: `Error pulling image: authentication required`

**Root Cause:** Docker can't access the image registry.

**Solution:**

1. **Authenticate:**
   ```bash
   docker login
   ```

2. **Use public images:**
   ```python
   run_container(
       image="ubuntu:latest",  # Public image
       ...
   )
   ```

3. **Check image exists locally:**
   ```bash
   docker images | grep my-image
   ```

4. **Disable pull:**
   ```python
   run_container(
       ...
       pull_latest=False,  # Use local image if available
   )
   ```

---

### "Permission denied in container"

**Symptom:** `Permission denied` when container writes to mounted volume.

**Root Cause:** Container user doesn't have write permission on host mount.

**Solution:**

1. **Make directory writable:**
   ```bash
   chmod 777 ./outputs
   ```

2. **Run container as current user:**
   ```python
   # (Requires custom Dockerfile or user configuration)
   # docker run --user $(id -u):$(id -g) ...
   ```

3. **Create output directory with correct permissions:**
   ```python
   output_dir = Path("./outputs")
   output_dir.mkdir(parents=True, exist_ok=True, mode=0o777)
   ```

---

## Performance Issues

### "Runs are very slow"

**Symptom:** Each run takes much longer than expected.

**Root Cause:** Several possibilities:

1. **No cache hits:** Check if signature is changing unexpectedly.
2. **File I/O bottleneck:** Large artifact materialization.
3. **Database queries slow:** Too many cross-run queries.
4. **Container startup overhead:** Each container run adds 1-2 seconds.

**Solution:**

1. **Profile execution:**
   ```python
   import time
   start = time.time()
   result = consist.run(...)
   print(f"Elapsed: {time.time() - start}s")
   print(f"Cache hit: {result.cache_hit}")
   ```

2. **Avoid unnecessary materialization:**
   ```python
   # Don't materialize if you don't need it
   result = consist.run(..., cache_hydration="none")
   ```

3. **Use Parquet instead of CSV** (faster parsing):
   ```python
   df.to_parquet("output.parquet")  # Instead of .to_csv()
   ```

4. **Batch containers to reduce startup overhead:**
   ```python
   # DON'T:
   for i in range(100):
       run_container(...)  # 100 containers, 100 startups

   # DO:
   run_container(
       command=["python", "process_batch.py", "--n", "100"],
       ...
   )
   ```

---

### "Database is huge and slow"

**Symptom:** Queries are slow, database file is large.

**Root Cause:** Too much data ingested or too many runs.

**Solution:**

1. **Vacuum database:**
   ```python
   with tracker.engine.begin() as conn:
       conn.exec_driver_sql("VACUUM")
   ```

2. **Archive old runs:**
   ```bash
   # Move old database
   mv provenance.duckdb provenance.backup.duckdb
   # Start fresh
   ```

3. **Use selective ingestion:**
   ```python
   # Don't ingest everything, just what you need
   consist.log_dataframe(df.head(1000), key="sample")  # Sample instead of all
   ```

---

## Debugging Tools

### Enable Logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("consist")
logger.setLevel(logging.DEBUG)
```

This prints detailed provenance tracking, signature computation, and cache decisions.

### Inspect Run Metadata

```python
run = tracker.get_run("run_id")
print(f"Signature: {run.signature}")
print(f"Code hash: {run.config_hash}")
print(f"Meta: {run.meta}")
```

### Inspect Database

```python
import duckdb

conn = duckdb.connect("provenance.duckdb")
print(conn.query("SELECT * FROM run LIMIT 5").df())
print(conn.query("SELECT * FROM artifact LIMIT 5").df())
```

### Check File Hashes

```python
from pathlib import Path

artifact = consist.log_artifact(Path("input.csv"))
print(f"Path: {artifact.path}")
print(f"Hash: {artifact.hash}")
print(f"Size: {artifact.path.stat().st_size}")
```

---

## Getting Help

If you hit an issue not covered here:

1. **Check the logs:**
   ```python
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Inspect database:**
   ```bash
   sqlite3 provenance.duckdb ".schema"
   ```

3. **File an issue** on GitHub with:
   - Error message and traceback
   - Minimal reproducible example
   - Output of `consist runs` (recent run history)
   - Output of logging (with DEBUG enabled)

---

## See Also

- [Container Integration](containers-guide.md#error-handling)
- [DLT Loader](dlt-loader-guide.md#common-errors)
- [Architecture](architecture.md) (for implementation details)
- [CLI Reference](cli-reference.md) (for debugging commands)
