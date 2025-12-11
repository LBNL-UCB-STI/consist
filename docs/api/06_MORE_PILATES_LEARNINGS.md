This integration process has been an excellent stress test because it exposed the friction between **"Logical Workflows"** (what the orchestrator thinks is happening) and **"Physical Execution"** (what actually happens in containers).

Based on the friction points we hit (nesting crashes, manual input extraction, status signaling), here are 4 specific features that would have made this integration much smoother and would be valuable for Consist v2:

### 1. Nested Run Support (Sub-Runs)
**The Problem:** You hit `RuntimeError: A run is already active` multiple times. You had to choose between tracking the *Python wrapper* (via decorator) or the *Container execution* (via `run_container`), but you couldn't easily track both hierarchy.

**The Solution:** `nested=True`
Consist should allow runs to start within runs.
```python
# Parent: The Logical Step
with tracker.start_run("urbansim_logic", nested=True):
    # ... preprocessing ...
    
    # Child: The Physical Execution
    # Consist automatically links this as a child of "urbansim_logic"
    consist.run_container("urbansim_container", ...) 
```
*   **Benefit:** You wouldn't have to delete your `@provenance_logging` decorators. The container run would simply appear as a child node in the lineage graph, preserving the full context.

### 2. "Attach" Mode for Containers
**The Problem:** Sometimes you don't want the container to be a separate run at all. You want the container execution to just be an *event* inside the current Python run.

**The Solution:** `attach_to_current=True`
```python
# Python starts the run
with tracker.start_run("atlas_model"):
    # The container executes, but logs its inputs/outputs/logs 
    # directly to "atlas_model", effectively merging the two contexts.
    consist.run_container(..., attach_to_current=True)
```
*   **Benefit:** This solves the "missing output" problem where the wrapper didn't know what the container produced. The container's outputs would automatically be registered to the wrapper's run ID.

### 3. Smart Input Extraction (The `inputs_from` Arg)
**The Problem:** You wrote this boilerplate 3 times:
```python
input_paths = [r.file_path for r in store.all_records()]
run_container(..., input_artifacts=input_paths)
```

**The Solution:** Protocol-based extraction.
Consist should accept arbitrary objects as inputs, provided you tell it how to read them or if they follow a protocol (like `__iter__` returning paths).
```python
# Consist inspects 'store'. 
# If it finds .file_path attributes or looks like a list of paths, it uses them.
consist.run_container(..., inputs=store) 
```
*   **Benefit:** Drastically reduces glue code in Adapters.

### 4. Explicit "Ghost Mode" Check
**The Problem:** PILATES has its own retry logic and `should_run` logic. It was awkward to check if Consist *would* have a cache hit without actually starting the run context.

**The Solution:** `tracker.check_cache(signature)`
```python
# Before starting expensive setup or retries...
if tracker.check_cache(config=..., inputs=...):
    logger.info("Consist has this cached. Skipping retry loop.")
    # Hydrate results immediately without entering the run context
    return tracker.hydrate(...)
```
*   **Benefit:** Allows the Orchestrator (PILATES) to optimize its own control flow based on Consist's state, rather than blindly entering a `with start_run():` block and hoping for a cache hit.

---

**Summary for your Stress Test:**
You have successfully implemented the **"Delegation Pattern"** (Python delegates strictly to Container). This is a solid, production-ready pattern.

Good luck with the run! Given the depth of these fixes, the likelihood of a clean run is high.