"""
Command Execution and Provenance Tracking.

This module provides low-overhead, process-level provenance instrumentation
for Consist. It allows executing external CLI commands and recording explicit
input/output boundaries, environment digests, execution timing, and process
exit status directly into DuckDB without refactoring code into Python callables.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shlex
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from pydantic import ConfigDict, Field as PydanticField
from sqlmodel import select

from consist.core.run_ordering import recent_run_order_by
from consist.models.artifact import Artifact
from consist.models.run import Run, RunResult

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

logger = logging.getLogger(__name__)

_FILE_HASH_CHUNK_SIZE = 8 * 1024 * 1024
_DEFAULT_ENV_ALLOWLIST = frozenset(
    {
        "PATH",
        "LANG",
        "LC_ALL",
        "PYTHONPATH",
        "PYTHONHOME",
        "VIRTUAL_ENV",
        "CONDA_PREFIX",
        "JAVA_HOME",
        "R_HOME",
    }
)


def _compute_file_sha256(path: Path) -> str:
    """Compute SHA-256 hex digest for a file."""
    sha256 = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(_FILE_HASH_CHUNK_SIZE)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


class CommandExecutionResult(RunResult):
    """
    Structured result of a command execution tracked by Consist.
    """

    run: Optional[Run] = None
    inputs: Dict[str, Artifact] = PydanticField(default_factory=dict)
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    stdout_path: Optional[Path] = None
    stderr_path: Optional[Path] = None
    duration: float = 0.0
    command: List[str] = PydanticField(default_factory=list)
    plan: Optional[Dict[str, Any]] = None
    dry_run: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def is_success(self) -> bool:
        """Return True if dry run or if process exited 0 and run completed."""
        if self.dry_run:
            return True
        return self.exit_code == 0 and (
            self.run is None or self.run.status == "completed"
        )

    @property
    def error(self) -> Optional[str]:
        """Return the error message recorded on the run, if any."""
        if self.run and self.run.meta:
            cmd_meta = self.run.meta.get("command")
            if isinstance(cmd_meta, dict) and cmd_meta.get("error"):
                return str(cmd_meta["error"])
            if self.run.meta.get("error"):
                return str(self.run.meta["error"])
        return None


class CommandExecutionError(RuntimeError):
    """
    Raised when command execution fails and check=True is specified.
    """

    def __init__(self, message: str, result: CommandExecutionResult) -> None:
        super().__init__(message)
        self.result = result
        self.exit_code = result.exit_code


def compute_env_digest(
    custom_env: Optional[Mapping[str, str]] = None,
    allowlist: Optional[Iterable[str]] = None,
) -> Tuple[str, Dict[str, str]]:
    """
    Compute a deterministic SHA-256 digest of relevant environment variables.
    """
    keys_to_check = set(allowlist or _DEFAULT_ENV_ALLOWLIST)
    if custom_env:
        keys_to_check.update(custom_env.keys())

    filtered_env: Dict[str, str] = {}
    for key in sorted(keys_to_check):
        if custom_env and key in custom_env:
            filtered_env[key] = str(custom_env[key])
        elif key in os.environ:
            filtered_env[key] = os.environ[key]

    serialized = json.dumps(filtered_env, sort_keys=True)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return digest, filtered_env


def discover_suggested_inputs(
    argv: Sequence[str],
    base_dir: Path,
    declared_outputs: Set[Path],
) -> List[Tuple[str, Path]]:
    """
    Discover regular files mentioned in argv (skipping argv[0]) that are not declared outputs.
    """
    if len(argv) <= 1:
        return []

    suggested: List[Tuple[str, Path]] = []
    seen_paths: Set[Path] = set()

    for token in argv[1:]:
        if not token or token.startswith("-") or "://" in token:
            continue
        # Skip purely numeric arguments
        clean_num = token.replace(".", "", 1)
        if clean_num.isdigit():
            continue

        try:
            cand = Path(token)
            resolved = (
                (base_dir / cand).resolve()
                if not cand.is_absolute()
                else cand.resolve()
            )
            if (
                resolved.is_file()
                and resolved not in declared_outputs
                and resolved not in seen_paths
            ):
                seen_paths.add(resolved)
                try:
                    rel_name = str(resolved.relative_to(base_dir))
                except ValueError:
                    rel_name = resolved.name
                suggested.append((rel_name, resolved))
        except (ValueError, OSError):
            continue

    return suggested


def compare_with_baseline(
    tracker: "Tracker",
    model_name: str,
    current_run_id: str,
    current_command: List[str],
    current_inputs: Dict[str, Artifact],
    current_outputs: Dict[str, Artifact],
    current_duration: float,
) -> Optional[Dict[str, Any]]:
    """
    Compare the current run against the most recent completed run of the same model name.
    """
    if not tracker.db:
        return None

    try:
        with tracker.db.session_scope() as session:
            stmt = (
                select(Run)
                .where(Run.model_name == model_name)
                .where(Run.status == "completed")
                .where(Run.id != current_run_id)
                .order_by(*recent_run_order_by())
                .limit(1)
            )
            baseline_run = session.exec(stmt).first()
            if not baseline_run:
                return None

            baseline_id = baseline_run.id
            baseline_meta = baseline_run.meta or {}
            baseline_cmd_meta = (
                baseline_meta.get("command")
                if isinstance(baseline_meta.get("command"), dict)
                else {}
            )
            baseline_command = baseline_cmd_meta.get("argv", [])
            baseline_duration = baseline_cmd_meta.get("duration")
            if (
                baseline_duration is None
                and baseline_run.ended_at
                and baseline_run.started_at
            ):
                baseline_duration = (
                    baseline_run.ended_at - baseline_run.started_at
                ).total_seconds()
    except Exception as exc:
        logger.debug("Error querying baseline run: %s", exc)
        return None

    try:
        baseline_artifacts = tracker.get_artifacts_for_run(baseline_id)
    except Exception as exc:
        logger.debug("Error querying baseline artifacts: %s", exc)
        return None

    command_changed = baseline_command != current_command

    curr_inp_keys = set(current_inputs.keys())
    base_inp_keys = set(baseline_artifacts.inputs.keys())
    added_inputs = sorted(curr_inp_keys - base_inp_keys)
    removed_inputs = sorted(base_inp_keys - curr_inp_keys)
    modified_inputs: List[str] = []
    unchanged_inputs: List[str] = []
    for k in sorted(curr_inp_keys.intersection(base_inp_keys)):
        if current_inputs[k].hash != baseline_artifacts.inputs[k].hash:
            modified_inputs.append(k)
        else:
            unchanged_inputs.append(k)

    curr_out_keys = set(current_outputs.keys())
    base_out_keys = set(baseline_artifacts.outputs.keys())
    added_outputs = sorted(curr_out_keys - base_out_keys)
    removed_outputs = sorted(base_out_keys - curr_out_keys)
    modified_outputs: List[str] = []
    unchanged_outputs: List[str] = []
    for k in sorted(curr_out_keys.intersection(base_out_keys)):
        if current_outputs[k].hash != baseline_artifacts.outputs[k].hash:
            modified_outputs.append(k)
        else:
            unchanged_outputs.append(k)

    duration_delta: Optional[float] = None
    if baseline_duration is not None and current_duration is not None:
        duration_delta = round(current_duration - float(baseline_duration), 3)

    return {
        "baseline_run_id": baseline_id,
        "command_changed": command_changed,
        "baseline_command": baseline_command,
        "inputs": {
            "added": added_inputs,
            "removed": removed_inputs,
            "modified": modified_inputs,
            "unchanged": unchanged_inputs,
        },
        "outputs": {
            "added": added_outputs,
            "removed": removed_outputs,
            "modified": modified_outputs,
            "unchanged": unchanged_outputs,
        },
        "baseline_duration": baseline_duration,
        "duration_delta": duration_delta,
    }


def _safe_stream_write(stream: Any, chunk: bytes) -> None:
    """
    Safely write byte chunk to a stream target without disrupting pipe draining.
    Handles raw binary streams (.buffer) and text streams (e.g. StringIO / wrappers).
    """
    if stream is None or not chunk:
        return
    try:
        buf = getattr(stream, "buffer", None)
        if buf is not None:
            buf.write(chunk)
            buf.flush()
        else:
            text = chunk.decode("utf-8", errors="replace")
            stream.write(text)
            stream.flush()
    except Exception:
        # Never let stream display or pipe errors disrupt pipe draining
        pass


def execute_command(
    tracker: "Tracker",
    name: str,
    argv: Sequence[str] | str,
    *,
    inputs: Optional[Mapping[str, Path | str]] = None,
    outputs: Optional[Mapping[str, Path | str]] = None,
    outputs_dir: Optional[Path | str] = None,
    cwd: Optional[Path | str] = None,
    env: Optional[Mapping[str, str]] = None,
    dry_run: bool = False,
    shell: bool = False,
    accept_suggested: bool = False,
    stream_output: bool = True,
    log_dir: Optional[Path | str] = None,
    tags: Optional[List[str]] = None,
    description: Optional[str] = None,
    compare_baseline: bool = True,
    check: bool = False,
    run_id: Optional[str] = None,
    **kwargs: Any,
) -> CommandExecutionResult:
    """
    Execute an external command and record its provenance boundary in Consist.
    """
    invocation_cwd = Path.cwd().resolve()
    resolved_cwd = Path(cwd).resolve() if cwd is not None else invocation_cwd

    # Normalize argv
    if isinstance(argv, str):
        if shell:
            cmd_to_spawn: Union[str, List[str]] = argv
            cmd_list = shlex.split(argv)
        else:
            cmd_list = shlex.split(argv)
            cmd_to_spawn = cmd_list
    else:
        cmd_list = [str(x) for x in argv]
        if shell:
            cmd_to_spawn = " ".join(shlex.quote(x) for x in cmd_list)
        else:
            cmd_to_spawn = cmd_list

    # Compute environment digest
    env_digest, filtered_env = compute_env_digest(env)

    # Resolve declared inputs
    resolved_inputs: Dict[str, Path] = {}
    if inputs:
        for k, v in inputs.items():
            cand = Path(v)
            if cand.is_absolute():
                p = cand
            elif (invocation_cwd / cand).exists():
                p = (invocation_cwd / cand).resolve()
            elif (resolved_cwd / cand).exists():
                p = (resolved_cwd / cand).resolve()
            else:
                p = (invocation_cwd / cand).resolve()

            if not p.exists():
                raise FileNotFoundError(
                    f"Declared input '{k}' at '{v}' does not exist on disk."
                )
            resolved_inputs[str(k)] = p

    # Resolve declared outputs
    resolved_outputs: Dict[str, Path] = {}
    if outputs:
        for k, v in outputs.items():
            cand = Path(v)
            if cand.is_absolute():
                p = cand
            elif resolved_cwd != invocation_cwd:
                p = (resolved_cwd / cand).resolve()
            else:
                p = (invocation_cwd / cand).resolve()
            resolved_outputs[str(k)] = p

    # Check input/output disjointness
    input_paths_set = set(resolved_inputs.values())
    output_paths_set = set(resolved_outputs.values())
    overlap = input_paths_set.intersection(output_paths_set)
    if overlap:
        overlap_str = ", ".join(str(p) for p in sorted(overlap))
        raise ValueError(
            f"Path cannot be declared as both an input and an output: {overlap_str}"
        )

    # Resolve outputs_dir
    resolved_outputs_dir: Optional[Path] = None
    if outputs_dir:
        cand_out_dir = Path(outputs_dir)
        if cand_out_dir.is_absolute():
            resolved_outputs_dir = cand_out_dir
        elif cwd is not None:
            resolved_outputs_dir = (resolved_cwd / cand_out_dir).resolve()
        elif (invocation_cwd / cand_out_dir).exists():
            resolved_outputs_dir = (invocation_cwd / cand_out_dir).resolve()
        else:
            resolved_outputs_dir = (resolved_cwd / cand_out_dir).resolve()

    # Discover suggested inputs from argv
    suggested = discover_suggested_inputs(
        cmd_list,
        base_dir=resolved_cwd,
        declared_outputs=output_paths_set,
    )

    if accept_suggested:
        for s_key, s_path in suggested:
            if s_path not in input_paths_set and s_path not in output_paths_set:
                final_key = s_key
                counter = 1
                while final_key in resolved_inputs:
                    final_key = f"{s_key}_{counter}"
                    counter += 1
                resolved_inputs[final_key] = s_path
                input_paths_set.add(s_path)

    # Dry-run handling
    if dry_run:
        input_fingerprints: Dict[str, Dict[str, Any]] = {}
        for k, p in resolved_inputs.items():
            if p.is_file():
                input_fingerprints[k] = {
                    "path": str(p),
                    "hash": _compute_file_sha256(p),
                }
            elif p.is_dir():
                input_fingerprints[k] = {
                    "path": str(p),
                    "type": "directory",
                }

        output_inspection: Dict[str, Dict[str, Any]] = {}
        for k, p in resolved_outputs.items():
            output_inspection[k] = {
                "path": str(p),
                "exists": p.exists(),
            }

        unaccepted_suggested = [
            {"key": k, "path": str(p)} for k, p in suggested if p not in input_paths_set
        ]

        plan: Dict[str, Any] = {
            "name": name,
            "command": cmd_list,
            "shell": shell,
            "cwd": str(resolved_cwd),
            "env_digest": env_digest,
            "inputs": input_fingerprints,
            "outputs": output_inspection,
            "outputs_dir": (
                str(resolved_outputs_dir) if resolved_outputs_dir else None
            ),
            "suggested_inputs": unaccepted_suggested,
        }

        return CommandExecutionResult(
            run=None,
            inputs={},
            outputs={},
            exit_code=0,
            stdout="",
            stderr="",
            duration=0.0,
            command=cmd_list,
            plan=plan,
            dry_run=True,
        )

    # Snapshot outputs_dir pre-execution
    outputs_dir_snapshot: Dict[Path, Tuple[int, int]] = {}
    if resolved_outputs_dir and resolved_outputs_dir.exists():
        for f in resolved_outputs_dir.rglob("*"):
            if f.is_file():
                try:
                    st = f.stat()
                    outputs_dir_snapshot[f.resolve()] = (st.st_mtime_ns, st.st_size)
                except OSError:
                    pass

    # Begin run in tracker
    effective_run_id = (
        run_id
        if run_id is not None
        else f"{name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{os.urandom(4).hex()}"
    )

    run = tracker.begin_run(
        run_id=effective_run_id,
        model=name,
        tags=tags,
        description=description,
        **kwargs,
    )

    # Log declared inputs
    logged_inputs: Dict[str, Artifact] = {}
    for k, p in resolved_inputs.items():
        art = tracker.log_artifact(p, key=k, direction="input")
        logged_inputs[k] = art

    # Determine log sidecar paths
    if log_dir is not None:
        effective_log_dir = Path(log_dir).resolve()
    else:
        effective_log_dir = tracker.run_artifact_dir(run)
    effective_log_dir.mkdir(parents=True, exist_ok=True)
    stdout_file_path = effective_log_dir / "stdout.log"
    stderr_file_path = effective_log_dir / "stderr.log"

    # Populate initial run metadata
    if run.meta is None:
        run.meta = {}
    run.meta["command"] = {
        "argv": cmd_list,
        "shell": shell,
        "cwd": str(resolved_cwd),
        "env_digest": env_digest,
        "outputs_dir": (str(resolved_outputs_dir) if resolved_outputs_dir else None),
        "stdout_path": str(stdout_file_path),
        "stderr_path": str(stderr_file_path),
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    # Setup execution environment
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    start_time = time.monotonic()
    stdout_chunks: List[bytes] = []
    stderr_chunks: List[bytes] = []

    out_file = stdout_file_path.open("wb")
    err_file = stderr_file_path.open("wb")

    def _stream_reader(
        pipe: Any,
        sink_chunks: List[bytes],
        sink_file: Any,
        target_stream: Optional[Any],
    ) -> None:
        if pipe is None:
            return
        try:
            while True:
                chunk = pipe.read(4096)
                if not chunk:
                    break
                sink_chunks.append(chunk)
                try:
                    sink_file.write(chunk)
                    sink_file.flush()
                except Exception:
                    pass
                if target_stream is not None:
                    _safe_stream_write(target_stream, chunk)
        except Exception:
            pass
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    proc = None
    try:
        proc = subprocess.Popen(
            cmd_to_spawn,
            shell=shell,
            cwd=str(resolved_cwd),
            env=merged_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        out_target = sys.stdout if stream_output else None
        err_target = sys.stderr if stream_output else None

        out_thread = threading.Thread(
            target=_stream_reader,
            args=(
                proc.stdout,
                stdout_chunks,
                out_file,
                out_target,
            ),
            daemon=True,
        )
        err_thread = threading.Thread(
            target=_stream_reader,
            args=(
                proc.stderr,
                stderr_chunks,
                err_file,
                err_target,
            ),
            daemon=True,
        )

        out_thread.start()
        err_thread.start()

        proc.wait()
        out_thread.join(timeout=3.0)
        err_thread.join(timeout=3.0)

    except FileNotFoundError as exc:
        duration = round(time.monotonic() - start_time, 4)
        exit_code = 127
        err_msg = f"Command not found: {cmd_list[0]}"
        run.meta["command"]["exit_code"] = exit_code
        run.meta["command"]["duration"] = duration
        run.meta["command"]["error"] = err_msg
        tracker.end_run(status="failed", error=exc)
        res = CommandExecutionResult(
            run=run,
            inputs=logged_inputs,
            outputs={},
            exit_code=exit_code,
            stdout="",
            stderr=str(exc),
            stdout_path=stdout_file_path,
            stderr_path=stderr_file_path,
            duration=duration,
            command=cmd_list,
            dry_run=False,
        )
        if check:
            raise CommandExecutionError(err_msg, result=res) from exc
        return res

    except PermissionError as exc:
        duration = round(time.monotonic() - start_time, 4)
        exit_code = 126
        err_msg = f"Permission denied executing command: {cmd_list[0]}"
        run.meta["command"]["exit_code"] = exit_code
        run.meta["command"]["duration"] = duration
        run.meta["command"]["error"] = err_msg
        tracker.end_run(status="failed", error=exc)
        res = CommandExecutionResult(
            run=run,
            inputs=logged_inputs,
            outputs={},
            exit_code=exit_code,
            stdout="",
            stderr=str(exc),
            stdout_path=stdout_file_path,
            stderr_path=stderr_file_path,
            duration=duration,
            command=cmd_list,
            dry_run=False,
        )
        if check:
            raise CommandExecutionError(err_msg, result=res) from exc
        return res

    except OSError as exc:
        duration = round(time.monotonic() - start_time, 4)
        exit_code = 127
        err_msg = f"Failed to execute command '{cmd_list[0]}': {exc}"
        run.meta["command"]["exit_code"] = exit_code
        run.meta["command"]["duration"] = duration
        run.meta["command"]["error"] = err_msg
        tracker.end_run(status="failed", error=exc)
        res = CommandExecutionResult(
            run=run,
            inputs=logged_inputs,
            outputs={},
            exit_code=exit_code,
            stdout="",
            stderr=str(exc),
            stdout_path=stdout_file_path,
            stderr_path=stderr_file_path,
            duration=duration,
            command=cmd_list,
            dry_run=False,
        )
        if check:
            raise CommandExecutionError(err_msg, result=res) from exc
        return res

    except KeyboardInterrupt:
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        run.meta["command"]["interrupted"] = True
        run.meta["command"]["exit_code"] = 130
        run.meta["command"]["error"] = "Process interrupted by user (SIGINT)"
        tracker.end_run(
            status="failed",
            error=KeyboardInterrupt("Process interrupted by user (SIGINT)"),
        )
        raise

    except Exception as exc:
        duration = round(time.monotonic() - start_time, 4)
        exit_code = 1
        err_msg = f"Unexpected execution error: {exc}"
        run.meta["command"]["exit_code"] = exit_code
        run.meta["command"]["duration"] = duration
        run.meta["command"]["error"] = err_msg
        tracker.end_run(status="failed", error=exc)
        res = CommandExecutionResult(
            run=run,
            inputs=logged_inputs,
            outputs={},
            exit_code=exit_code,
            stdout="",
            stderr=str(exc),
            stdout_path=stdout_file_path,
            stderr_path=stderr_file_path,
            duration=duration,
            command=cmd_list,
            dry_run=False,
        )
        if check:
            raise CommandExecutionError(err_msg, result=res) from exc
        return res

    finally:
        out_file.close()
        err_file.close()

    duration = round(time.monotonic() - start_time, 4)
    raw_code = proc.returncode if proc else 1
    if raw_code < 0:
        exit_code = 128 + abs(raw_code)
        run.meta["command"]["signal"] = abs(raw_code)
    else:
        exit_code = raw_code

    stdout_str = b"".join(stdout_chunks).decode("utf-8", errors="replace")
    stderr_str = b"".join(stderr_chunks).decode("utf-8", errors="replace")

    run.meta["command"]["exit_code"] = exit_code
    run.meta["command"]["duration"] = duration

    logged_outputs: Dict[str, Artifact] = {}

    # Check process exit code
    if exit_code != 0:
        if raw_code < 0:
            err_msg = (
                f"Command terminated by signal {abs(raw_code)} (exit code {exit_code})"
            )
        else:
            err_msg = f"Command failed with exit code {exit_code}"
        run.meta["command"]["error"] = err_msg
        tracker.end_run(status="failed", error=RuntimeError(err_msg))

        res = CommandExecutionResult(
            run=run,
            inputs=logged_inputs,
            outputs={},
            exit_code=exit_code,
            stdout=stdout_str,
            stderr=stderr_str,
            stdout_path=stdout_file_path,
            stderr_path=stderr_file_path,
            duration=duration,
            command=cmd_list,
            dry_run=False,
        )
        if check:
            raise CommandExecutionError(err_msg, result=res)
        return res

    # Verify declared outputs
    missing_outputs: List[Tuple[str, Any]] = []
    for k, p in resolved_outputs.items():
        if not p.exists():
            missing_outputs.append((k, outputs[k] if (outputs and k in outputs) else p))

    if missing_outputs:
        first_key, first_path = missing_outputs[0]
        err_msg = f"Declared output '{first_key}' at '{first_path}' was not created."
        run.meta["command"]["error"] = err_msg
        tracker.end_run(status="failed", error=RuntimeError(err_msg))

        res = CommandExecutionResult(
            run=run,
            inputs=logged_inputs,
            outputs={},
            exit_code=1,
            stdout=stdout_str,
            stderr=stderr_str,
            stdout_path=stdout_file_path,
            stderr_path=stderr_file_path,
            duration=duration,
            command=cmd_list,
            dry_run=False,
        )
        if check:
            raise CommandExecutionError(err_msg, result=res)
        return res

    # Log verified declared outputs
    for k, p in resolved_outputs.items():
        art = tracker.log_artifact(p, key=k, direction="output")
        logged_outputs[k] = art

    # Scan outputs_dir for new/modified files
    if resolved_outputs_dir and resolved_outputs_dir.exists():
        declared_output_paths = {p.resolve() for p in resolved_outputs.values()}
        for f in resolved_outputs_dir.rglob("*"):
            if f.is_file():
                try:
                    f_res = f.resolve()
                    st = f.stat()
                    curr_tuple = (st.st_mtime_ns, st.st_size)
                    if (
                        f_res not in outputs_dir_snapshot
                        or curr_tuple != outputs_dir_snapshot[f_res]
                    ):
                        if f_res not in declared_output_paths:
                            rel_key = str(f.relative_to(resolved_outputs_dir))
                            final_key = rel_key
                            counter = 1
                            while final_key in logged_outputs:
                                final_key = f"{rel_key}_{counter}"
                                counter += 1
                            art = tracker.log_artifact(
                                f, key=final_key, direction="output"
                            )
                            logged_outputs[final_key] = art
                except OSError:
                    pass

    # Baseline comparison
    if compare_baseline and tracker.db:
        try:
            baseline_diff = compare_with_baseline(
                tracker=tracker,
                model_name=name,
                current_run_id=run.id,
                current_command=cmd_list,
                current_inputs=logged_inputs,
                current_outputs=logged_outputs,
                current_duration=duration,
            )
            if baseline_diff:
                run.meta["command"]["baseline_comparison"] = baseline_diff
        except Exception as exc:
            logger.debug("Failed to compute baseline comparison: %s", exc)

    # Complete the run
    tracker.end_run(status="completed")

    res = CommandExecutionResult(
        run=run,
        inputs=logged_inputs,
        outputs=logged_outputs,
        exit_code=0,
        stdout=stdout_str,
        stderr=stderr_str,
        stdout_path=stdout_file_path,
        stderr_path=stderr_file_path,
        duration=duration,
        command=cmd_list,
        dry_run=False,
    )
    return res
