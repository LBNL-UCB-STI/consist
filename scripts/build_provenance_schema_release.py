"""Build portable, checksummed release assets for the Consist LinkML module."""

from __future__ import annotations

import argparse
import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).parents[1]
DEFAULT_SCHEMA_DIR = PROJECT_ROOT / "src" / "consist" / "schemas"
SCHEMA_NAMES = ("provenance", "binding")


def _sha256(path: Path) -> str:
    """Return the SHA-256 digest of one release asset."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_checksums(output_dir: Path) -> Path:
    """Write checksums for every release asset except the checksum manifest."""
    checksum_path = output_dir / "SHA256SUMS"
    assets = sorted(
        path
        for path in output_dir.rglob("*")
        if path.is_file() and path != checksum_path
    )
    lines = [
        f"{_sha256(path)}  {path.relative_to(output_dir).as_posix()}" for path in assets
    ]
    checksum_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return checksum_path


def _format_utc_timestamp(value: datetime) -> str:
    """Return one timezone-aware timestamp in portable UTC ``Z`` notation."""
    if value.tzinfo is None:
        raise ValueError("release metadata timestamps must be timezone-aware")
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _normalize_merged_schema_metadata(
    serialized_schema: str,
    *,
    source_file_date: str,
    generation_date: str,
) -> str:
    """Replace LinkML's naïve merge metadata with timezone-aware UTC values."""
    metadata_values = {
        "source_file_date": source_file_date,
        "generation_date": generation_date,
    }
    normalized_lines: list[str] = []
    remaining = set(metadata_values)
    for line in serialized_schema.splitlines():
        for field_name, value in metadata_values.items():
            if line.startswith(f"{field_name}:"):
                normalized_lines.append(f"{field_name}: '{value}'")
                remaining.discard(field_name)
                break
        else:
            normalized_lines.append(line)

    normalized_lines.extend(
        f"{field_name}: '{metadata_values[field_name]}'"
        for field_name in sorted(remaining)
    )
    return "\n".join(normalized_lines) + "\n"


def build_release_assets(schema_dir: Path, output_dir: Path) -> list[Path]:
    """Build modular sources, merged closures, and reference documentation.

    LinkML imports are intentionally local to this developer-facing release
    builder. Importing ``consist`` for normal tracking, caching, or recovery
    never loads LinkML.
    """
    from linkml.generators.docgen import DocGenerator
    from linkml.generators.yamlgen import YAMLGenerator

    schema_paths = [schema_dir / f"{name}.yaml" for name in SCHEMA_NAMES]
    missing_schema_paths = [path for path in schema_paths if not path.is_file()]
    if missing_schema_paths:
        missing = ", ".join(str(path) for path in missing_schema_paths)
        raise FileNotFoundError(f"Schema source does not exist: {missing}")

    if output_dir.exists():
        if not output_dir.is_dir():
            raise NotADirectoryError(f"Release output is not a directory: {output_dir}")
        if any(output_dir.iterdir()):
            raise ValueError(
                "Release output must be absent or empty to prevent stale generated assets: "
                f"{output_dir}"
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    source_paths: list[Path] = []
    merged_paths: list[Path] = []
    reference_dirs: list[Path] = []
    generation_date = _format_utc_timestamp(datetime.now(timezone.utc))
    for schema_path in schema_paths:
        schema_name = schema_path.stem
        source_path = output_dir / schema_path.name
        merged_path = output_dir / f"{schema_name}.merged.yaml"
        reference_dir = output_dir / "reference" / schema_name

        shutil.copyfile(schema_path, source_path)
        source_file_date = _format_utc_timestamp(
            datetime.fromtimestamp(schema_path.stat().st_mtime, timezone.utc)
        )
        merged_path.write_text(
            _normalize_merged_schema_metadata(
                YAMLGenerator(
                    schema_path, mergeimports=True, metadata=False
                ).serialize(),
                source_file_date=source_file_date,
                generation_date=generation_date,
            ),
            encoding="utf-8",
        )
        DocGenerator(
            schema_path,
            mergeimports=True,
            metadata=False,
            preserve_names=True,
        ).serialize(directory=str(reference_dir))
        source_paths.append(source_path)
        merged_paths.append(merged_path)
        reference_dirs.append(reference_dir)
    checksum_path = _write_checksums(output_dir)

    return [*source_paths, *merged_paths, *reference_dirs, checksum_path]


def parse_args() -> argparse.Namespace:
    """Parse the release-builder command line."""
    parser = argparse.ArgumentParser(
        description="Build checksummed Consist LinkML provenance-schema release assets."
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=DEFAULT_SCHEMA_DIR,
        help="Directory containing the modular Consist LinkML source schemas.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Directory that will receive source, merged, documentation, and checksums.",
    )
    return parser.parse_args()


def main() -> None:
    """Build release assets and report their output directory."""
    args = parse_args()
    build_release_assets(args.schema_dir.resolve(), args.output.resolve())
    print(args.output.resolve())


if __name__ == "__main__":  # pragma: no cover - command-line entry point
    main()
