"""Build portable, checksummed release assets for the Consist LinkML module."""

from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).parents[1]
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "src" / "consist" / "schemas" / "provenance.yaml"


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


def build_release_assets(schema_path: Path, output_dir: Path) -> list[Path]:
    """Build modular source, merged closure, and generated reference documentation.

    LinkML imports are intentionally local to this developer-facing release
    builder. Importing ``consist`` for normal tracking, caching, or recovery
    never loads LinkML.
    """
    from linkml.generators.docgen import DocGenerator
    from linkml.generators.yamlgen import YAMLGenerator

    if not schema_path.is_file():
        raise FileNotFoundError(f"Schema source does not exist: {schema_path}")

    if output_dir.exists():
        if not output_dir.is_dir():
            raise NotADirectoryError(f"Release output is not a directory: {output_dir}")
        if any(output_dir.iterdir()):
            raise ValueError(
                "Release output must be absent or empty to prevent stale generated assets: "
                f"{output_dir}"
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = output_dir / "provenance.yaml"
    merged_path = output_dir / "provenance.merged.yaml"
    reference_dir = output_dir / "reference"

    shutil.copyfile(schema_path, source_path)
    merged_path.write_text(
        YAMLGenerator(schema_path, mergeimports=True, metadata=False).serialize(),
        encoding="utf-8",
    )
    DocGenerator(
        schema_path,
        mergeimports=False,
        metadata=False,
        preserve_names=True,
    ).serialize(directory=str(reference_dir))
    checksum_path = _write_checksums(output_dir)

    return [source_path, merged_path, reference_dir, checksum_path]


def parse_args() -> argparse.Namespace:
    """Parse the release-builder command line."""
    parser = argparse.ArgumentParser(
        description="Build checksummed Consist LinkML provenance-schema release assets."
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help="Path to the modular provenance LinkML source schema.",
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
    build_release_assets(args.schema.resolve(), args.output.resolve())
    print(args.output.resolve())


if __name__ == "__main__":  # pragma: no cover - command-line entry point
    main()
