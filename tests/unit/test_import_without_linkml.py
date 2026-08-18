import subprocess
import sys


def test_importing_consist_does_not_import_linkml() -> None:
    """Normal Consist runtime use remains independent of LinkML tooling."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import consist; "
            "print('linkml' in sys.modules); "
            "print('linkml_runtime' in sys.modules)",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.splitlines() == ["False", "False"]
