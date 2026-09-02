from importlib.metadata import version as package_version
import subprocess
import sys

import consist


def test_package_exposes_installed_distribution_version() -> None:
    """Consumers can record the installed Consist version from the public API."""
    assert consist.__version__ == package_version("consist")


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
