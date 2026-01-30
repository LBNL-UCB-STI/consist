from consist.integrations.containers.api import _build_container_manifest


def test_manifest_omits_env_values():
    manifest = _build_container_manifest(
        image="img:v1",
        image_digest="sha:123",
        command=["run"],
        environment={"SECRET": "do-not-store"},
        working_dir=None,
        backend_type="docker",
        volumes={"/host": "/container"},
    )

    assert "environment" not in manifest
    assert "environment_hash" in manifest
    assert "do-not-store" not in str(manifest)
