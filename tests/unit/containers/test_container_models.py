from consist.integrations.containers.models import ContainerDefinition


def test_config_hashing_sensitivity():
    """
    Verifies that `ContainerDefinition.to_hashable_config()` produces different
    dictionary outputs when any significant field of the container definition changes.

    This is fundamental for Consist's caching mechanism, as it ensures that even
    a minor alteration in the container's execution parameters will result in a
    unique configuration hash, thus invalidating the cache and triggering a re-run.

    What happens:
    1. A `base` container configuration is defined.
    2. `config1` is generated from this `base`.
    3. Separate configurations (`config2`, `config3`, `config4`) are generated
       by making a single, specific change to the `base` configuration:
       - `config2`: Changes the `command`.
       - `config3`: Changes an `environment` variable.
       - `config4`: Changes the `image_digest`.

    What's checked:
    - `config1` is not equal to `config2` (change in command invalidates hash).
    - `config1` is not equal to `config3` (change in environment invalidates hash).
    - `config1` is not equal to `config4` (change in image digest invalidates hash).
    """
    base_def = ContainerDefinition(
        image="img:v1",
        image_digest="sha:123",
        command=["run"],
        environment={"A": "1"},
        backend="docker",
        extra_args={},
    )
    config1 = base_def.to_hashable_config()
    assert "environment" not in config1
    assert "environment_hash" in config1

    # Case 1: Change Command
    base_cmd = base_def.model_copy(update={"command": ["run", "--fast"]})
    config2 = base_cmd.to_hashable_config()
    assert config1 != config2

    # Case 2: Change Env
    base_env = base_def.model_copy(update={"environment": {"A": "2"}})
    config3 = base_env.to_hashable_config()
    assert config1 != config3

    # Case 3: Change Image Digest (Simulate image update)
    base_img = base_def.model_copy(update={"image_digest": "sha:456"})
    config4 = base_img.to_hashable_config()
    assert config1 != config4
