from consist.integrations.containers.models import ContainerDefinition


def test_config_hashing_sensitivity():
    """
    Verify that changing any field results in a different config dictionary.
    This is the foundation of Cache Invalidation.
    """
    base = dict(
        image="img:v1",
        image_digest="sha:123",
        command=["run"],
        environment={"A": "1"},
        backend="docker",
        extra_args={},
    )

    config1 = ContainerDefinition(**base).to_hashable_config()

    # Case 1: Change Command
    base_cmd = base.copy()
    base_cmd["command"] = ["run", "--fast"]
    config2 = ContainerDefinition(**base_cmd).to_hashable_config()
    assert config1 != config2

    # Case 2: Change Env
    base_env = base.copy()
    base_env["environment"] = {"A": "2"}
    config3 = ContainerDefinition(**base_env).to_hashable_config()
    assert config1 != config3

    # Case 3: Change Image Digest (Simulate image update)
    base_img = base.copy()
    base_img["image_digest"] = "sha:456"
    config4 = ContainerDefinition(**base_img).to_hashable_config()
    assert config1 != config4
