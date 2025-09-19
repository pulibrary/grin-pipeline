from pathlib import Path
import pytest
from pipeline.plumbing import Pipeline
from pipeline.config_loader import load_config


def test_add_bucket(shared_datadir):
    test_config_file = shared_datadir / "test_config.yaml"
    config: dict = load_config(str(test_config_file))

    pipeline = Pipeline(config)
    test_location = Path("/tmp")
    pipeline.add_bucket("tmp", test_location)

    assert pipeline.bucket("tmp") == test_location


def test_getting_bucket(shared_datadir):
    test_config_file = shared_datadir / "test_config.yaml"
    config: dict = load_config(str(test_config_file))

    pipeline = Pipeline(config)
    start_path = Path("/var/tmp/grin/pipeline/start")
    assert pipeline.bucket("start") == start_path
    with pytest.raises(ValueError):
        pipeline.bucket("does_not_exist")


def test_getting_pipe(shared_datadir):
    test_config_file = shared_datadir / "test_config.yaml"
    config: dict = load_config(str(test_config_file))

    pipeline = Pipeline(config)
    start_path = Path("/var/tmp/grin/pipeline/start")
    requested_path = Path("/var/tmp/grin/pipeline/requested")
    test_pipe = pipeline.pipe("start", "requested")

    assert test_pipe.input == start_path
    assert test_pipe.output == requested_path
