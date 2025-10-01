from pathlib import Path

from clients import GrinClient, S3Client
from reporters.reporter import ObjectStoreReporter


def test_can_retrieve_stored_objects():
    reporter = ObjectStoreReporter()

    count = len(reporter.objects_in_store())

    assert count > 0
