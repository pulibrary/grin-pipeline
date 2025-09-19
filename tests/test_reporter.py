from pathlib import Path
from clients import GrinClient, S3Client
from reporters.reporter import Reporter, ObjectStoreReporter


def test_has_GrinClient():
    grin_client: GrinClient = GrinClient()
    reporter = Reporter(grin_client)

    assert reporter.grin_client == grin_client


def test_has_aws_client():
    grin_client: GrinClient = GrinClient()
    s3_client: S3Client = S3Client(local_cache=Path("/dev/null"))
    reporter = ObjectStoreReporter(grin_client, s3_client)

    assert reporter.grin_client is grin_client
    assert reporter.s3_client is s3_client


def test_can_count_stored_objects():
    grin_client: GrinClient = GrinClient()
    s3_client: S3Client = S3Client(local_cache=Path("/dev/null"))
    reporter = ObjectStoreReporter(grin_client, s3_client)

    count = reporter.number_of_objects_in_store()

    assert count > 0
