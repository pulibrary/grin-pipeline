import logging
import os
from collections import namedtuple
from csv import DictWriter
from io import StringIO

from clients import GrinClient, S3Client
from pipeline.config_loader import load_config

config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
config: dict = load_config(config_path)

# Set up logging
log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

logging.basicConfig(level=log_level)

S3Rec = namedtuple(
    "S3Rec",
    [
        "Key",
        "LastModified",
        "ETag",
        "ChecksumAlgorithm",
        "ChecksumType",
        "Size",
        "StorageClass",
    ],
)


class Reporter:
    """Family of classes that gather information about aspects of GRIN
    and the GRIN pipeline: what barcodes have already been processed;
    what barcodes are in process, converted, available; what barcodes
    are available but have not yet been processed; etc."""

    def __init__(self) -> None:
        pass

    def report(self, **kwargs) -> dict:
        pass


class ReporterOld:
    """Family of classes that gather information about aspects of GRIN
    and the GRIN pipeline: what barcodes have already been processed;
    what barcodes are in process, converted, available; what barcodes
    are available but have not yet been processed; etc."""

    def __init__(self, grin_client: GrinClient):
        self.grin_client = grin_client

    def report(self, **kwargs) -> dict:
        pass


class ObjectStoreReporter(Reporter):
    def __init__(self, grin_client: GrinClient, s3_client: S3Client):
        super().__init__()
        self.s3_client = S3Client("/tmp")
        self.grin_client = GrinClient()

    def objects_in_store(self):
        paginator = self.s3_client.client.get_paginator("list_objects_v2")

        page_iterator = paginator.paginate(Bucket=self.s3_client.bucket_name)
        objects = []
        for page in page_iterator:
            contents = page.get("Contents", [])
            for object in contents:
                objects.append(S3Rec(**object))

        return objects

    def format_as_table_to_print(self, oblist):
        with StringIO() as out_buf:
            writer = DictWriter(out_buf, S3Rec._fields)
            writer.writeheader()
            for ob in oblist:
                writer.writerow(ob._asdict())
            csv_string = out_buf.getvalue()
        return csv_string

    def report(self, **kwargs) -> str:
        format: str = kwargs.get("format")
        match format:
            case "table":
                return self.format_as_table_to_print(self.objects_in_store())
            case "barcodes":
                return [ob.key for ob in self.objects_in_store()]
            case _:
                return ""
