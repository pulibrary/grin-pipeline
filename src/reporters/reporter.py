import os
from collections import namedtuple
from csv import DictWriter
from io import StringIO
from pathlib import Path

from clients import GrinClient, S3Client
from pipeline.book_ledger import Book, BookLedger
from pipeline.config_loader import load_config
from pipeline.plumbing import Pipeline
from pipeline.secretary import Secretary
from pipeline.token_bag import TokenBag

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


class ObjectStoreReporter(Reporter):
    def __init__(self):
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


class SecretaryReporter(Reporter):
    def __init__(self, config: dict) -> None:
        super().__init__()
        self.secretary = Secretary(
            TokenBag(Path(config.get("global", {}).get("token_bag", None))),
            BookLedger(Path(config.get("global", {}).get("ledger_file", None))),
        )
