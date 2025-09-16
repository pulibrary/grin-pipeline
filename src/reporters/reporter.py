import os
import logging
from pipeline.config_loader import load_config
from clients import GrinClient
from clients import S3Client

config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
config: dict = load_config(config_path)

# Set up logging
log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

logging.basicConfig(level=log_level)


class Reporter:
    """Family of classes that gather information about aspects of GRIN
    and the GRIN pipeline: what barcodes have already been processed;
    what barcodes are in process, converted, available; what barcodes
    are available but have not yet been processed; etc."""

    def __init__(self, grin_client:GrinClient):
        self.grin_client = grin_client

        
class ObjectStoreReporter(Reporter):
    def __init__(self, grin_client:GrinClient, s3_client:S3Client):
        super().__init__(grin_client)
        self.s3_client = s3_client


    def number_of_objects_in_store(self):
        paginator = self.s3_client.client.get_paginator("list_objects_v2")

        page_iterator = paginator.paginate(Bucket = self.s3_client.bucket_name)

        count = 0
        for page in page_iterator:
            contents = page.get("Contents", [])
            count += len(contents)

        return count
        
