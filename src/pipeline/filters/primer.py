# downloader.py

# Use to download files from GRIN.

import logging
import json
from pathlib import Path
from clients import GrinClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logger: logging.Logger = logging.getLogger(__name__)

"""
If the first bucket in the pipeline is empty, the pipeline must be primed. This class
uses the GRIN API to fetch all the barcodes in the GRIN converted resource and creates
tokens for all of them.

NB: this is a very naive implementation, just for testing the pipeline.
"""
class Primer:
    def __init__(self, to_bucket, processing_bucket):
        self.grin_client:GrinClient = GrinClient()
        self.to_bucket = Path(to_bucket)
        self.processing_bucket = processing_bucket
        

    def replentish_tokens(self):
        converted_books = self.grin_client.converted_books
        barcodes = [i.split('.')[0] for i in converted_books]
        for barcode in barcodes:
            token_info:dict = {
                "barcode" : barcode,
                "processing_bucket" : self.processing_bucket
            }
            token_filepath:Path = self.to_bucket / Path(f"{barcode}.json")
            with open(token_filepath, 'w') as f:
                json.dump(token_info, fp=f, indent=2)
