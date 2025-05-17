# downloader.py

# Use to download files from GRIN.

import logging
from pathlib import Path
from pipeline.plumbing import Pipe, Filter, Token
from clients import GrinClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logger: logging.Logger = logging.getLogger(__name__)


class Downloader(Filter):
    def __init__(self, pipe:Pipe):
        super().__init__(pipe)
        self.grin_client:GrinClient = GrinClient()

    def validate_token(self, token:Token) -> bool:
        return True

    def process_token(self, token:Token) -> bool:
        completed:bool = False
        barcode = token.content['barcode']
        dest = str(Path(token.content['processing_bucket']))
        self.grin_client.download_book(barcode, dest)
        completed = True
        return completed
    
            
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    pipe:Pipe = Pipe(Path(args.input), Path(args.output))

    downloader:Downloader = Downloader(pipe)
    logger.info("starting downloader")
    downloader.run_forever()
