# downloader.py

# Use to download files from GRIN.

import logging
from pathlib import Path
from pipeline.plumbing import Pipe, Filter, Token
from clients import GrinClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger: logging.Logger = logging.getLogger(__name__)


class Downloader(Filter):
    """
    Pipeline filter that downloads converted files from the GRIN service.

    The Downloader filter retrieves converted book files from GRIN after
    conversion requests have been processed. It downloads files to the
    processing bucket specified in the token.
    """
    def __init__(self, pipe: Pipe):
        super().__init__(pipe)

    def validate_token(self, token: Token) -> bool:
        """Validate that the token has required fields for downloading.

        Args:
            token (Token): Token to validate

        Returns:
            bool: Always returns True as all tokens are valid for downloading
        """
        return True

    def process_token(self, token: Token) -> bool:
        """Download the converted book file from GRIN.

        Uses the GrinClient to download the book file to the processing bucket
        directory specified in the token.

        Args:
            token (Token): Token containing barcode and processing bucket info

        Returns:
            bool: True if download completed successfully
        """
        completed: bool = False
        barcode = token.content["barcode"]
        dest = str(Path(token.content["processing_bucket"]))
        grin_client = GrinClient()
        grin_client.download_book(barcode, dest)
        completed = True
        return completed


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    downloader: Downloader = Downloader(pipe)
    logger.info("starting downloader")
    downloader.run_forever()
