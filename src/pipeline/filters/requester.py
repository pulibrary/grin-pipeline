import logging
from pathlib import Path
from clients import GrinClient
from pipeline.plumbing import Pipe, Filter, Token


class Requester(Filter):
    """
    Pipeline filter that initiates conversion requests for books.

    The Requester filter takes tokens containing book barcodes and submits
    them to the GRIN service for conversion processing. This is typically
    the first processing stage in the pipeline.

    Attributes:
        grin (GrinClient): Client for communicating with the GRIN conversion service
    """

    def __init__(self, pipe: Pipe) -> None:
        super().__init__(pipe)
        self.grin: GrinClient = GrinClient()

    def validate_token(self, token: Token) -> bool:
        """Validate that the token contains required fields for conversion request.

        Args:
            token (Token): Token to validate

        Returns:
            bool: Always returns True as all tokens are valid for conversion requests
        """
        return True

    def process_token(self, token: Token) -> bool:
        """Submit a book conversion request to the GRIN service.

        Extracts the barcode from the token and submits it to the GRIN client
        for conversion processing. Logs the response status to the token.

        Args:
            token (Token): Token containing the book barcode to convert

        Returns:
            bool: True if the conversion request was successful, False otherwise
        """
        successflg = False
        barcode = token.content["barcode"]
        response = self.grin.convert_book(barcode)
        if response is not None:
            status = response[barcode]
            self.log_to_token(token, "INFO", status)
            successflg = True
        else:
            logging.error(f"submission of barcode for conversion failed: {barcode}")
            successflg = False

        return successflg


if __name__ == "__main__":
    import argparse

    logger: logging.Logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    requester: Requester = Requester(pipe)
    logger.info("starting requester")
    requester.run_forever()
