import logging
from pathlib import Path
from clients import GrinClient
from pipeline.plumbing import Pipe, Filter, Token


class Requester(Filter):
    def __init__(self, pipe:Pipe) -> None:
        super().__init__(pipe)
        self.grin: GrinClient = GrinClient()


    def validate_token(self, token: Token) -> bool:
        return True


    def process_token(self, token: Token) -> bool:
        successflg = False
        barcode = token.content["barcode"]
        response = self.grin.convert_book(barcode)
        if response is not None:
            status = response[barcode]
            self.log_to_token(token, "INFO", status)
            successflg=True
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
