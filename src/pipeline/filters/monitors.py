from pathlib import Path
import os
import logging
from pipeline.config_loader import load_config
from pipeline.plumbing import Pipeline, Token, load_token, dump_token
from clients import GrinClient


logger: logging.Logger = logging.getLogger(__name__)


class Monitor:
    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline

    def run(self):
        pass

    def dry_run(self):
        pass


class RequestMonitor(Monitor):
    """Monitors the Requested bucket, examining
    each token to see if its book has been converted
    by GRIN yet.  If so, it moves the token to the
    Converted bucket."""

    def __init__(self, pipeline: Pipeline) -> None:
        super().__init__(pipeline)
        self.client = GrinClient()
        self.pipe = self.pipeline.pipe("requested", "converted")


    @property
    def converted_barcodes(self) -> list[str] | None:
        converted_barcodes: list[str] | None = []
        grin_converted_books = self.client.converted_books
        if grin_converted_books is not None:
            converted_barcodes = [
                rec["barcode"] for rec in grin_converted_books
            ]
        return converted_barcodes


    @property
    def in_process_barcodes(self) -> list[str] | None:
        in_process_barcodes: list[str] | None = []
        grin_in_process_books = self.client.in_process_books
        if grin_in_process_books is not None:
                in_process_barcodes = [
                    rec["barcode"] for rec in grin_in_process_books
                ]
        return in_process_barcodes


    def is_in_process(self, token: Token) -> bool:
        return token.get_prop("barcode") in self.in_process_barcodes


    def is_converted(self, token: Token) -> bool:
        return token.get_prop("barcode") in self.converted_barcodes


    def report(self) -> dict[str, list[Token]]:
        report = {}
        report['pending'] = []
        report['converted'] = []
        for f in self.pipe.input.glob("*.json"):
            token = load_token(f)
            if self.is_in_process(token):
                report['pending'].append(token)
            if self.is_converted(token):
                report['converted'].append(token)
        return report
        

    def dry_run(self):
        print("barcode\tin_process\tconverted")
        for f in self.pipe.input.glob("*.json"):
            tok = load_token(f)
            barcode = tok.get_prop("barcode")
            requested_p = self.is_in_process(tok)
            converted_p = self.is_converted(tok)
            print(f"{barcode}\t{requested_p}\t{converted_p}")


    def run(self):
        flg = True
        while flg is True:
            token = self.pipe.take_token()
            if token is None:
                flg = False
                break

            if self.is_in_process(token):
                logger.info(f"conversion pending: {token}")
                token.write_log("conversion pending", "INFO", "Monitor")
                self.pipe.put_token_back()

            elif self.is_converted(token):
                logger.info(f"conversion complete: {token}")
                token.write_log("conversion complete", "INFO", "Monitor")
                self.pipe.put_token()

            else:
                raise ValueError(f"{token} is neither pending nor converted")
            



if __name__ == "__main__":
    config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
    config: dict = load_config(config_path)

    monitor = Monitor(Pipeline(config))
