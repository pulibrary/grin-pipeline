from pathlib import Path
import os
import logging
from pipeline.config_loader import load_config
from pipeline.plumbing import Pipeline, Token, load_token, dump_token
from clients import GrinClient


logger: logging.Logger = logging.getLogger(__name__)

class Monitor:
    def __init__(self, pipeline:Pipeline) -> None:
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
    
    def __init__(self, pipeline:Pipeline) -> None:
        super().__init__(pipeline)
        self.client = GrinClient()
        self.pipe = self.pipeline.pipe('requested', 'converted')
        self._converted_barcodes = None
        self._in_process_barcodes = None

    @property
    def converted(self):
        if self._converted_barcodes is None:
            grin_converted_books = self.client.converted_books
            if grin_converted_books is not None:
                self._converted_barcodes = [rec['barcode'] for rec in grin_converted_books]
        return self._converted_barcodes

    @property
    def in_process(self):
        if self._in_process_barcodes is None:
            grin_in_process_books = self.client.in_process_books
            if grin_in_process_books is not None:
                self._in_process_barcodes = [rec['barcode'] for rec in grin_in_process_books]
        return self._in_process_barcodes



    def is_in_process(self, token:Token) -> bool:
        if self.in_process:
            return token.get_prop('barcode') in self.in_process
        else:
            return False
    

    def is_converted(self, token:Token) -> bool:
        if self.converted:
            return token.get_prop('barcode') in self.converted
        else:
            return False
    
    def dry_run(self):
        print("barcode\tin_process\tconverted")
        for f in self.pipe.input.glob("*.json"):
            tok = load_token(f)
            barcode = tok.get_prop('barcode')
            requested_p = self.is_in_process(tok)
            converted_p = self.is_converted(tok)
            print(f"{barcode}\t{requested_p}\t{converted_p}")

    def run(self):
        tokens_to_process =  self.pipe.input.glob("*.json")
        for token_path in tokens_to_process:
            token: Token = load_token(token_path)
            


            requested_p = self.is_in_process(token)
            converted_p = self.is_converted(token)

            if requested_p:
                logger.info(f"conversion in process: {token}")
                token.write_log("conversion in process", "INFO", "Monitor")
                dump_token(token, self.pipe.input / Path(token.name).with_suffix('.json'))

            elif converted_p:
                logger.info(f"conversion complete: {token}")
                token.write_log("conversion complete", "INFO", "Monitor")
                dump_token(token, self.pipe.output / Path(token.name).with_suffix('.json'))
                token_path.unlink()
                



if __name__ == "__main__":
    config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
    config: dict = load_config(config_path)

    monitor = Monitor(Pipeline(config))
    
    
