import os
import logging
from pathlib import Path
from pipeline.plumbing import Pipe, Filter, Token


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger: logging.Logger = logging.getLogger(__name__)



class Cleaner(Filter):
    """The Cleaner simply moves the tarball from the processing bucket
    to the done bucket."""
    def __init__(self, pipe: Pipe, finished_bucket:str="/var/tmp/done") -> None:
        super().__init__(pipe)
        self.finished_bucket = Path(finished_bucket)


    def source_file(self, token: Token) -> Path:
        input_path = Path(token.content["processing_bucket"])
        input_filename: Path = Path(token.content["barcode"]).with_suffix(".tgz")
        return input_path / input_filename

    def destination_file(self, token: Token) -> Path:
        filename = Path(token.content["barcode"]).with_suffix(".tgz")
        destination_path = self.finished_bucket / filename
        return destination_path


    def validate_token(self, token) -> bool:
        status: bool = True
        if self.source_file(token).exists() is False:
            logging.error(f"file to clean does not exist: {self.source_file(token)}")
            self.log_to_token(
                token, "ERROR", f"file to clean does not exist: {self.source_file(token)}"
            )
            status = False

        if self.finished_bucket.is_dir() is False:
            logging.error(f"target directory does not exist: {self.finished_bucket}")
            self.log_to_token(
                token, "ERROR", f"target directory does not exist: {self.finished_bucket}"
            )
            status = False
            
        return status


    def process_token(self, token:Token) -> bool:
        successflg = False
        try:
            self.source_file(token).rename(self.destination_file(token))
            self.log_to_token(token, "INFO", "Object moved to done")
            successflg = True
        except PermissionError as e:
            self.log_to_token(token, "ERROR", f"Object not moved! {e}")
            successflg = False
        return successflg

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    cleaner:Cleaner = Cleaner(pipe, os.environ.get("FINISHED_BUCKET")
    logger.info("starting cleaner")
    cleaner.run_forever()
    
