import os
import sys
import logging
from pathlib import Path
from pipeline.plumbing import Pipe, Filter, Token


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger: logging.Logger = logging.getLogger(__name__)


class Cleaner(Filter):
    """
    Final cleanup filter that moves processed files to a finished directory.

    The Cleaner filter performs the final stage of processing by moving
    successfully processed tarball files from the processing bucket to
    a 'done' directory for archival.

    Attributes:
        finished_bucket (Path): Directory where completed files are stored
    """

    def __init__(self, pipe: Pipe, finished_bucket: str = "/var/tmp/done") -> None:
        super().__init__(pipe)
        self.finished_bucket = Path(finished_bucket)

    def source_file(self, token: Token) -> Path:
        """Get the path to the source file to be moved.

        Args:
            token (Token): Token containing barcode and processing bucket

        Returns:
            Path: Path to the .tgz file in the processing bucket
        """
        input_path = Path(token.content["processing_bucket"])
        input_filename: Path = Path(token.content["barcode"]).with_suffix(".tgz")
        return input_path / input_filename

    def destination_file(self, token: Token) -> Path:
        """Get the destination path for the file in the finished bucket.

        Args:
            token (Token): Token containing barcode

        Returns:
            Path: Path where the file will be moved in the finished bucket
        """
        filename = Path(token.content["barcode"]).with_suffix(".tgz")
        destination_path = self.finished_bucket / filename
        return destination_path

    def validate_token(self, token) -> bool:
        """Validate that source file and target directory exist.

        Args:
            token (Token): Token to validate

        Returns:
            bool: True if source file exists and target directory is valid
        """
        status: bool = True
        if self.source_file(token).exists() is False:
            logging.error(f"file to clean does not exist: {self.source_file(token)}")
            self.log_to_token(
                token,
                "ERROR",
                f"file to clean does not exist: {self.source_file(token)}",
            )
            status = False

        if self.finished_bucket.is_dir() is False:
            logging.error(f"target directory does not exist: {self.finished_bucket}")
            self.log_to_token(
                token,
                "ERROR",
                f"target directory does not exist: {self.finished_bucket}",
            )
            status = False

        return status

    def process_token(self, token: Token) -> bool:
        """Move the processed file to the finished directory.

        Args:
            token (Token): Token containing file paths

        Returns:
            bool: True if file was moved successfully, False otherwise
        """
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
    if "FINISHED_BUCKET" not in os.environ:
        print("Please set the FINISHED_BUCKET environment variable.")
        sys.exit(1)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    finished_bucket = os.environ.get("FINISHED_BUCKET")
    if finished_bucket is None:
        print("FINISHED_BUCKET environment variable is not set.")
        sys.exit(1)
    cleaner: Cleaner = Cleaner(pipe, finished_bucket)
    logger.info("starting cleaner")
    cleaner.run_forever()
