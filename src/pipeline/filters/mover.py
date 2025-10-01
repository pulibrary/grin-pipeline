# mover.py

# Use to debug the pipeline. Mover monitors an
# input directory, and when it finds a file there
# it moves it to an output directory.
import logging
from pathlib import Path


from pipeline.plumbing import Pipe, Filter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

logger: logging.Logger = logging.getLogger(__name__)


class Mover(Filter):
    def __init__(self, pipe: Pipe):
        super().__init__(pipe)

    def validate_token(self, token) -> bool:
        status: bool = True

        if Path(token.content["source_file"]).exists() is False:
            logging.error(f"source file does not exist: {token.content['source_file']}")
            self.log_to_token(
                token,
                "ERROR",
                f"source file does not exist: {token.content['source_file']}",
            )
            status = False

        if Path(token.content["destination_file"]).exists() is True:
            logging.error(f"destination file already exists: {token.content['destination_file']}")
            self.log_to_token(token, "ERROR", "destination file already exists")
            status = False

        return status

    def process_token(self, token) -> bool:
        completed: bool = False
        # move the file
        try:
            Path(token.content["source_file"]).rename(Path(token.content["destination_file"]))
            self.log_to_token(token, level="INFO", message="moved file")
            completed = True
        except (FileNotFoundError, PermissionError) as e:
            self.log_to_token(token, level="ERROR", message=f"could not move file: {e}")
            completed = False

        return completed


if __name__ == "__main__":
    # if 'GPG_PASSPHRASE' not in os.environ:
    #     print("Please set the GPG_PASSPHRASE environment variable.")
    #     sys.exit(1)

    # from sys import argv
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    mover: Mover = Mover(pipe)
    logger.info("starting mover")
    mover.run_forever()
