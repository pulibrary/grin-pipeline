# mover.py

# Use to debug the pipeline. Mover monitors an
# input directory, and when it finds a file there
# it moves it to an output directory.
import logging
from pathlib import Path


from pipeline.plumbing import InPipe, OutPipe, Filter

logger: logging.Logger = logging.getLogger(__name__)

class Mover(Filter):
    def __init__(self, input_pipe:InPipe, output_pipe:OutPipe) :
        super().__init__(input_pipe, output_pipe)

    def validate_token(self, token) -> bool:
        status:bool = True
        if Path(token.content['source_file']).exists() is False:
            self.log_to_token(token, "ERROR", f"source file does not exist: {token.content['source_file']}")
            status = False

        if Path(token.content['destination_file']).exists() is True:
            self.log_to_token(token, "ERROR", "destination file already exists")
            status = False
        return status
    

    def process_token(self, token) -> bool:
        completed:bool = False
        # move the file
        try:
            Path(token.content['source_file']).rename(Path(token.content['destination_file']))
            self.log_to_token(token, level="INFO", message="moved file")
            completed = True
        except (FileNotFoundError, PermissionError) as e:
            self.log_to_token(token, level="ERROR",
                              message=f"could not move file: {e}")
            completed = False

        return completed
