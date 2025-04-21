# mover.py

# Use to debug the pipeline. Mover monitors an
# input directory, and when it finds a file there
# it moves it to an output directory.

from pipeline.plumbing import Pipe, Filter


class Mover(Filter):
    def __init__(self, input_pipe:Pipe, output_pipe:Pipe) :
        super().__init__(input_pipe, output_pipe)

    def process_token(self, token) -> None:
        """ Do nothing; just move the token."""
        pass
