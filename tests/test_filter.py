from pathlib import Path
import json
from pipeline.plumbing import Filter, Pipe


inpath = Path("/tmp/test_pipeline_filter/in")
inpath.mkdir(parents=True, exist_ok=True)

outpath = Path("/tmp/test_pipeline_filter/out")
outpath.mkdir(parents=True, exist_ok=True)


pipe = Pipe(inpath, outpath)


input_token_file: Path = inpath / Path("1234567.json")
expected_outfile: Path = outpath / Path("1234567.json")

token_info: dict = {"barcode": "1234567"}

for f in [input_token_file, expected_outfile]:
    if f.exists():
        f.unlink()

with open(input_token_file, "w") as f:
    json.dump(token_info, f, indent=2)

# Use a test filter: DoNothing just logs
# to the token.


class DoNothing(Filter):
    def __init__(self, pipe: Pipe) -> None:
        super().__init__(pipe)

    def validate_token(self, token) -> bool:
        return True

    def process_token(self, token) -> bool:
        self.log_to_token(token, "INFO", "did nothing on purpose")
        return True


filter: DoNothing = DoNothing(pipe)


def test_filter():
    assert input_token_file.exists() is True
    assert expected_outfile.exists() is False

    filter.run_once()

    assert input_token_file.exists() is False
    assert expected_outfile.exists() is True
