from pathlib import Path
import json
from pipeline.plumbing import Pipe
from pipeline.filters.mover import Mover

# Set up the pipeline
pipe_in = Path("/tmp/test_pipeline/in")
pipe_in.mkdir(parents=True, exist_ok=True)
pipe_out = Path("/tmp/test_pipeline/out")
pipe_out.mkdir(parents=True, exist_ok=True)

input_token_file:Path = pipe_in / Path("234.json")

pipe = Pipe(pipe_in, pipe_out)

# Set up the mock data filesystem
source: Path = Path("/tmp/test_data/source")
source.mkdir(parents=True, exist_ok=True)
destination: Path = Path("/tmp/test_data/destination")
destination.mkdir(parents=True, exist_ok=True)

test_file = Path("test_file.txt")

source_file:Path = source / test_file
destination_file:Path = destination / test_file

for f in [source_file, destination_file, input_token_file]:
    if f.exists():
        f.unlink()

token_info:dict = {
    "barcode" : "1234567",
    "source_file" : str(source_file),
    "destination_file" : str(destination_file)
    
}

with open(input_token_file, 'w') as f:
    json.dump(token_info, f, indent=2)
    
# mock up the data file
with open(source_file, mode='w') as f:
    f.write("This is test data.")

# finally create the filter
filter:Mover = Mover(pipe)




def test_filter():

    assert(source_file.exists() is True)
    assert(destination_file.exists() is False)

    filter.run_once()

    assert(source_file.exists() is False)
    assert(destination_file.exists() is True)
    
