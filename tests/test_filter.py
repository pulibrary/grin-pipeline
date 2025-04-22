from pathlib import Path
import json
from pipeline.plumbing import Filter, InPipe, OutPipe
from pipeline.filters.mover import Mover

inpath = Path("/tmp/test_filter/in")
inpath.mkdir(parents=True, exist_ok=True)
outpath = Path("/tmp/test_filter/out")
outpath.mkdir(parents=True, exist_ok=True)


inpipe = InPipe(str(inpath))
outpipe: OutPipe = OutPipe(str(outpath))


input_token_file:Path = inpath / Path("1234567.json")
expected_outfile:Path = outpath / Path("1234567.json")
expected_infile_after_run = inpath / Path("1234567.bak")

token_info:dict = {
    "barcode" : "1234567"
}
with open(inpath / "1234567.json", 'w') as f:
    json.dump(token_info, f, indent=2)
    
filter:Mover = Mover(inpipe, outpipe)



def test_filter():

    assert(input_token_file.exists() is True)
    assert(expected_outfile.exists() is False)

    filter.run_once()

    assert(input_token_file.exists() is False)
    assert(expected_infile_after_run.exists() is True)
    assert (expected_outfile.exists() is True)
    
    
