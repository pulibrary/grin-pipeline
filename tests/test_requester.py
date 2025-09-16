from pathlib import Path
import tempfile
from pipeline.plumbing import Pipe, Token, dump_token
from pipeline.filters.requester import Requester


def test_requester():
    with tempfile.TemporaryDirectory() as tmpdir:
        pipe_in = Path(tmpdir) / "in"
        pipe_out = Path(tmpdir) / "out"

        pipe_in.mkdir()
        pipe_out.mkdir()

        pipe = Pipe(pipe_in, pipe_out)

        tok = Token({"barcode" : "12345" })
        dump_token(tok, pipe_in)
        
        
        requester = Requester(pipe)

        requester.run_once()
        assert len(list(pipe.input.glob("*.*"))) == 0
        assert len(list(pipe.output.glob("*.*"))) == 1

