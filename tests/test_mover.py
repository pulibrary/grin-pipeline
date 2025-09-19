from pathlib import Path
import pytest
import shutil
from pipeline.plumbing import Pipe, Token, dump_token
from pipeline.filters.mover import Mover

test_dir = Path("/tmp/test_pipe")
barcode = "12345678"
dummy_in = test_dir / "in"
dummy_out = test_dir / "out"
source = test_dir / "source"
destination = test_dir / "destination"
test_file = Path(f"{barcode}.json")


def reset_test_dirs():
    if test_dir.is_dir():
        try:
            shutil.rmtree(test_dir)
        except OSError as e:
            print(f"error deleting path {test_dir} : {e}")
    dummy_in.mkdir(parents=True)
    dummy_out.mkdir(parents=True)
    source.mkdir(parents=True)
    destination.mkdir(parents=True)
    token = Token({"barcode": barcode})
    token.put_prop("source_file", str(source / test_file))
    token.put_prop("destination_file", str(destination / test_file))
    token_path = dummy_in / Path(barcode).with_suffix(".json")
    dump_token(token, token_path)
    with (source / test_file).open('w') as f:
        f.write("This is test data.")
    

@pytest.fixture
def mover():
    pipe: Pipe = Pipe(dummy_in, dummy_out)
    return Mover(pipe)



def test_filter(mover):
    reset_test_dirs()
    assert (source / test_file).exists() is True
    assert (destination / test_file).exists() is False

    mover.run_once()

    assert (source / test_file).exists() is False
    assert (destination / test_file).exists() is True

