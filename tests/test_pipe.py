from pathlib import Path
import shutil
import pytest
from pipeline.plumbing import Token, Pipe, dump_token

test_dir = Path("/tmp/test_pipe")
dummy_in = test_dir / "in"
dummy_out = test_dir / "out"
barcode = "12345678"


def reset_test_dirs():
    if test_dir.is_dir():
        try:
            shutil.rmtree(test_dir)
        except OSError as e:
            print(f"error deleting path {test_dir} : {e}")
    dummy_in.mkdir(parents=True)
    dummy_out.mkdir(parents=True)
    token = Token({"barcode": barcode})
    token_path = dummy_in / Path(barcode).with_suffix(".json")
    dump_token(token, token_path)


@pytest.fixture
def test_token():
    tok = Token({"barcode": barcode})
    return tok


@pytest.fixture
def test_pipe():
    pipe: Pipe = Pipe(dummy_in, dummy_out)
    return pipe


def test_pipe_init(test_pipe):
    reset_test_dirs()
    assert test_pipe.input == dummy_in
    assert test_pipe.output == dummy_out
    assert test_pipe.token is None


def test_take_token(test_pipe):
    reset_test_dirs()
    filename = Path(f"{dummy_in}/{barcode}.json")
    backup_name = Path(f"{dummy_in}/{barcode}.bak")
    assert test_pipe.token is None
    assert filename in list(dummy_in.glob("*.*"))
    assert backup_name not in list(dummy_in.glob("*.*"))

    test_pipe.take_token()
    assert test_pipe.token is not None
    assert filename not in list(dummy_in.glob("*.*"))
    assert backup_name in list(dummy_in.glob("*.*"))


def test_take_token_with_arg(test_pipe):
    reset_test_dirs()
    filename = Path(f"{dummy_in}/{barcode}.json")
    backup_name = Path(f"{dummy_in}/{barcode}.bak")
    assert test_pipe.token is None
    assert filename in list(dummy_in.glob("*.*"))
    assert backup_name not in list(dummy_in.glob("*.*"))

    test_pipe.take_token(barcode)
    assert test_pipe.token is not None
    assert filename not in list(dummy_in.glob("*.*"))
    assert backup_name in list(dummy_in.glob("*.*"))



def test_take_token_with_arg_fail(test_pipe):
    reset_test_dirs()
    wrong_barcode="notarealbarcode"
    filename = Path(f"{dummy_in}/{wrong_barcode}.json")
    backup_name = Path(f"{dummy_in}/{wrong_barcode}.bak")
    assert test_pipe.token is None

    test_pipe.take_token(wrong_barcode)
    assert test_pipe.token is None
    assert filename not in list(dummy_in.glob("*.*"))
    assert backup_name not in list(dummy_in.glob("*.*"))

    


def test_put_token(test_pipe):
    reset_test_dirs()
    test_pipe.take_token()
    test_pipe.put_token()
    out_files = list(dummy_out.glob("*.json"))
    filename = Path(f"{dummy_out}/{barcode}.json")
    assert filename in out_files


def test_put_token_with_errorFlg(test_pipe):
    reset_test_dirs()
    test_pipe.take_token()
    test_pipe.put_token(errorFlg=True)
    out_files = list(dummy_in.glob("*.err"))
    error_filename = Path(f"{dummy_in}/{barcode}.err")
    assert error_filename in out_files
