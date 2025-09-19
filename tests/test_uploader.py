from pathlib import Path
import shutil
import pytest
from unittest.mock import MagicMock
from pipeline.plumbing import Pipe, Token, dump_token, load_token
from pipeline.filters.uploader import AWSUploader

test_pipeline = Path("/tmp/test_pipeline")
pipe_in = test_pipeline / Path("in")
pipe_out = test_pipeline / Path("out")
processing_bucket = Path("/tmp/test_data/processing")
barcode = "1234567"
test_file_path = processing_bucket / Path(barcode).with_suffix(".tgz")
test_token_in_path = pipe_in / Path(barcode).with_suffix(".json")
test_token_out_path = pipe_out / Path(barcode).with_suffix(".json")
test_token_err_path = pipe_in / Path(barcode).with_suffix(".err")


def gen_token() -> Token:
    return Token({"barcode": "1234567", "processing_bucket": str(processing_bucket)})


def reset_data():
    if test_pipeline.is_dir():
        shutil.rmtree(test_pipeline)
    pipe_in.mkdir(parents=True)
    pipe_out.mkdir(parents=True)
    dump_token(gen_token(), test_token_in_path)

    if processing_bucket.is_dir():
        shutil.rmtree(processing_bucket)
    processing_bucket.mkdir(parents=True)
    with test_file_path.open("w+") as f:
        f.write("this is test data")


# Set up the pipeline


@pytest.fixture
def pipe() -> Pipe:
    reset_data()
    pipe = Pipe(pipe_in, pipe_out)
    return pipe


def test_when_object_already_exists(pipe):
    mock_s3 = MagicMock()
    mock_s3.object_exists.return_value = True  # pretend object already exists
    uploader: AWSUploader = AWSUploader(pipe, mock_s3)
    uploader.run_once()
    token = load_token(test_token_out_path)
    assert token.get_prop("upload_status") == "duplicate"
    assert token.content["log"][0]["message"] == "Object already stored."


def test_when_object_does_not_already_exist(pipe):
    mock_s3 = MagicMock()
    mock_s3.object_exists.return_value = False  # pretend object does not exist
    mock_s3.store_object.return_value = True  # pretend object was stored

    uploader: AWSUploader = AWSUploader(pipe, mock_s3)
    uploader.run_once()
    token = load_token(test_token_out_path)
    assert token.content["upload_status"] == "success"
    assert token.content["log"][0]["message"] == "Object stored"


def test_upload_fails(pipe):
    mock_s3 = MagicMock()
    mock_s3.object_exists.return_value = False  # pretend object does not exist
    mock_s3.store_object.return_value = False  # pretend object was not stored

    uploader: AWSUploader = AWSUploader(pipe, mock_s3)
    uploader.run_once()
    token = load_token(test_token_err_path)
    assert token.get_prop("upload_status") == "fail"
