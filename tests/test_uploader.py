from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock
import json
from pipeline.plumbing import Pipe, Token
from pipeline.filters.uploader import AWSUploader
from clients import S3Client


# Fixture to mock boto3 client
# @pytest.fixture
# def mock_s3_upload():
#     with patch('pipeline.filters.uploader.boto3.client') as mock_client_factory:
#         mock_s3 = MagicMock()
#         mock_client_factory.return_value = mock_s3
#         yield mock_s3

# Set up the pipeline

def test_upload_when_object_already_exists():
    pipe_in = Path("/tmp/test_pipeline/in")
    pipe_in.mkdir(parents=True, exist_ok=True)
    pipe_out = Path("/tmp/test_pipeline/out")
    pipe_out.mkdir(parents=True, exist_ok=True)
    
    input_token_file: Path = pipe_in / Path("1234567.json")
    
    pipe = Pipe(pipe_in, pipe_out)
    
    # Set up the mock data filesystem
    processing_bucket = Path("/tmp/test_data/processing")
    processing_bucket.mkdir(parents=True, exist_ok=True)
    
    test_file:Path = processing_bucket / Path("1234567.tgz")
    
    # mock up the data file
    with test_file.open("w") as f:
        f.write("this is test data")
        
    key = "1234567"
    test_s3_bucket = "google-books-dev"
        

    token_info: dict = {
        "barcode": "1234567",
        "processing_bucket": str(processing_bucket)
    }

    with open(input_token_file, "w") as f:
        json.dump(token_info, f, indent=2)
    mock_s3 = MagicMock()
    mock_s3.object_exists.return_value = True  # pretend object already exists
    uploader = AWSUploader(pipe, mock_s3)
    token = Token(content=token_info, name="1234567")
    result = uploader.process_token(token)

    assert result is True
    assert token.content['upload_status'] == 'duplicate'
    assert token.content['log'][0]['message'] == 'Object exists in store'

def test_upload_when_object_does_not_already_exist():
    pipe_in = Path("/tmp/test_pipeline/in")
    pipe_in.mkdir(parents=True, exist_ok=True)
    pipe_out = Path("/tmp/test_pipeline/out")
    pipe_out.mkdir(parents=True, exist_ok=True)

    input_token_file: Path = pipe_in / Path("1234567.json")

    pipe = Pipe(pipe_in, pipe_out)

    # Set up the mock data filesystem
    processing_bucket = Path("/tmp/test_data/processing")
    processing_bucket.mkdir(parents=True, exist_ok=True)

    test_file:Path = processing_bucket / Path("1234567.tgz")

    # mock up the data file
    with test_file.open("w") as f:
        f.write("this is test data")

    key = "1234567"
    test_s3_bucket = "google-books-dev"


    token_info: dict = {
        "barcode": "1234567",
        "processing_bucket": str(processing_bucket)
    }

    with open(input_token_file, "w") as f:
        json.dump(token_info, f)

    mock_s31 = MagicMock()
    mock_s31.object_exists.return_value = False  # pretend object does not exist
    mock_s31.store_object.return_value = True  # pretend object was stored

    uploader = AWSUploader(pipe, mock_s31)
    token = Token(content=token_info, name="789")

    result = uploader.process_token(token)
    assert result is True
    assert token.content['upload_status'] == 'success'
    assert token.content['log'][0]['message'] == 'Object stored'


def test_upload_failure():
    pipe_in = Path("/tmp/test_pipeline/in")
    pipe_in.mkdir(parents=True, exist_ok=True)
    pipe_out = Path("/tmp/test_pipeline/out")
    pipe_out.mkdir(parents=True, exist_ok=True)

    input_token_file: Path = pipe_in / Path("1234567.json")

    pipe = Pipe(pipe_in, pipe_out)

    # Set up the mock data filesystem
    processing_bucket = Path("/tmp/test_data/processing")
    processing_bucket.mkdir(parents=True, exist_ok=True)

    test_file:Path = processing_bucket / Path("1234567.tgz")

    # mock up the data file
    with test_file.open("w") as f:
        f.write("this is test data")

    key = "1234567"
    test_s3_bucket = "google-books-dev"


    token_info: dict = {
        "barcode": "1234567",
        "processing_bucket": str(processing_bucket)
    }

    with open(input_token_file, "w") as f:
        json.dump(token_info, f, indent=2)
    mock_s31 = MagicMock()
    mock_s31.object_exists.return_value = False  # pretend object does not exist
    mock_s31.store_object.return_value = False  # pretend object was not stored

    uploader = AWSUploader(pipe, mock_s31)
    token = Token(content=token_info, name="789")

    result = uploader.process_token(token)
    assert result is False
    assert token.content['upload_status'] == 'fail'
    assert token.content['log'][0]['message'] == 'Object not stored'
        
