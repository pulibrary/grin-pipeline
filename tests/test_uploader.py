from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock
import json
from pipeline.plumbing import Pipe, Token
from pipeline.filters.uploader import Uploader



# Fixture to mock boto3 client
@pytest.fixture
def mock_s3_upload():
    with patch('pipeline.filters.uploader.boto3.client') as mock_client_factory:
        mock_s3 = MagicMock()
        mock_client_factory.return_value = mock_s3
        yield mock_s3

# Set up the pipeline
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


# finally create the filter

def test_filter(mock_s3_upload):
    token = Token(content=token_info, name="1234567")

    uploader = Uploader(pipe, test_s3_bucket)

    result = uploader.process_token(token)


    assert result is True
    assert token.content['upload_status'] == 'success'
    assert token.content['log'][0]['message'] == 'Upload successful'
    mock_s3_upload.upload_file.assert_called_once_with(
        test_file, test_s3_bucket, key)    
