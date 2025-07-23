from pathlib import Path
import json
import boto3
from pipeline.plumbing import Pipe
from pipeline.filters.uploader import Uploader

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
filter: Uploader = Uploader(pipe, test_s3_bucket)


def test_filter():
    s3 = boto3.client('s3')
    try:
        existsp = s3.get_object(Bucket=test_s3_bucket, Key=key)
    except:
        existsp = False

    assert existsp is False


    filter.run_once()

    response = s3.get_object(Bucket=test_s3_bucket, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
