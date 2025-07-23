import logging
from pathlib import Path

import boto3
from botocore.exceptions import NoCredentialsError
from pipeline.plumbing import Pipe, Filter, Token


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger: logging.Logger = logging.getLogger(__name__)



def upload_to_s3(file_name, bucket_name, object_name=None):
    """
    Uploads a file to an S3 bucket.
    
    :param file_name: Path to the local file to upload
    :param bucket_name: Name of the target S3 bucket
    :param object_name: S3 object name (if different from local file name)
    :return: True if file was uploaded, else False
    """
    if object_name is None:
        object_name = file_name

    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"Upload Successful: {file_name} -> s3://{bucket_name}/{object_name}")
        return True
    except FileNotFoundError:
        print("Error: The file was not found.")
    except NoCredentialsError:
        print("Error: AWS credentials not available.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return False


class Uploader(Filter):
    def __init__(self, pipe: Pipe, s3_bucket:str) -> None:
        super().__init__(pipe)
        self.s3_bucket = s3_bucket


    def infile(self, token: Token) -> Path:
        input_path = Path(token.content["processing_bucket"])
        input_filename: Path = Path(token.content["barcode"]).with_suffix(".tgz")
        return input_path / input_filename


    def validate_token(self, token) -> bool:
        status: bool = True
        if self.infile(token).exists() is False:
            logging.error(f"source file does not exist: {self.infile(token)}")
            self.log_to_token(
                token, "ERROR", f"source file does not exist: {self.infile(token)}"
            )
            status = False

        return status

    def process_token(self, token:Token) -> bool:
        successflg = False
        s3 = boto3.client('s3')
        key = token.content['barcode']
        try:
            s3.upload_file(self.infile(token), self.s3_bucket, key)
            token.content['upload_status'] = "success"
            self.log_to_token(token, "INFO", "Upload successful")
            successflg = True

        except FileNotFoundError as e:
            token.content['upload_status'] = "fail"
            self.log_to_token(token, "ERROR", f"Upload error: {e}")
            successflg = False

        except NoCredentialsError as e:
            token.content['upload_status'] = "fail"
            self.log_to_token(token, "ERROR", f"Credentials error: {e}")
            successflg = False

        
        return successflg
