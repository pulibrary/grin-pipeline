import logging
from pathlib import Path

import boto3
from botocore.exceptions import NoCredentialsError
from pipeline.plumbing import Pipe, Filter, Token
from clients import S3Client


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger: logging.Logger = logging.getLogger(__name__)



class Uploader(Filter):
    def __init__(self, pipe: Pipe, s3_bucket:str="google-books-dev") -> None:
        super().__init__(pipe)
        self.s3_bucket = s3_bucket


    def infile(self, token: Token) -> Path:
        input_path = Path(token.content["processing_bucket"])
        input_filename: Path = Path(token.content["barcode"]).with_suffix(".tgz")
        return input_path / input_filename


    def validate_token(self, token) -> bool:
        s3 = S3Client(local_cache=Path(token.content['processing_bucket']))
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
        s3 = S3Client(local_cache=Path(token.content['processing_bucket']))
        barcode = token.content['barcode']
        if s3.object_exists(barcode):
            self.log_to_token(token, "INFO", "Object exists in store")
            successflg = True
        else:
            logging.info(f"Store operation starting: {barcode}")
            status = s3.store_object(barcode)
            logging.info(f"Store operation complete: {barcode}")
            if status:
                self.log_to_token(token, "INFO", "Object stored")
                successflg = True
            else:
                logging.error(f"Object not stored: {barcode}")
                self.log_to_token(token, "ERROR", "Object not stored")
                successflg = False
            
        return successflg


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))

    uploader: Uploader = Uploader(pipe)
    logger.info("starting uploader")
    uploader.run_forever()
    
