import sys
import os
import logging
from pathlib import Path

from pipeline.plumbing import Pipe, Filter, Token
from clients import S3Client


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger: logging.Logger = logging.getLogger(__name__)



class Uploader(Filter):
    def __init__(self, pipe: Pipe) -> None:
        super().__init__(pipe)



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
        raise NotImplementedError("Subclasses must implement 'process_token'.")


class AWSUploader(Uploader):
    def __init__(self, pipe: Pipe, s3_client:S3Client) -> None:
        super().__init__(pipe)
        self.client = s3_client


    def validate_token(self, token:Token) -> bool:
        status:bool = True
        barcode = token.get_prop('barcode')
        if barcode is not None:
            if self.client.object_exists(barcode):
                token.put_prop("upload_status", "duplicate")
        return status

    def process_token(self, token:Token) -> bool:
        successflg = False
        barcode = token.get_prop('barcode')

        logging.info(f"Store operation starting: {barcode}")
        if token.get_prop("upload_status") == "duplicate":
            self.log_to_token(token, "INFO", "Object already stored.")
            successflg = True

        else:
            status = self.client.store_object(barcode)
        
            logging.info(f"Store operation complete: {barcode}")
            if status is True:
                self.log_to_token(token, "INFO", "Object stored")
                token.put_prop("upload_status", "success")
                successflg = True
            else:
                logging.error(f"Object not stored: {barcode}")
                self.log_to_token(token, "ERROR", "Object not stored")
                token.put_prop("upload_status", "fail")

                successflg = False

        return successflg
    

if __name__ == "__main__":
    if "OBJECT_STORE" not in os.environ:
        print("Please set the OBJECT_STORE environment variable.")
        sys.exit(1)

    if "LOCAL_DIR" not in os.environ:
        print("Please set the LOCAL_DIR environment variable (probably the processing bucket).")
        sys.exit(1)


    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pipe: Pipe = Pipe(Path(args.input), Path(args.output))
    s3_client = S3Client(os.environ.get("LOCAL_DIR"), os.environ.get("OBJECT_STORE"))


    uploader: AWSUploader = AWSUploader(pipe, s3_client)
    logger.info("starting uploader")
    uploader.run_forever()
    
