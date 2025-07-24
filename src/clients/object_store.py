# object_store.py

# This module implements clients to object-storage services that may
# be used to upload Google Books objects (tarballs) to a storage
# system.

from pathlib import Path
import boto3


class ObjectStore:
    def __init__(self):
        self.object_service = None


class S3Client(ObjectStore):
    def __init__(self, local_cache:Path, bucket_name:str = 'google-books-dev'):
        super().__init__()
        self.object_service = "Amazon S3"
        self.bucket_name = bucket_name
        self.cache = local_cache
        self.client = boto3.client('s3')

    def object_exists(self, key:str) -> bool:
        try:
            self.client.get_object_attributes(
                Bucket=self.bucket_name,
                Key=key,
                ObjectAttributes=['ETag'])
            return True
        except self.client.exceptions.NoSuchKey as e:
            print(f"nosuchkey {key}: {e}")
            return False


    def store_file(self, file_path, object_name=None) -> bool:
        if object_name is None:
            object_name = Path(file_path).stem
        try:
            self.client.upload_file(file_path, self.bucket_name, object_name)
            return True

        except self.client.exceptions.NoCredentialsError as e:
            print(f"AWS credentials not available: {e}")
            return False

    def store_object(self, barcode, overwrite=False) -> bool:
        result = False
        file_path = self.cache / Path(barcode).with_suffix(".tgz")
        if file_path.is_file():
            if self.object_exists(barcode):
                print(f"object {barcode} has already been stored.")
                if overwrite is True:
                    print(f"overwriting {barcode}")
                    result = self.store_file(file_path, barcode)
            else:
                result = self.store_file(file_path, barcode)
        return result
            
            

        
