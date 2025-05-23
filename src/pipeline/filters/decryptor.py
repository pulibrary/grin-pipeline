import os
import subprocess
import yaml
import tempfile
import signal
import sys
import time
import logging
from pathlib import Path
from pipeline.plumbing import Pipe, Filter, Token

logger: logging.Logger = logging.getLogger(__name__)

class Decryptor(Filter):
    def __init__(self, pipe:Pipe) -> None:
        passphrase = os.environ.get("DECRYPTION_PASSPHRASE")
        if not passphrase:
            raise RuntimeError("DECRYPTION_PASSPHRASE not set in environment")
        super().__init__(pipe)
        self.passphrase = passphrase
        
    def infile(self, token) -> Path:
        input_path = Path(token.content['buckets']['downloaded'])
        input_filename: Path = Path(token.content['barcode']).with_suffix(".tar.gz.gpg")
        return input_path / input_filename

    def outfile(self, token) -> Path:
        output_path: Path = Path(token.content['buckets']['decrypted'])
        output_filename: Path = Path(token.content['barcode']).with_suffix(".tgz")
        return output_path / output_filename
        
    def validate_token(self, token) -> bool:
        status:bool = True
        
        if self.infile(token).exists() is False:
            logging.error(f"source file does not exist: {self.infile(token)}")
            self.log_to_token(token, "ERROR",
                              f"source file does not exist: {self.infile(token)}")
            status = False

        return status

    
    def process_token(self, token:Token) -> bool:
        successflg = False
        result = subprocess.run([
            'gpg',
            '--batch',
            '--yes',
            '--passphrase', os.environ['DECRYPTION_PASSPHRASE'],
            '--decrypt',
            '--output', str(self.outfile(token)),
            str(self.infile(token))
        ], capture_output=True, text=True)

        if result.returncode != 0:
            successflg = False
            token.content['decryption_status'] = 'fail'
            self.log_to_token(token, "WARNING", "Decryption failed")
        else:
            successflg = True
            token.content['decryption_status'] = 'success'
            self.log_to_token(token, "INFO", "Decryption successful")
            
        return successflg


    def process_token_old(self, token:Token):
        # encrypted_path = Path(token.content['encrypted_path'])
        encrypted_path = Path(token.content['buckets']['downloaded'])

        if not encrypted_path.exists():
            raise FileNotFoundError(f"Encrypted file not found: {encrypted_path}")

        decrypted_path = encrypted_path.with_suffix('')

        result = subprocess.run([
            'gpg',
            '--batch',
            '--yes',
            '--passphrase', os.environ['DECRYPTION_PASSPHRASE'],
            '--decrypt',
            '--output', str(decrypted_path),
            str(encrypted_path)
        ], capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"GPG decryption failed: {result.stderr.strip()}")

        token.content['decryption_status'] = 'success'
        token.content['decrypted_path'] = str(decrypted_path)
        token.content['decrypted_size'] = decrypted_path.stat().st_size
        self.log_to_token(token, "INFO", "Decryption successful")

if __name__ == '__main__':
    if 'DECRYPTION_PASSPHRASE' not in os.environ:
        print("Please set the DECRYPTION_PASSPHRASE environment variable.")
        sys.exit(1)

    # from sys import argv
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--token_input', required=True)
    parser.add_argument('--token_output', required=True)
    args = parser.parse_args()

    pipe:Pipe = Pipe(Path(args.token_input), Path(args.token_output))
    decryptor = Decryptor(pipe)
    decryptor.run_forever()
