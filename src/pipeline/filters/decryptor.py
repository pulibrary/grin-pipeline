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

# logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

logger: logging.Logger = logging.getLogger(__name__)

class Decryptor(Filter):
    def process_token(self, token:Token):
        encrypted_path = Path(token.content['encrypted_path'])
        if not encrypted_path.exists():
            raise FileNotFoundError(f"Encrypted file not found: {encrypted_path}")

        decrypted_path = encrypted_path.with_suffix('')

        result = subprocess.run([
            'gpg',
            '--batch',
            '--yes',
            '--passphrase', os.environ['GPG_PASSPHRASE'],
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
    # if 'GPG_PASSPHRASE' not in os.environ:
    #     print("Please set the GPG_PASSPHRASE environment variable.")
    #     sys.exit(1)

    # from sys import argv
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    pipe:Pipe = Pipe(args.input, args.output)
    decryptor = Decryptor(pipe)
    decryptor.run_forever()
