import os
import subprocess
import yaml
import tempfile
import signal
import sys
import time
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

class Pipe:
    def __init__(self, dir_path: str):
        self.dir = Path(dir_path)
        self.dir.mkdir(parents=True, exist_ok=True)

    def list_tokens(self):
        return sorted(self.dir.glob("*.json"))

    def get_token(self):
        tokens = self.list_tokens()
        if tokens:
            return tokens[0]
        return None

    def move_token(self, token_path, dest_pipe):
        dest_path = dest_pipe.dir / token_path.name
        token_path.rename(dest_path)
        return dest_path

class Filter:
    def __init__(self, input_pipe, output_pipe):
        self.input_pipe = input_pipe
        self.output_pipe = output_pipe
        self.stage_name = self.__class__.__name__.lower()

    def log_to_token(self, token, level, message):
        entry = {
            "stage": self.stage_name,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "message": message
        }
        token.setdefault("log", []).append(entry)

    def run_once(self):
        token_path = self.input_pipe.get_token()
        if not token_path:
            return False

        with open(token_path, 'r') as f:
            token = yaml.safe_load(f)

        try:
            self.process_token(token)
            self.log_to_token(token, "INFO", "Stage completed successfully")

            updated_path = token_path.with_suffix('.tmp')
            with open(updated_path, 'w') as f:
                yaml.dump(token, f)

            final_token_path = self.output_pipe.dir / token_path.name
            updated_path.rename(final_token_path)
            token_path.unlink()
            logging.info("Processed token: %s", token_path.name)
            return True

        except Exception as e:
            self.log_to_token(token, "ERROR", str(e))
            logging.error("Error processing %s: %s", token_path.name, e)
            return False

    def run_forever(self, poll_interval=5):
        while True:
            if not self.run_once():
                time.sleep(poll_interval)

    def process_token(self, token):
        raise NotImplementedError("Subclasses must implement this")

class Decryptor(Filter):
    def process_token(self, token):
        encrypted_path = Path(token['encrypted_path'])
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

        token['decryption_status'] = 'success'
        token['decrypted_path'] = str(decrypted_path)
        token['decrypted_size'] = decrypted_path.stat().st_size
        self.log_to_token(token, "INFO", "Decryption successful")

if __name__ == '__main__':
    if 'GPG_PASSPHRASE' not in os.environ:
        print("Please set the GPG_PASSPHRASE environment variable.")
        sys.exit(1)

    from sys import argv
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    input_pipe = Pipe(args.input)
    output_pipe = Pipe(args.output)
    decryptor = Decryptor(input_pipe, output_pipe)
    decryptor.run_forever()
