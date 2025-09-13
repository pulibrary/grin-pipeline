from pathlib import Path
import os
from pipeline.secretary import Secretary
from pipeline.token_bag import TokenBag
from pipeline.book_ledger import BookLedger, Book
from pipeline.stager import Stager
from pipeline.config_loader import load_config
import logging

config_path = "/Users/wulfmanc/gh/pulibrary/grin-pipeline/config.yml"
config:dict = load_config(config_path)

# Set up logging
log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

logging.basicConfig(level=log_level)

class Primer:
    def __init__(self, config:dict) -> None:
        self.config = config
        ledger = BookLedger(config['global']['ledger_file'])
        token_bag = TokenBag(config['global']['token_bag'])
        processing_bucket = config['global']['processing_bucket']
        self.start_bucket = Path([bucket['path'] for bucket in self.config['buckets'] if bucket['name'] == 'start'][0])
        self.secretary = Secretary(token_bag, ledger)
        self.stager = Stager(self.secretary, processing_bucket, self.start_bucket)


    def prime(self):
        pass



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--howmany", default=20)

    args = parser.parse_args()

    config:dict = load_config(args.config)
    primer = Primer(config)
    primer.prime(args.howmany)
