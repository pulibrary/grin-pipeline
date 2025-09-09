# stager.py
import logging
import os
from pathlib import Path
from pipeline.config_loader import load_config
from pipeline.secretary import Secretary

from pipeline.book_ledger import BookLedger, Book
from pipeline.token_bag import TokenBag
from pipeline.plumbing import Token

config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
config: dict = load_config(config_path)

# Set up logging
log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

logging.basicConfig(level=log_level)


class Stager:
    def __init__(self, secretary:Secretary, path_to_processing_bucket:Path) -> None:
        self.secretary = secretary
        self.processing_bucket = path_to_processing_bucket



    def choose_books(self, how_many:int):
        unprocessed_books = self.secretary.unprocessed_books
        books_to_choose:list[Book] | None  = unprocessed_books[0:how_many]
        if books_to_choose:
            for book in books_to_choose:
                self.secretary.choose_book(book.barcode)


    def update_tokens(self):
        """Fills the token bag. Sets the processing directory
        in the tokens.
        """
        bag = self.secretary.bag
        for token in bag.tokens:
            token.put_prop("processing_bucket", str(self.processing_bucket))
        

    def stage(self):
        self.secretary.commit()
