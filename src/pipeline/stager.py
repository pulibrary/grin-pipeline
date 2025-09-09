# stager.py
import logging
import os
from pathlib import Path
from pipeline.config_loader import load_config
from pipeline.token_bag_manager import BagManager
from pipeline.book_ledger import BookLedger, Book
from pipeline.token_bag import TokenBag
from pipeline.plumbing import Token

config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
config: dict = load_config(config_path)

# Set up logging
log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

logging.basicConfig(level=log_level)


class Stager:
    def __init__(self, path_to_ledger:Path,
                 path_to_token_bag:Path,
                 path_to_processing_bucket:Path) -> None:
        self.ledger = BookLedger(path_to_ledger)
        self.bag_manager = BagManager(TokenBag(path_to_token_bag),
                                      BookLedger(path_to_ledger))
        self.processing_bucket = path_to_processing_bucket



    def report(self):
        chosen_books = self.ledger.all_chosen_books
        print(f"number of chosen books:\t{len(chosen_books)}")


    def commit_changes(self):
        self.bag_manager.commit()


    def choose_books(self, how_many:int):
        all_unprocessed_books = self.ledger.all_unprocessed_books
        books_to_choose:list[Book] | None  = all_unprocessed_books[0:how_many]
        if books_to_choose:
            for book in books_to_choose:
                self.bag_manager.choose_book(book.barcode)


    def stage(self):
        """Fills the token bag. Sets the processing directory
        in the tokens.
        """
        pass
