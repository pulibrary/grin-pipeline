import logging
import os
from pathlib import Path

from clients.object_store import S3Client
from pipeline.book_ledger import BookLedger
from pipeline.secretary import Secretary
from pipeline.token_bag import TokenBag


class AwsSynchronizer:
    def __init__(self, ledger: BookLedger, bag: TokenBag):
        self.ledger = ledger
        self.bag = bag
        self.s3 = S3Client("/tmp")

    def sync(self):
        chosen_barcodes = set([book.barcode for book in self.ledger.all_chosen_books])
        uploaded_barcodes = set([obj.Key for obj in self.s3.list_objects()])

        # First, take the set difference of chosen barcodes with ids on AWS to find
        # those that have been chosen but not processed (or processed incompletely).
        # We will try running these through the pipeline again.

        chosen_not_processed = chosen_barcodes.difference(uploaded_barcodes)

        # Then, take the inverse set difference to find those that have been processed
        # but were never chosen from the ledger. These we will simply mark as completed.

        processed_not_chosen = uploaded_barcodes.difference(chosen_barcodes)

        secretary = Secretary(self.bag, self.ledger)

        for barcode in processed_not_chosen:
            secretary.mark_book_completed(barcode)

            for barcode in chosen_not_processed:
                secretary.choose_book(barcode)

        secretary.commit()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger_file", required=True)
    parser.add_argument("--token_bag_dir", required=True)
    args = parser.parse_args()

    ledger = BookLedger(Path(args.ledger_file))
    bag = TokenBag(Path(args.token_bag_dir))
    synchronizer = AwsSynchronizer(ledger, bag)
    synchronizer.sync()
