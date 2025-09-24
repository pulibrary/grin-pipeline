from pathlib import Path

from clients.object_store import S3Client
from pipeline.book_ledger import BookLedger
from pipeline.secretary import Secretary
from pipeline.token_bag import TokenBag

# ledger_file = Path("/tmp/ledgerd/ledger.csv")
# bag_dir = Path("/var/tmp/grin/token_bag")

base_path = Path("/var/tmp/grin/GrinSiphon_data")

ledger_file = base_path / Path("ledger.csv")
bag_dir = base_path / Path("token_bag")

ledger = BookLedger(ledger_file)
bag = TokenBag(bag_dir)
bag.load()

chosen_barcodes = set([book.barcode for book in ledger.all_chosen_books])

s3 = S3Client("/tmp")
uploaded_barcodes = set([obj.Key for obj in s3.list_objects()])


# First, take the set difference of chosen barcodes with ids on AWS to find
# those that have been chosen but not processed (or processed incompletely).
# We will try running these through the pipeline again.

chosen_not_processed = chosen_barcodes.difference(uploaded_barcodes)

# Then, take the inverse set difference to find those that have been processed
# but were never chosen from the ledger. These we will simply mark as completed.

processed_not_chosen = uploaded_barcodes.difference(chosen_barcodes)

secretary = Secretary(bag, ledger)

for barcode in processed_not_chosen:
    secretary.mark_book_completed(barcode)

for barcode in chosen_not_processed:
    secretary.choose_book(barcode)

secretary.commit()
