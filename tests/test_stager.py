import csv
from pathlib import Path
import tempfile
import shutil
from pipeline.book_ledger import BookLedger, Book
from pipeline.token_bag import TokenBag
from pipeline.plumbing import Token
from pipeline.stager import Stager

def test_stager_init(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"


    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)

        with test_ledger_file.open('r') as f:
            reader = csv.DictReader(f)
            ledger_data = list(reader)
            

        processing_bucket = Path("/tmp")

        stager = Stager(test_ledger_file, test_token_dir, processing_bucket)

        assert isinstance(stager.ledger, BookLedger)
        assert isinstance(stager.token_bag, TokenBag)
        assert stager.processing_bucket == processing_bucket
        assert len(stager.ledger.books) == len(ledger_data)


def test_choose_books(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"


    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)

        processing_bucket = Path("/tmp")

        stager = Stager(test_ledger_file, test_token_dir, processing_bucket)
        first_key = list(stager.ledger.books.keys())[0]
        first_book:Book | None = stager.ledger.book(first_key)
        assert first_book is not None and first_book.status is None
        assert len(stager.token_bag.tokens) == 2
        assert stager.token_bag.find(first_key) is None

        stager.choose_books(1)

        first_book:Book | None = stager.ledger.book(first_key)
        assert first_book is not None and first_book.status is 'chosen'
        assert stager.token_bag.find(first_key) is not None

        
    
