from pathlib import Path
import tempfile
import shutil
from pipeline.book_ledger import BookLedger, Book
from pipeline.token_bag import TokenBag


def test_add_to_bag_from_ledger(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"

    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)
        assert test_token_dir.is_dir()

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)
        assert test_ledger_file.is_file()

        bag = TokenBag(test_token_dir)
        bag.load()
        assert len(bag.tokens) == 2

        ledger = BookLedger(test_ledger_file)
        assert len(ledger.books) == 9

        barcode = "32101078166681"

        book: Book | None = ledger.entry(barcode)
        assert book is not None and book.status is None

        chosen_book: Book | None = ledger.choose_book(barcode)
        assert chosen_book == book
        assert chosen_book is not None
        assert chosen_book.status == "chosen"
        assert len(bag.tokens) == 2
        bag.add_book(chosen_book.barcode)
        assert len(bag.tokens) == 3
