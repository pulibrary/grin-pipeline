from pathlib import Path
import tempfile
import shutil
from pipeline.book_ledger import BookLedger, Book
from pipeline.token_bag import TokenBag
from pipeline.secretary import Secretary



def test_token_bag_manager(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"

    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)

        bag = TokenBag(test_token_dir)
        ledger = BookLedger(test_ledger_file)

        secretary = Secretary(bag, ledger)

        stats = secretary.status()
        assert stats['bag_current_size'] == 2

        barcode  = list(ledger.books.keys())[0]
        first_book = ledger.entry(barcode)
        assert first_book is not None
        assert first_book.status is None

        secretary.choose_book(barcode)

        assert first_book.status == 'chosen'

        secretary.commit()
        new_bag = TokenBag(test_token_dir)
        new_bag.load()
        assert new_bag.size == 3

        new_ledger = BookLedger(test_ledger_file)
        book = new_ledger.entry(barcode)
        assert book is not None and book.status == 'chosen'
