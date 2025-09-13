from pathlib import Path
import tempfile
import shutil
from pipeline.book_ledger import BookLedger, Book
from pipeline.token_bag import TokenBag
from pipeline.secretary import Secretary

def test_sizes(shared_datadir):
    ledger = BookLedger(shared_datadir / "test_ledger.csv")
    bag = TokenBag(shared_datadir / "tokens")
    secretary = Secretary(bag, ledger)

    assert secretary.bag_size == 2
    assert len(secretary.unprocessed_books) == 9
    assert len(secretary.chosen_books) == 0

def test_choose_book(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)

        secretary = Secretary(TokenBag(test_token_dir),
                              BookLedger(test_ledger_file))

        barcode = '32101078166681'

        book = secretary.find_in_ledger(barcode)
        assert book is not None and book.status is None
        assert secretary.find_in_bag(barcode) is None
        
        secretary.choose_book(barcode)

        assert book.status == 'chosen'
        assert secretary.find_in_bag(barcode) is not None
        
def test_choose_books(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)

        secretary = Secretary(TokenBag(test_token_dir),
                              BookLedger(test_ledger_file))

        pretest_bag_size = secretary.bag_size
        pretest_chosen_books = secretary.chosen_books

        secretary.choose_books(2)
        assert secretary.bag_size == pretest_bag_size + 2
        assert len(secretary.chosen_books) == len(pretest_chosen_books) + 2
