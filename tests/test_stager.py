import csv
from pathlib import Path
import tempfile
import shutil
from pipeline.book_ledger import BookLedger, Book
from pipeline.secretary import Secretary
from pipeline.token_bag import TokenBag
from pipeline.stager import Stager

def test_choose_books(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"


    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)


        processing_bucket = Path("/tmp")


        ledger = BookLedger(test_ledger_file)
        bag = TokenBag(test_token_dir)
        secretary = Secretary(bag, ledger)
                              

        assert len(secretary.chosen_books) == 0
        stager = Stager(secretary, processing_bucket)


        stager.choose_books(how_many=1)
        assert len(secretary.chosen_books) == 1

    
