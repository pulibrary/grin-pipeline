import csv
from pathlib import Path
import tempfile
import shutil
from pipeline.book_ledger import BookLedger, Book
from pipeline.secretary import Secretary
from pipeline.token_bag import TokenBag
from pipeline.stager import Stager



def test_update_tokens(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"


    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)

        processing_bucket = Path("/tmp")

        start_bucket = Path(tmpdir) / "start"
        start_bucket.mkdir()
        

        ledger = BookLedger(test_ledger_file)
        bag = TokenBag(test_token_dir)
        secretary = Secretary(bag, ledger)

        stager = Stager(secretary, processing_bucket, start_bucket)


        secretary.choose_books(how_many=3)
        assert all([tok.get_prop('processing_bucket') is None for tok in bag.tokens])
        stager.update_tokens()
        assert all([tok.get_prop('processing_bucket') == str(processing_bucket) for tok in bag.tokens])


def test_stage(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    ledger_file = shared_datadir / "test_ledger.csv"


    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)

        test_ledger_file = Path(tmpdir) / "ledger.csv"
        shutil.copy(ledger_file, test_ledger_file)

        processing_bucket = Path("/tmp")

        start_bucket = Path(tmpdir) / "start"
        start_bucket.mkdir()
        

        ledger = BookLedger(test_ledger_file)
        bag = TokenBag(test_token_dir)
        secretary = Secretary(bag, ledger)

        stager = Stager(secretary, processing_bucket, start_bucket)

        secretary.choose_books(how_many=3)
        assert all([tok.get_prop('processing_bucket') is None for tok in secretary.bag.tokens])
        assert len(secretary.ledger.all_chosen_books) == 3
        stager.update_tokens()
        assert all([tok.get_prop('processing_bucket') == str(processing_bucket) for tok in secretary.bag.tokens])
        
        stager.stage()

        # when the data is read back in, the new values have been persisted
        ledger = BookLedger(test_ledger_file)
        bag = TokenBag(test_token_dir)
        secretary = Secretary(bag, ledger)
        assert len(secretary.ledger.all_chosen_books) == 3
        assert all([tok.get_prop('processing_bucket') == str(processing_bucket) for tok in secretary.bag.tokens])
