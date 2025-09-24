import shutil
import tempfile
from pathlib import Path

from pipeline.token_bag import TokenBag


def test_load_bag(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    assert len(bag.tokens) == 2


def test_find_token(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    tok = bag.find_token("345")
    assert tok is not None
    assert tok.name == "345"

    tok = bag.find_token("foobarbaz")
    assert tok is None


def test_put_token(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    assert len(bag.tokens) == 2
    tok = bag.take_token("345")
    assert tok.name == "345"
    assert len(bag.tokens) == 1
    bag.put_token(tok)
    assert len(bag.tokens) == 2

    assert bag.find_token("345") == tok


def test_add_books(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    barcodes = ["5678", "91234"]

    bag.add_books(barcodes)

    new_tok = bag.find_token(barcodes[0])
    assert new_tok is not None
    assert new_tok.name == barcodes[0]

    new_tok = bag.find_token(barcodes[1])
    assert new_tok is not None
    assert new_tok.name == barcodes[1]


def test_dump_bag(shared_datadir):
    with tempfile.TemporaryDirectory() as tmpdir:
        bag_dir = shared_datadir / "tokens"
        test_bag_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_bag_dir)

        bag = TokenBag(test_bag_dir)
        bag.load()
        assert len(bag.tokens) == 2
        bag.dump()
        bag2 = TokenBag(test_bag_dir)
        bag2.load()
        assert len(bag2.tokens) == 2


def test_take_token(shared_datadir):
    with tempfile.TemporaryDirectory() as tmpdir:
        bag_dir = shared_datadir / "tokens"
        test_bag_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_bag_dir)

        barcode = "234"
        files = list(test_bag_dir.glob("*.json"))
        assert len(files) == 2
        bag = TokenBag(test_bag_dir)
        bag.load()
        assert len(bag.tokens) == 2
        # tok = bag.find_token(barcode)
        tok = bag.take_token(barcode)
        assert tok is not None and tok.get_prop("barcode") == barcode

        assert len(bag.tokens) == 1

        tok = bag.find_token(barcode)
        assert tok is None

        bag.dump()

        files = list(test_bag_dir.glob("*.json"))
        assert len(files) == 1

        bag = TokenBag(test_bag_dir)
        bag.load()
        assert len(bag.tokens) == 1
        tok = bag.find_token(barcode)
        assert tok is None


def test_assign_processing_bucket(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    barcodes = ["5678", "91234"]

    bag.add_books(barcodes)
    tok = bag.find_token("5678")
    assert tok is not None
    assert tok.get_prop("processing_bucket") is None
    fake_processing_bucket = "/tmp"
    bag.set_processing_directory(fake_processing_bucket)
    tok = bag.find_token("5678")
    assert tok is not None
    assert tok.get_prop("processing_bucket") == fake_processing_bucket


def test_pour_into(shared_datadir):
    with tempfile.TemporaryDirectory() as tmpdir:
        bag_dir = shared_datadir / "tokens"
        test_bag_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_bag_dir)
        bucket = Path(tmpdir) / "bucket"
        bucket.mkdir()

        bag = TokenBag(test_bag_dir)
        bag.load()
        assert len(bag.tokens) == 2
        assert len(list(bucket.glob("*.json"))) == 0
        bag.pour_into(bucket)
        assert len(list(bucket.glob("*.json"))) == 2
