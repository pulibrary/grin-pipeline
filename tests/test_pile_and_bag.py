from pathlib import Path
import tempfile
import shutil
from pipeline.book_pile import BookPile, Book
from pipeline.token_bag import TokenBag



def test_add_to_bag_from_pile(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    pile_file = shared_datadir / "test_pile.csv"

    with tempfile.TemporaryDirectory() as tmpdir:
        test_token_dir = Path(tmpdir) / "tokens"
        shutil.copytree(bag_dir, test_token_dir)
        assert test_token_dir.is_dir()

        test_pile_file = Path(tmpdir) / "pile.csv"
        shutil.copy(pile_file, test_pile_file)
        assert test_pile_file.is_file()

        bag = TokenBag(test_token_dir)
        bag.load()
        assert len(bag.tokens) == 2

        pile = BookPile(test_pile_file)
        assert len(pile.books) == 9

        barcode = '32101078166681'

        book:Book | None = pile.get_book(barcode)
        assert book is not None and book.status is None

        chosen_book:Book | None = pile.choose_book(barcode)
        assert chosen_book == book
        assert chosen_book is not None
        assert chosen_book.status == 'chosen'

        bag.add_book(chosen_book.barcode)
        assert len(bag.tokens) == 3

        
