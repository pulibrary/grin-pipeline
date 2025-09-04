from pipeline.book_pile import BookPile
from pipeline.token_bag import TokenBag


def test_add_to_bag_from_pile(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    pile_file = shared_datadir / "test_pile.csv"

    pile = BookPile(pile_file)
    bag = TokenBag(bag_dir)

    
    
