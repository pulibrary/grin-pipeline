from pipeline.book_pile import BookPile


def test_book_pile_read(shared_datadir):
    test_csv_file = shared_datadir / "test_pile.csv"
    pile = BookPile(test_csv_file)

    assert len(pile.books) == 9
