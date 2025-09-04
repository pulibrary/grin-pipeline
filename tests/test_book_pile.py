from pipeline.book_pile import BookPile, Book


def test_book_pile_read(shared_datadir):
    test_csv_file = shared_datadir / "test_pile.csv"
    pile = BookPile(test_csv_file)

    assert len(pile.books) == 9

def test_book_pile_get_book(shared_datadir):
    test_csv_file = shared_datadir / "test_pile.csv"
    pile = BookPile(test_csv_file)

    barcode = '32101078166681'

    book:Book | None = pile.get_book(barcode)
    assert book is not None and book.status == None
    

def test_book_pile_choose_book(shared_datadir):
    test_csv_file = shared_datadir / "test_pile.csv"
    pile = BookPile(test_csv_file)

    barcode = '32101078166681'

    book:Book | None = pile.get_book(barcode)
    assert book is not None
    assert book.status is None
    

