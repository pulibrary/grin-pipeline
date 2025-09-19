from pipeline.book_ledger import BookLedger, Book


def test_book_ledger_read(shared_datadir):
    test_csv_file = shared_datadir / "test_ledger.csv"
    ledger = BookLedger(test_csv_file)

    assert len(ledger.books) == 9


def test_book_ledger_book(shared_datadir):
    test_csv_file = shared_datadir / "test_ledger.csv"
    ledger = BookLedger(test_csv_file)

    barcode = "32101078166681"

    book: Book | None = ledger.entry(barcode)
    assert book is not None and book.status is None


def test_book_ledger_choose_book(shared_datadir):
    test_csv_file = shared_datadir / "test_ledger.csv"
    ledger = BookLedger(test_csv_file)

    barcode = "32101078166681"

    book: Book | None = ledger.entry(barcode)
    assert book is not None
    assert book.status is None
