from pipeline.token_bag import TokenBag
from pipeline.book_ledger import BookLedger, Book
from pipeline.plumbing  import Token





class Secretary:
    def __init__(self, bag:TokenBag, ledger:BookLedger) -> None:
        self.bag = bag
        self.ledger = ledger

        self.bag.load()
        self.ledger.read_ledger()


    @property
    def bag_size(self) -> int:
        return self.bag.size



    def find_in_bag(self, barcode) -> Token | None:
        return self.bag.find(barcode)

    def find_in_ledger(self, barcode) -> Book | None:
        return self.ledger.book(barcode)

    @property
    def all_unprocessed_books(self) -> list[Book]:
        book_list = []
        books = self.ledger.all_unprocessed_books
        if books:
            book_list = books
        return book_list

    @property
    def all_chosen_books(self) -> list[Book]:
        book_list = []
        books = self.ledger.all_chosen_books
        if books:
            book_list = books
        return book_list


    def status(self):
        stats = {}

        stats['bag_current_size'] = len(self.bag.tokens)

        return stats


    def choose_book(self, barcode:str) -> Book:
        if self.ledger.book(barcode) is not  None:
            book:Book = self.ledger.choose_book(barcode)
            self.bag.add_book(book.barcode)
            return book
        else:
            raise KeyError(f"{barcode} not in ledger")

        
            
    def commit(self) -> None:
        self.ledger.write_ledger()
        self.bag.dump()
