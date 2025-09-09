from pipeline.token_bag import TokenBag
from pipeline.book_ledger import BookLedger, Book





class BagManager:
    def __init__(self, bag:TokenBag, ledger:BookLedger) -> None:
        self.bag = bag
        self.ledger = ledger

        self.bag.load()
        self.ledger.read_ledger()


    def status(self):
        stats = {}

        stats['bag_current_size'] = len(self.bag.tokens)
        stats['bag_percent_full'] = len(self.bag.tokens) / self.bag.max_size * 100

        return stats


    def choose_book(self, barcode:str):
        if self.ledger.book(barcode) is not  None:
            book:Book = self.ledger.choose_book(barcode)
            self.bag.add_book(book.barcode)
        else:
            raise KeyError(f"{barcode} not in ledger")

        
            
    def commit(self) -> None:
        self.ledger.write_ledger()
        self.bag.dump()
