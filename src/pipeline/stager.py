# stager.py
from pathlib import Path
from pipeline.book_ledger import Book
from pipeline.secretary import Secretary


class Stager:
    def __init__(self, secretary:Secretary,
                 path_to_processing_bucket:Path,
                 path_to_start_bucket:Path) -> None:
        self.secretary = secretary
        self.processing_bucket = path_to_processing_bucket
        self.start_bucket = path_to_start_bucket



    # def choose_books(self, how_many:int):
    #     unprocessed_books = self.secretary.unprocessed_books
    #     if how_many > len(unprocessed_books):
    #         how_many = len(unprocessed_books)
    #     books_to_choose:list[Book] | None  = unprocessed_books[0:how_many]
    #     if books_to_choose:
    #         for book in books_to_choose:
    #             self.secretary.choose_book(book.barcode)


    def update_tokens(self):
        """Fills the token bag. Sets the processing directory
        in the tokens.
        """
        bag = self.secretary.bag
        for token in bag.tokens:
            token.put_prop("processing_bucket", str(self.processing_bucket))
        

    def stage(self, commit:bool=True):
        self.secretary.pour_bag(self.start_bucket)
        if commit:
            self.secretary.commit()

