# book_ledger.py
from datetime import datetime
import csv
from dataclasses import dataclass, asdict
from pathlib import Path
import shutil


@dataclass
class Book:
    barcode:str
    date_chosen:str | None
    date_completed:str | None
    status:str | None

    def __init__(self,
                 barcode:str, date_chosen: str, date_completed: str, status: str| None) -> None:
        self.barcode = barcode
        self.date_chosen = None
        self.date_completed = None
        self.status = None

        if date_chosen: self.date_chosen = date_chosen
        if date_completed: self.date_completed = date_completed
        if status: self.status = status

    
                 
    


class BookLedger:
    def __init__(self, csv_file):
        self.csv_file = Path(csv_file)
        self._books: dict[str, Book] | None = None
        self._fieldnames = []
        self.chosen_books = []
        
            

    def read_ledger(self) -> dict[str, Book]:
        books = {}
        with self.csv_file.open('r') as f:
            reader:csv.DictReader = csv.DictReader(f)
            self._fieldnames = reader.fieldnames
            for row in reader:
                barcode = row.get('barcode')
                books[barcode] = Book(**row)
        return books


    def write_ledger(self, backup=True):
        if backup is True:
            backup_path = Path(f"{str(self.csv_file)}~")
            shutil.copy2(self.csv_file, backup_path)
            with self.csv_file.open('r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
            if fieldnames:
                with self.csv_file.open('w') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for _,book in self.books.items():
                        writer.writerow(asdict(book))
            else:
                raise ValueError(f"no fieldnames")
     
        
    @property
    def books(self):
        if self._books is None:
            self._books = self.read_ledger()
        return self._books


    def book(self, barcode) -> Book | None:
        book = self.books.get(barcode)
        if book is not None:
            return book
        else:
            return None
        

    def set_book(self, barcode, book:Book):
        self.books[barcode] = book


    def choose_book(self, barcode):
        book = self.book(barcode)
        if book:
            book.status = 'chosen'
            book.date_chosen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.chosen_books.append(book)
            return book

        else:
            raise ValueError(f"book {barcode} not in ledger")

    @property
    def all_chosen_books(self) -> list[Book] | None:
        books = [book for _,book in self.books.items() if book.status == 'chosen']
        
        

    @property
    def all_completed_books(self) -> list[Book] | None:
        books = [book for _,book in self.books.items() if book.status == 'completed']


    @property
    def all_unprocessed_books(self) -> list[Book] | None:
        return [book for _,book in self.books.items() if book.status is None]
