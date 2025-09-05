# book_ledger.py

import csv
from dataclasses import dataclass
from pathlib import Path
import shutil


@dataclass
class Book:
    barcode:str
    date_requested:str | None
    date_completed:str | None
    status:str | None

    def __init__(self,
                 barcode:str, date_requested: str, date_completed: str, status: str| None) -> None:
        self.barcode = barcode
        self.date_requested = None
        self.date_completed = None
        self.status = None

        if date_requested: self.date_requested = date_requested
        if date_completed: self.date_completed = date_completed
        if status: self.status = status

    
                 
    


class BookLedger:
    def __init__(self, csv_file):
        self.csv_file = Path(csv_file)
        self._books = None
        self._fieldnames = []
        self.chosen_books = []
        

    def read_ledger(self):
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
                books = self.books
                with self.csv_file.open('w') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for _,v in books.items():
                        writer.writerow(v)
            else:
                raise ValueError(f"no fieldnames")
     
        
    @property
    def books(self):
        if self._books is None:
            self._books = self.read_ledger()
        return self._books


    def get_book(self, barcode):
        book:Book | None = self.books.get(barcode)
        if book is not None:
            return book
        

    def set_book(self, barcode, book:Book):
        self.books[barcode] = book


    def choose_book(self, barcode):
        book = self.get_book(barcode)
        if book:
            book.status = 'chosen'
            self.chosen_books.append(book)
            return book

        else:
            raise ValueError(f"book {barcode} not in ledger")
