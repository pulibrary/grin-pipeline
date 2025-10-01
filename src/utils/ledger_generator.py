from dataclasses import fields
from pathlib import Path
from csv import DictWriter
from clients import GrinClient, S3Client
from pipeline.book_ledger import Book, BookStatus


class LedgerGenerator:
    def __init__(self) -> None:
        self.grin = GrinClient()
        self.s3 = S3Client("/tmp")

        self._grin_books = None
        self._ledger = None
        self._s3_books = None

    @property
    def grin_books(self):
        if self._grin_books is None:
            self._grin_books = self.grin.all_books
        return self._grin_books

    @property
    def s3_books(self):
        if self._s3_books is None:
            self._s3_books = set([obj.Key for obj in self.s3.list_objects()])
        return self._s3_books

    @property
    def ledger(self):
        if self._ledger is None:
            self._ledger = []
            for row in self.grin_books:
                record = {
                    "barcode": row["barcode"],
                    "date_chosen": None,
                    "date_completed": None,
                    "status": None,
                }
                self._ledger.append(record)
        return self._ledger

    def synchronize_ledger(self):
        """Mark as completed each book in
        the ledger that is in the s3 bucket."""

        for rec in self.ledger:
            if rec["barcode"] in self.s3_books:
                rec["status"] = BookStatus.COMPLETED

    def dump_ledger(self, file_path: Path):
        fieldnames = [f.name for f in fields(Book)]
        with file_path.open("w+", newline="") as f:
            writer = DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(self.ledger)
