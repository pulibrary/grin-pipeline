# synchronizer.py
from pathlib import Path
from clients import GrinClient
from pipeline.plumbing import Pipeline
from pipeline.token_bag import TokenBag
from pipeline.book_ledger import BookLedger, Book
from pipeline.stager import Stager
from pipeline.secretary import Secretary
from pipeline.filters.monitors import RequestMonitor


class Synchronizer:
    """A helper object. Sometimes, especially during development, the
    GRIN converted list gets out of sync with our pipeline.  This
    object reads that list and chooses those barcodes from the ledger
    and puts them in the token bag. The stage flag on the synchronize
    method controls whether the tokens stay in the bag or are staged
    directly to the converted_bucket.
    """

    def __init__(self, config: dict) -> None:
        self.config = config
        self.ledger = BookLedger(config["global"]["ledger_file"])
        self.token_bag = TokenBag(config["global"]["token_bag"])
        self.secretary = Secretary(self.token_bag, self.ledger)
        processing_bucket = Path(config["global"]["processing_bucket"])

        # Don't stage to the start bucket as usual; stage to the converted bucket
        pipeline_bucket = Path(
            [
                bucket["path"]
                for bucket in self.config["buckets"]
                if bucket["name"] == "converted"
            ][0]
        )
        self.stager = Stager(self.secretary, processing_bucket, pipeline_bucket)
        self.pipeline = Pipeline(config)
        self.request_monitor = RequestMonitor(self.pipeline)
        self.client = GrinClient()

    @property
    def out_of_sync_barcodes(self) -> list[str] | None:
        """
        Retrieve the list of converted books from GRIN.
        Compare them with those books already marked as chosen;
        return the list of barcodes that are in GRIN's converted
        books list but have not been chosen.
        :return: list of out-of-sync barcodes
        :rtype: list[str] | None
        """
        already_chosen_barcodes: list[str] | None = [
            book.barcode for book in self.secretary.chosen_books
        ]
        unchosen = [
            rec["barcode"]
            for rec in self.client.converted_books
            if rec["barcode"] not in already_chosen_barcodes
        ]
        return unchosen

    def synchronize(
        self, out_of_sync_only: bool = False, stage: bool = True
    ) -> list[Book] | None:
        """
        Synchronize ledger and bag with the converted books in GRIN.

        :param bool out_of_sync_only: If true, choose only books that haven't already been chosen;
        if false, choose all books in the converted books list from GRIN.
        :param bool stage: If true, stage the chosen books to the chosen pipeline bucket.
        :return: list of chosen books
        :rtype: list[Book] | None
        """

        if out_of_sync_only is True:
            barcodes = self.out_of_sync_barcodes
        else:
            barcodes = [rec["barcode"] for rec in self.client.converted_books]

        chosen: list[Book] | None = []
        for barcode in barcodes:
            chosen.append(self.secretary.choose_book(barcode))

        self.secretary.commit()
        if stage is True:
            self.stager.update_tokens()
            self.stager.stage()
        return chosen
