# synchronizer.py
from pathlib import Path
from clients import GrinClient
from pipeline.plumbing import Pipeline
from pipeline.token_bag import TokenBag
from pipeline.book_ledger import BookLedger, Book
from pipeline.stager import Stager
from pipeline.secretary import Secretary
from pipeline.config_loader import load_config
from pipeline.filters.monitors import RequestMonitor


class Synchronizer:
    def __init__(self, config:dict):
        self.config = config
        self.ledger = BookLedger(config['global']['ledger_file'])
        self.token_bag = TokenBag(config['global']['token_bag'])
        self.secretary = Secretary(self.token_bag, self.ledger)
        processing_bucket = Path(config['global']['processing_bucket'])
        start_bucket = Path([bucket['path'] for bucket in self.config['buckets'] if bucket['name'] == 'start'][0])        
        self.stager = Stager(self.secretary, processing_bucket, start_bucket)
        self.pipeline = Pipeline(config)
        self.request_monitor = RequestMonitor(self.pipeline)
        self.client = GrinClient()
 

    @property
    def out_of_sync_barcodes(self):
        already_chosen_barcodes : list[str] | None = [book.barcode for book in self.secretary.chosen_books]
        unchosen = [rec['barcode'] for rec in self.client.converted_books
                    if rec['barcode'] not in already_chosen_barcodes]
        return unchosen
        
    def synchronize(self):
        chosen = []
        for barcode in self.out_of_sync_barcodes:
            chosen.append(self.secretary.choose_book(barcode))
        self.secretary.commit()
        self.stager.stage()
        
        return chosen
