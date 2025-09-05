# token_bag.py


import json
from pathlib import Path
from src.pipeline.plumbing  import Token, load_token, dump_token

        
    

class TokenBag:
    def __init__(self, bag_dir) -> None:
        self.tokens = []
        self.bag_dir = Path(bag_dir)


    def load(self):
        for item in self.bag_dir.iterdir():
            if item.is_file() and item.suffix == '.json':
                token = load_token(item)
                self.tokens.append(token)

    def dump(self) -> None:
        for token in self.tokens:
            dump_token(token, self.bag_dir)

    def find(self, barcode):
        hits = [tok for tok in self.tokens if tok.name == barcode]
        if len(hits) > 0:
            return hits[0]

    def take_token(self, barcode):
        token = self.find(barcode)
        if token is not None:
            self.tokens.remove(token)
            return token
        else:
            raise ValueError(f"token {barcode} not found")


    def put_token(self, token):
        self.tokens.append(token)
        

    def add_book(self, barcode):
        book_token:Token = Token({'barcode' : barcode})
        self.put_token(book_token)

    def add_books(self, book_list:list[str]):
        for book in book_list:
            self.add_book(book)
 
