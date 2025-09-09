# token_bag.py


import json
from pathlib import Path
from pipeline.plumbing  import Token, load_token, dump_token

        
    

class TokenBag:
    def __init__(self, bag_dir: Path | None = None) -> None:
        self.tokens = []
        if bag_dir:
            self.bag_dir = Path(bag_dir)


    @property
    def size(self) -> int:
        return len(self.tokens)

    def set_bag_dir(self, path:Path):
        self.bag_dir = path

    def clear_bag_dir(self):
        if self.bag_dir:
            for f in self.bag_dir.glob("*.json"):
                f.unlink()

    def load(self):
        if self.bag_dir:
            for item in self.bag_dir.iterdir():
                if item.is_file() and item.suffix == '.json':
                    token = load_token(item)
                    self.tokens.append(token)

    def dump(self) -> None:
        if self.bag_dir:
            self.clear_bag_dir()
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


    def set_processing_directory(self, directory:str, update_tokens:bool=True):
        self.processing_directory = directory
        if update_tokens is True:
            for token in self.tokens:
                token.put_prop('processing_bucket', directory)
