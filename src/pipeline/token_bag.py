# token_bag.py


from pathlib import Path
from pipeline.plumbing import Token, load_token, dump_token


class TokenBag:
    """
    Holds tokens ready for processing in the pipeline.

    The TokenBag serves as a staging area for tokens before they enter the main
    pipeline. It provides methods for loading tokens from disk, managing them
    in memory, and transferring them to pipeline buckets.

    Attributes:
        tokens (list): List of Token objects currently in the bag
        bag_dir (Path): Directory path where tokens are persisted
    """

    def __init__(self, bag_dir: Path | None = None) -> None:
        self.tokens = []
        if bag_dir:
            self.bag_dir = Path(bag_dir)

    @property
    def size(self) -> int:
        return len(self.tokens)

    def set_bag_dir(self, path: Path):
        self.bag_dir = path

    def clear_bag_dir(self):
        if self.bag_dir:
            for f in self.bag_dir.glob("*.json"):
                f.unlink()

    def load(self):
        """Load all token files from the bag directory into memory."""
        if self.bag_dir:
            for item in self.bag_dir.iterdir():
                if item.is_file() and item.suffix == ".json":
                    token = load_token(item)
                    self.tokens.append(token)

    def dump(self) -> None:
        """Save all in-memory tokens to the bag directory.

        Clears the bag directory first, then writes all tokens as JSON files.
        """
        if self.bag_dir:
            self.clear_bag_dir()
            for token in self.tokens:
                dump_token(token, self.bag_dir / Path(token.name).with_suffix(".json"))

    def find(self, barcode):
        """Find a token by its barcode.

        Args:
            barcode (str): The barcode to search for

        Returns:
            Token | None: The found token, or None if not found
        """
        hits = [tok for tok in self.tokens if tok.name == barcode]
        if len(hits) > 0:
            return hits[0]

    def take_token(self, barcode):
        """Remove and return a token by barcode.

        Args:
            barcode (str): The barcode of the token to remove

        Returns:
            Token: The removed token

        Raises:
            ValueError: If the token is not found
        """
        token = self.find(barcode)
        if token is not None:
            self.tokens.remove(token)
            return token
        else:
            raise ValueError(f"token {barcode} not found")

    def put_token(self, token):
        self.tokens.append(token)

    def add_book(self, barcode):
        """Add a new book token with the given barcode.

        Args:
            barcode (str): The barcode for the new book token
        """
        book_token: Token = Token({"barcode": barcode})
        self.put_token(book_token)

    def add_books(self, book_list: list[str]):
        for book in book_list:
            self.add_book(book)

    def set_processing_directory(self, directory: str, update_tokens: bool = True):
        self.processing_directory = directory
        if update_tokens is True:
            for token in self.tokens:
                token.put_prop("processing_bucket", directory)

    def pour_into(self, bucket: Path) -> None:
        """Transfer all tokens from the bag to a pipeline bucket.

        Removes all tokens from the bag and writes them as JSON files
        in the specified bucket directory.

        Args:
            bucket (Path): Destination bucket directory path
        """
        barcodes = [tok.get_prop("barcode") for tok in self.tokens]
        for barcode in barcodes:
            token = self.take_token(barcode)
            dump_token(token, bucket / Path(token.name).with_suffix(".json"))
