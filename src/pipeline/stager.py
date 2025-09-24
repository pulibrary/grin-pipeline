# stager.py
from pathlib import Path

from pipeline.secretary import Secretary


class Stager:
    """
    Moves tokens from the token bag to the pipeline start bucket.

    The Stager handles the transition of tokens from the staging area (token bag)
    into the active pipeline by updating token metadata and moving them to the
    start bucket where filters can begin processing them.

    Attributes:
        secretary (Secretary): Secretary instance for accessing the token bag
        processing_bucket (Path): Directory where files will be processed
        start_bucket (Path): Pipeline start bucket where tokens begin processing
    """

    def __init__(
        self,
        secretary: Secretary,
        path_to_processing_bucket: Path,
        path_to_start_bucket: Path,
    ) -> None:
        self.secretary = secretary
        self.processing_bucket = path_to_processing_bucket
        self.start_bucket = path_to_start_bucket

    def update_tokens(self):
        """
        Update tokens with processing bucket metadata.

        Sets the processing directory path in each token so that filters
        know where to store and retrieve files during processing.
        """

        bag = self.secretary.bag
        for token in bag.tokens:
            token.put_prop("processing_bucket", str(self.processing_bucket))

    def stage(self, commit: bool = True):
        """Move all tokens from the bag to the pipeline start bucket.

        Args:
            commit (bool): Whether to persist changes to disk. Defaults to True.
        """
        # Transfer all tokens from the bag to the start bucket
        self.secretary.pour_bag(self.start_bucket)
        if commit:
            # Persist ledger and bag state changes to disk
            self.secretary.commit()
