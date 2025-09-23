from pathlib import Path
import tempfile
from unittest.mock import patch
from clients import GrinClient
from pipeline.filters.monitors import RequestMonitor
from pipeline.plumbing import Pipeline, Token, dump_token, load_token

converted_barcode = "1"
in_process_barcode = "2"
neither_barcode = "3"
converted_token = Token({"barcode": converted_barcode})
in_process_token = Token({"barcode": in_process_barcode})
neither_token = Token({"barcode": neither_barcode})


def list_tokens(p: Path):
    all_barcodes = []
    for f in p.glob("*.json"):
        token = load_token(f)
        all_barcodes.append(token.get_prop("barcode"))
    return all_barcodes


@patch.object(GrinClient, "converted_books", [{"barcode": converted_barcode}])
@patch.object(GrinClient, "in_process_books", [{"barcode": in_process_barcode}])
def test_converted_barcodes():
    with tempfile.TemporaryDirectory() as tmpdir:
        requested_bucket = Path(tmpdir) / "requested"
        requested_bucket.mkdir()
        converted_bucket = Path(tmpdir) / "converted"
        converted_bucket.mkdir()
        dump_token(converted_token, requested_bucket / Path("1.json"))
        dump_token(in_process_token, requested_bucket / Path("2.json"))

        pipeline = Pipeline({})
        pipeline.add_bucket("requested", requested_bucket)
        pipeline.add_bucket("converted", converted_bucket)

        monitor = RequestMonitor(pipeline)
        barcode = converted_token.get_prop("barcode")
        assert barcode in list_tokens(requested_bucket)
        assert barcode not in list_tokens(converted_bucket)
        monitor.run()
        assert barcode not in list_tokens(requested_bucket)
        assert barcode in list_tokens(converted_bucket)
