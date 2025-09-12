from pathlib import Path
import shutil
from prime_batch import Primer
from pipeline.secretary import Secretary

def test_primer(shared_datadir):
    ledger_file = shared_datadir / "test_ledger.csv"

    tmpdir = Path("/tmp/test_primer")
    if tmpdir.is_dir():
        shutil.rmtree(tmpdir)
    else:
        tmpdir.mkdir()
    test_token_dir = Path(tmpdir) / "tokens"
    test_token_dir.mkdir(parents=True)

    test_ledger_file = Path(tmpdir) / "ledger.csv"
    shutil.copy(ledger_file, test_ledger_file)


    processing_bucket = Path(tmpdir) / "processing_bucket"
    processing_bucket.mkdir(parents=True)

    start_bucket = Path(tmpdir) / "start_bucket"
    start_bucket.mkdir(parents=True)
        
    config = {}
    config['global'] = {}
    config['global']['ledger_file'] = str(test_ledger_file)
    config['global']['token_bag'] = str(test_token_dir)
    config['global']['processing_bucket'] = str(processing_bucket)
    config['buckets'] =[{'name' : 'start', 'path' : start_bucket}]

    primer:Primer = Primer(config)
    breakpoint()
    secretary = primer.secretary

