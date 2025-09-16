from pathlib import Path
import shutil

from pipeline.manager import Manager

def test_manager(shared_datadir):
    tmpdir = Path("/tmp/test_manager")
    if tmpdir.is_dir():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir

    test_token_dir = Path(tmpdir) / "tokens"
    test_token_dir.mkdir(parents=True)

    ledger_file = shared_datadir / "test_ledger.csv"
    test_ledger_file = tmpdir / "ledger.csv"
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

    manager = Manager(config)

    assert len(manager.pipeline_status['start']['waiting_tokens']) == 0

    assert len(list(start_bucket.glob("*.json"))) == 0

    assert manager.ledger_status == {'chosen': []}
    assert manager.token_bag_status == 0

    manager.fill_token_bag(5)
    assert manager.token_bag_status == 5
    assert all([tok.get_prop('processing_bucket') is None for tok in manager.secretary.bag.tokens])

    manager.stage()

    assert manager.token_bag_status == 0
    waiting_in_start = manager.pipeline_status['start']['waiting_tokens']
    assert len(waiting_in_start) == 5

    assert all([tok.get_prop('processing_bucket') == str(processing_bucket) for tok in manager.secretary.bag.tokens])

