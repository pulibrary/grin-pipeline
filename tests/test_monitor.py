from pathlib import Path
import shutil
from pipeline.filters.monitors import RequestMonitor
from pipeline.plumbing import Pipeline, Token, dump_token





def test_monitor(shared_datadir):
    config = {}
    config['buckets'] = [{'name': 'start', 'path': '/var/tmp/grin/pipeline/start'},
                         {'name': 'requested', 'path': '/var/tmp/grin/pipeline/requested'}]

    pipeline:Pipeline = Pipeline(config)


def test_setup():

    tmpdir = Path("/tmp/test_monitor")
    requested_dir = Path(tmpdir) / 'requested'
    converted_dir = Path(tmpdir) / 'converted'

    requested_dir.mkdir(parents=True, exist_ok=True)
    converted_dir.mkdir(parents=True, exist_ok=True)

    barcode1 = '1234567'
    barcode2 = '2345678'
    
    tok1 = Token({'barcode': barcode1})
    tok2 = Token({'barcode': barcode2})
    
    dump_token(tok1, requested_dir)
    dump_token(tok2, requested_dir)
    
    
    config = {}
    config['buckets'] = []
    config['buckets'].append({'name' : 'requested', 'path' : requested_dir})
    config['buckets'].append({'name' : 'converted', 'path' : converted_dir})
    
    requested_files = []
    for f in requested_dir.glob("*.json"):
        requested_files.append(f.stem)

    converted_files = []
    for f in converted_dir.glob("*.json"):
        converted_files.append(f.stem)
    
    assert barcode1 in requested_files
    assert barcode2 in requested_files
    assert barcode1 not in converted_files
    assert barcode2 not in converted_files


    
    pipeline:Pipeline = Pipeline(config)
    
    monitor = RequestMonitor(pipeline)
    monitor._in_process_barcodes = [barcode1]
    monitor._converted_barcodes = [barcode2]
        
    monitor.run()

    requested_files = []
    for f in requested_dir.glob("*.json"):
        requested_files.append(f.stem)

    converted_files = []
    for f in converted_dir.glob("*.json"):
        converted_files.append(f.stem)
    
    assert barcode1 in requested_files
    assert barcode2 not in requested_files
    assert barcode1 not in converted_files
    assert barcode2 in converted_files

    shutil.rmtree(tmpdir)
