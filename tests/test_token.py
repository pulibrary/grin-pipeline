from pipeline.plumbing import Token,load_token, dump_token


def test_load_token(shared_datadir):
    test_token_file = shared_datadir / "tokens/test_token.json"
    test_token:Token = load_token(test_token_file)

    assert test_token.name == '345'
    assert test_token.name == test_token.content['barcode']
    assert test_token.content['source_file'] == "/tmp/test_data/source/test-file.txt"
