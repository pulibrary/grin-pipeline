from pipeline.token_bag import TokenBag


def test_load_bag(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    assert len(bag.tokens) == 2


def test_find_token(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    tok = bag.find('345')
    assert tok is not None
    assert tok.name == '345'

    tok = bag.find("foobarbaz")
    assert tok is None


def test_take_token(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    assert len(bag.tokens) == 2
    tok = bag.take_token('345')
    assert tok.name == '345'
    assert len(bag.tokens) == 1
    assert bag.find('345') is None


def test_put_token(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    assert len(bag.tokens) == 2
    tok = bag.take_token('345')
    assert tok.name == '345'
    assert len(bag.tokens) == 1
    bag.put_token(tok)
    assert len(bag.tokens) == 2

    assert bag.find('345') == tok


def test_add_books(shared_datadir):
    bag_dir = shared_datadir / "tokens"
    bag = TokenBag(bag_dir)
    bag.load()

    barcodes = ['5678', '91234']

    bag.add_books(barcodes)

    new_tok = bag.find(barcodes[0])
    assert new_tok is not None
    assert new_tok.name == barcodes[0]
    
    new_tok = bag.find(barcodes[1])
    assert new_tok is not None
    assert new_tok.name == barcodes[1]
    
