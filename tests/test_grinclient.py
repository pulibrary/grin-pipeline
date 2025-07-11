from clients import GrinClient

# for development only; remove
cfile = "/Users/wulfmanc/gh/pulibrary/pugrin/.creds"
sfile = "/Users/wulfmanc/gh/pulibrary/pugrin/.secrets"


def test_client():
    client = GrinClient()
    assert client.base_url == "https://books.google.com/libraries"
    assert client.directory == "PRNC"
