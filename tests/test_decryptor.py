import json
from pathlib import Path
from pipeline.filters.decryptor import Decryptor, Pipe

def test_log_to_token(tmp_path):
    # Setup dummy token and paths
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()

    token_path = input_dir / "example.json"
    token_data = {"encrypted_path": "/tmp/example.tar.gz.gpg"}
    token_path.write_text(json.dumps(token_data))

    # Set up pipes and filter
    decryptor = Decryptor(Pipe(input_dir), Pipe(output_dir))

    # Log something directly to the token
    token = json.loads(token_path.read_text())
    decryptor.log_to_token(token, "INFO", "Fake decryption complete")

    assert "log" in token
    assert token["log"][0]["level"] == "INFO"
    assert "Fake decryption complete" in token["log"][0]["message"]
