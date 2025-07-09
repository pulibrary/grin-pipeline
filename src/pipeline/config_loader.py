import os
import yaml
import subprocess
import tempfile

def load_config(path: str) -> dict:
    """
    Load a YAML or GPG-encrypted config file.
    """
    if path.endswith(".gpg"):
        passphrase = os.environ.get("GPG_PASSPHRASE")
        if not passphrase:
            raise RuntimeError("GPG_PASSPHRASE not set in environment.")

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            result = subprocess.run([
                "gpg",
                "--batch",
                "--yes",
                "--passphrase", passphrase,
                "--decrypt",
                "--output", tmp.name,
                path
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise RuntimeError(f"GPG decryption failed: {result.stderr.strip()}")
            
            with open(tmp.name) as f:
                config = yaml.safe_load(f)
                os.unlink(tmp.name)
    else:
        with open(path) as f:
            config = yaml.safe_load(f)

    return config
        
