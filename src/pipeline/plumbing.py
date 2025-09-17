import json
from pathlib import Path
from datetime import datetime, timezone
from time import sleep
import logging
from typing import Optional

logger: logging.Logger = logging.getLogger(__name__)


class Token:
    def __init__(self, content: dict):
        self.content = content


    def __repr__(self) -> str:
        return f"Token({self.name})"

    def get_prop(self, prop):
        return self.content.get(prop)

    def put_prop(self, prop, val):
        self.content[prop] = val
        return self.get_prop(val)

    @property
    def name(self):
        return self.get_prop('barcode')

    def write_log(
        self, message: str, level: Optional[str] = None, stage: Optional[str] = None
    ):
        entry: dict = {"timestamp": str(datetime.now(timezone.utc)), "message": message}
        if stage:
            entry["stage"] = stage
        if level:
            entry["level"] = level

        self.content.setdefault("log", []).append(entry)

# Utilities for reading and writing Tokens

def load_token(token_file:Path):
    with token_file.open('r') as f:
        token_info = json.load(f)
        return Token(token_info)

def dump_token(token, destination:Path):
    with destination.open("w+") as f:
        json.dump(token.content, fp=f, indent=2)


class Pipe:
    def __init__(self, in_path: Path, out_path: Path) -> None:
        self.input = in_path
        self.output = out_path
        self.token: Token | None = None

    def __repr__(self) -> str:
        return f"Pipe('{self.input}', '{self.output}')"


    def in_path(self, token) -> Path:
        if token is not None and token.name is not None:
            return self.input / Path(token.name).with_suffix(".json")
        else:
            raise ValueError("no token or token name")
    
    def out_path(self, token) -> Path:
        if self.token and self.token.name:
            return self.output / Path(token.name).with_suffix(".json")
        else:
            raise ValueError("no token or token name")

    def marked_path(self, token) -> Path:
        if self.token and self.token.name:
            return self.input / Path(token.name).with_suffix(".bak")
        else:
            raise ValueError("no token or token name")

    def error_path(self, token) -> Path:
        if self.token and self.token.name:
            return self.input / Path(token.name).with_suffix(".err")
        else:
            raise ValueError("no token or token name")


    @property
    def token_in_path(self) -> Path:
        if self.token and self.token.name:
            return self.input
        else:
            raise ValueError("pipe doesn't contain a token")

    @property
    def token_out_path(self) -> Path:
        if self.token and self.token.name:
            return self.output

        else:
            raise ValueError("pipe doesn't contain a token")

    @property
    def token_marked_path(self) -> Path:
        if self.token and self.token.name:
            return self.input 
        else:
            raise ValueError("pipe doesn't contain a token")

    @property
    def token_error_path(self) -> Path:
        if self.token and self.token.name:
            return self.input 
        else:
            raise ValueError("pipe doesn't contain a token")

    def take_token(self):
        if self.token is not None:
            logging.error("there's already a current token")
            return None

        try:
            token_path = next(self.input.glob("*.json"))
            self.token = load_token(token_path)
            self.mark_token()
            return self.token

        except StopIteration:
            return None

    def mark_token(self):
        if self.token and self.token.name:
            unmarked_path:Path = self.in_path(self.token)
            marked_path:Path = self.marked_path(self.token)
            if unmarked_path.is_file():
                unmarked_path.rename(marked_path)


    def delete_marked_token(self):
        marked_path:Path = self.marked_path(self.token)
        marked_path.unlink()

    def put_token(self, errorFlg: bool = False) -> None:

        if self.token:
            if errorFlg:
                error_path = self.error_path(self.token)
                dump_token(self.token, error_path)
            else:
                dump_token(self.token, self.out_path(self.token))

            self.delete_marked_token()
            self.token = None

    def put_token_back(self, errorFlg: bool = False) -> None:
        if self.token:
            if errorFlg:
                put_back_path = self.error_path(self.token)
            else:
                put_back_path = self.in_path(self.token)
                
            dump_token(self.token, put_back_path)

            self.delete_marked_token()
            self.token = None


class Filter:
    def __init__(self, pipe: Pipe):
        self.pipe = pipe
        self.stage_name: str = self.__class__.__name__.lower()

    def log_to_token(self, token, level, message):
        token.write_log(message, level, self.stage_name)

    def run_once(self) -> bool:
        token: Token | None = self.pipe.take_token()
        if not token:
            # logging.info("No tokens available")
            return False

        if self.validate_token(token) is False:
            self.log_to_token(token, "ERROR", "Token did not validate")
            logging.error("token did not validate")
            self.pipe.put_token(errorFlg=True)

            return False

        try:
            processed: bool = self.process_token(token)
            if processed:
                logging.debug(f"Processed token: {token.name}")
                self.log_to_token(token, "INFO", "Stage completed successfully")
                self.pipe.put_token()
            else:
                logging.error(f"Did not proces token: {token.name}")
                self.log_to_token(token, "WARNING", "Stage did not run successfully")
                self.pipe.put_token(errorFlg=True)

            return True

        except Exception as e:
            self.log_to_token(token, "ERROR", f"in {self.stage_name}: {str(e)}")
            logging.error(f"Error processing {token.name}: {str(e)}")
            self.pipe.put_token(errorFlg=True)
            return False

    def run_forever(self, poll_interval=5):
        while True:
            if not self.run_once():
                sleep(poll_interval)

    def process_token(self, token: Token):
        raise NotImplementedError("Subclasses must implement this")

    def validate_token(self, token: Token) -> bool:
        raise NotImplementedError("Subclasses must implement this")

class Pipeline:
    def __init__(self, config:dict):
        self.config = config
        self.buckets = {}
        for rec in self.config.get("buckets", {}):
            name = rec.get("name", "")
            location =  Path(rec.get("path", "/dev/null"))
            self.add_bucket(name, location)

    def add_bucket(self, name:str, location:Path):
        self.buckets[name] = location

    def bucket(self, name:str) -> Path:
        if p := self.buckets.get(name):
            return p
        else:
            raise ValueError(f"no such bucket: {name}")


    def pipe(self, in_bucket:str, out_bucket:str):
        return Pipe(self.bucket(in_bucket), self.bucket(out_bucket))


    @property
    def snapshot(self):
        buckets = {}
        for name, location in self.buckets.items():
            info = { 'waiting_tokens' : [f.name for f in Path(location).glob('*.json')],
                     'errored_tokens' : [f.name for f in Path(location).glob('*.err')],
                     'in_process_tokens' : [f.name for f in Path(location).glob('*.bak')]
            }
            buckets[name] = info
        return buckets
