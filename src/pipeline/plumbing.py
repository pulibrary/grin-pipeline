from typing import Generator
import json
import os
from pathlib import Path
from datetime import datetime, date, time, timezone
import time
import yaml
import logging

logger: logging.Logger = logging.getLogger(__name__)


class Token:
    def __init__(self, content:dict, name:str):
        self.name = name
        self.content = content

    def write_log(self, level:str, message:str):
        entry:dict = {"timestamp": str(datetime.now(timezone.utc)),
                      "level": level,
                      "message": message}
        self.content.setdefault("log", []).append(entry)


class Pipe:
    def __init__(self, dir_path:str):
        self.endpoint = Path(dir_path)

    def __repr__(self) -> str:
        return f"Pipe('{self.endpoint}')"


class InPipe(Pipe):
    def take_token(self) -> Token | None:
        try:
            token_path:Path = next(self.endpoint.glob("*.json"))
            token_name = token_path.name

            with open(token_path, 'r') as f:
                token = Token(json.load(f), token_name)

            self.mark_token(token_path)
            return token
        
        except StopIteration:
            return None

    def mark_token(self, token_path)-> Path:
        backup_path:Path = token_path.with_suffix(".bak")
        token_path.rename(backup_path)
        return backup_path
    

class OutPipe(Pipe):
    def put_token(self, token:Token) -> None:
        token_path = self.endpoint / token.name
        with open(token_path, 'w') as f:
            json.dump(token.content, f)


class Filter:
    def __init__(self, input_pipe:Pipe, output_pipe:Pipe):
        self.input_pipe = input_pipe
        self.output_pipe = output_pipe
        self.stage_name = self.__class__.__name__.lower()

    def log_to_token(self, token, level, message):
        entry = {
            "stage": self.stage_name,
            "timestamp": datetime.now(timezone.utc),
            "level": level,
            "message": message
        }
        token.setdefault("log", []).append(entry)

    def run_once(self):
        token_path = self.input_pipe.get_token()
        if not token_path:
            return False

        with open(token_path, 'r') as f:
            token = yaml.safe_load(f)

        try:
            self.process_token(token)
            self.log_to_token(token, "INFO", "Stage completed successfully")

            updated_path = token_path.with_suffix('.tmp')
            with open(updated_path, 'w') as f:
                yaml.dump(token, f)

            final_token_path = self.output_pipe.dir / token_path.name
            updated_path.rename(final_token_path)
            token_path.unlink()
            logging.info("Processed token: %s", token_path.name)
            return True

        except Exception as e:
            self.log_to_token(token, "ERROR", str(e))
            logging.error("Error processing %s: %s", token_path.name, e)
            return False

    def run_forever(self, poll_interval=5):
        while True:
            if not self.run_once():
                time.sleep(poll_interval)

    def process_token(self, token):
        raise NotImplementedError("Subclasses must implement this")
