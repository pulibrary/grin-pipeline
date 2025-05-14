from typing import Generator
import json
import os
from pathlib import Path
from datetime import datetime, date, time, timezone
import time
import yaml
import logging
from typing import Optional

logger: logging.Logger = logging.getLogger(__name__)


class Token:
    def __init__(self, content:dict, name:str):
        self.name = name
        self.content = content

    def write_log(self, message:str, level:Optional[str] = None,  stage:Optional[str] = None):
        entry:dict = {"timestamp": str(datetime.now(timezone.utc)),
                      "message": message}
        if stage:
            entry['stage'] = stage
        if level:
            entry['level'] = level

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
    def __init__(self, input_pipe:InPipe, output_pipe:OutPipe):
        self.input_pipe:InPipe = input_pipe
        self.output_pipe:OutPipe = output_pipe
        self.stage_name:str = self.__class__.__name__.lower()

    def log_to_token(self, token, level, message):
        token.write_log(message, level, self.stage_name)

    def run_once(self):
        token = self.input_pipe.take_token()

        if not token:
            return False

        if self.validate_token(token) is False:
            self.log_to_token(token, "ERROR",
                              "Token did not validate")
            return False

        try:
            processed:bool = self.process_token(token)
            if processed:
                self.log_to_token(token, "INFO", "Stage completed successfully")
                self.output_pipe.put_token(token)
                logging.info(f"Processed token: {token.name}")
                return True
            else:
                self.log_to_token(token, "WARNING", "Stage did not successfully")
        except Exception as e:
            self.log_to_token(token, "ERROR", f"in {self.stage_name}: {str(e)}")
            logging.error(f"Error processing {token.name}: {str(e)}")
            return False

                

    def run_forever(self, poll_interval=5):
        while True:
            if not self.run_once():
                time.sleep(poll_interval)

    def process_token(self, token:Token):
        raise NotImplementedError("Subclasses must implement this")

    def validate_token(self, token:Token) -> bool:
        raise NotImplementedError("Subclasses must implement this")


