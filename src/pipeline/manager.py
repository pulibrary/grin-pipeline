import sys
import os
import logging
import signal
from pathlib import Path
from pipeline.plumbing import Pipeline
from pipeline.token_bag import TokenBag
from pipeline.book_ledger import BookLedger
from pipeline.stager import Stager
from pipeline.secretary import Secretary
from pipeline.config_loader import load_config
from pipeline.filters.monitors import RequestMonitor
from pipeline.synchronizer import Synchronizer


class Manager:
    def __init__(self, config:dict):
        self.config = config
        self.ledger = BookLedger(config['global']['ledger_file'])
        self.token_bag = TokenBag(config['global']['token_bag'])
        self.secretary = Secretary(self.token_bag, self.ledger)
        processing_bucket = Path(config['global']['processing_bucket'])
        start_bucket = Path([bucket['path'] for bucket in self.config['buckets'] if bucket['name'] == 'start'][0])        
        self.stager = Stager(self.secretary, processing_bucket, start_bucket)
        self.pipeline = Pipeline(config)
        self.request_monitor = RequestMonitor(self.pipeline)
        self.synchronizer = Synchronizer(config)
        self.processes = []
        self.commands = {
            "exit": {"help" : "exit manager", 'fn': self._exit_command},
            "pipeline status": {'help': 'print pipeline status', 'fn': lambda: print(self.pipeline_status)},
            "ledger status": {"help" : "print ledger status", "fn" : lambda: print(self.ledger_status)},
            "token bag status" : {"help" : "print token bag status", "fn" : lambda: print(self.token_bag_status)},
            "fill token bag" : {"help" : "fill token bag with tokens", "fn" : self._fill_token_bag_command},
            "synchronize" : {"help" : "sync GRIN converted with pipeline", "fn" : self._synchronize_command},
            "dry request monitor" : {"help": "request monitor dry run", "fn": lambda: self.request_monitor.dry_run()},
            "request monitor" : {"help": "request monitor dry run", "fn": lambda: self.request_monitor.run()},
            "stage" : {"help" : "put the tokens into the pipeline", "fn": self._stage_command},
            "help" : {"help" : "show commands", "fn": self._help_command}
        }


    @property
    def pipeline_status(self):
        return self.pipeline.snapshot
            
    @property
    def ledger_status(self):
        return { 'chosen' : self.secretary.chosen_books }
                
    @property
    def token_bag_status(self):
        return self.secretary.bag_size


    def fill_token_bag(self, how_many:int=20):
        self.secretary.choose_books(how_many)
        self.secretary.commit()

    def stage(self):
        self.stager.update_tokens()
        self.stager.stage()


    def repl(self):
        while True:
            cmd = input("manager> ").strip()
            if cmd in self.commands:
                if self.commands[cmd]['fn']():
                    break
            else:
                print(f"Unknown command: {cmd}")


    def _exit_command(self):
                print("\nExiting Manager.")
                return True     # Signal to exit

    def _fill_token_bag_command(self):
        self.fill_token_bag()
        print(self.token_bag_status)
        return False            # Don't exit


    def _help_command(self):
        for k,v in self.commands.items():
            print(f"{k}: {v.get('help')}")
        return False


    def _synchronize_command(self):
        synced = self.synchronizer.synchronize(stage=True)
        print(f"Number of files synchronized: {len(synced)}")
        return False
        
            
    def _stage_command(self):
           self.stager.update_tokens()
           self.stager.stage()
           print("Staged.")
           return False

                
    def run(self):
        def shutdown_handler(signum, frame):
            print("\nShutting down Manager...")
            # do other things
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        self.repl()
    



if __name__ == "__main__":
    from pipeline.config_loader import load_config
    config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
    config: dict = load_config(config_path)

    # Set up logging
    log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())
    
    logging.basicConfig(level=log_level)


    manager = Manager(config)
    manager.run()
