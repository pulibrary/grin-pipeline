import sys
import os
import logging
import signal
import subprocess
from pathlib import Path


class Manager:
    def __init__(self, path_to_config:str) -> None:
        self.config_file = Path(path_to_config)
        self.config: dict = load_config(config_path)
        self.processes = []

    def start_process(self, process):
        extra_env = {}
        cmd = [
            sys.executable,
            process["script"],
            "--config",
            self.config_file
        ]
        logging.info(f"Starting process: {cmd}")
        proc = subprocess.Popen(cmd, env={**os.environ, **extra_env})
        self.processes.append((process["name"], proc))
        

    def stop_processes(self):
        for name, proc in self.processes:
            logging.info(f"Stopping process: {name}")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.processes.clear()



    def get_process(self, process_name):
        processes = self.config.get('processes', [])
        hits = [proc for proc in processes if proc.get('name') == process_name]
        if hits:
            return hits[0]
        else:
            return None

    def repl(self):
        print("\nManager REPL. Type 'help' for commands.")
        while True:
            try:
                cmd = input("manager> ").strip()
                if cmd == "exit":
                    print("Exiting manager.")
                    self.stop_processes()
                    break
                elif cmd == "help":
                    print("Available commands: exit, orchestrator start")
                elif cmd == "orchestrator start":
                    proc = self.get_process('orchestrator')
                    self.start_process(proc)
                elif cmd == "orchestrator stop":
                    pass
                else:
                    print(f"Unknown command: {cmd}")
            except (KeyboardInterrupt, EOFError):
                print("\nExiting Manager.")
                self.stop_processes()
                break
            
                


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


    manager = Manager(config_path)
    manager.run()
