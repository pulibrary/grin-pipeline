import os
import subprocess
import yaml
import signal
import sys
import logging
from pipeline.config_loader import load_config
from plumbing import Pipeline

config_path: str = os.environ.get("PIPELINE_CONFIG", "config.yml")
config: dict = load_config(config_path)

# Set up logging
log_level = getattr(logging, config.get("global", {}).get("log_level", "INFO").upper())

logging.basicConfig(level=log_level)


class Orchestrator:
    def __init__(self) -> None:
        self.processes = []
        self.pipeline = Pipeline(config)

    def start_filters(self):
        for filt in config.get("filters", []):
            self.start_filter(filt)

    def start_filter(self, filt):
        extra_env = {}

        in_bucket = str(self.pipeline.bucket(filt["pipe"]["in"]))
        out_bucket = str(self.pipeline.bucket(filt["pipe"]["out"]))
        cmd = [
            sys.executable,
            filt["script"],
            "--input",
            in_bucket,
            "--output",
            out_bucket,
        ]
        if filt.get('args'):
            for k,v in filt.get('args').items():
                extra_env[k] = v
                
        # if "decryption_passphrase" in filt:
        #     extra_env["DECRYPTION_PASSPHRASE"] = filt["decryption_passphrase"]

        # if "object_store" in filt:
        #     extra_env["OBJECT_STORE"] = filt["object_store"]

        # if "local_dir" in filt:
        #     extra_env["LOCAL_DIR"] = filt["local_dir"]
        logging.info("Starting filter: %s", " ".join(cmd))
        
        proc = subprocess.Popen(cmd, env={**os.environ, **extra_env})
        self.processes.append((filt["name"], proc))



    def start_filter_old(self, filt):
        extra_env = {}
        cmd = [
            sys.executable,
            filt["script"],
            "--input",
            filt["input_pipe"],
            "--output",
            filt["output_pipe"],
        ]

        if "decryption_passphrase" in filt:
            extra_env["DECRYPTION_PASSPHRASE"] = filt["decryption_passphrase"]

        logging.info("Starting filter: %s", " ".join(cmd))
        proc = subprocess.Popen(cmd, env={**os.environ, **extra_env})
        self.processes.append((filt["name"], proc))

    def stop_filters(self):
        for name, proc in self.processes:
            logging.info("Stopping filter: %s", name)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.processes.clear()

    def status(self):
        for name, proc in self.processes:
            status = "running" if proc.poll() is None else f"exited ({proc.returncode})"
            print(f"Filter '{name}': {status}")

    def reload_config(self):
        logging.info("Reloading configuration...")
        load_config(config_path)

    def add_filter(self, filt):
        logging.info("Adding new filter: %s", filt["name"])
        config["filters"].append(filt)
        self.start_filter(filt)

    def repl(self):
        print("\nOrchestrator REPL. Type 'help' for commands.")
        while True:
            try:
                cmd = input("orchestrator> ").strip()
                if cmd == "exit":
                    print("Exiting orchestrator.")
                    self.stop_filters()
                    break
                elif cmd == "status":
                    self.status()
                elif cmd == "stop":
                    self.stop_filters()
                elif cmd == "start":
                    self.start_filters()
                elif cmd == "restart":
                    self.stop_filters()
                    self.start_filters()
                elif cmd == "reload":
                    self.stop_filters()
                    self.reload_config()
                    self.start_filters()
                elif cmd.startswith("add "):
                    try:
                        new_filter = yaml.safe_load(cmd[4:])
                        self.add_filter(new_filter)
                    except Exception as e:
                        logging.error("Failed to parse new filter: %s", e)
                elif cmd == "help":
                    print(
                        "Available commands: status, start, stop, restart, reload, add <yaml>, exit, help"
                    )
                else:
                    print(f"Unknown command: {cmd}")
            except (KeyboardInterrupt, EOFError):
                print("\nExiting orchestrator.")
                self.stop_filters()
                break

    def run(self, repl:bool=True):
        def shutdown_handler(signum, frame):
            print("\nShutting down Orchestrator...")
            self.stop_filters()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        self.start_filters()
        if repl is True:
            self.repl()



if __name__ == "__main__":
    # if 'GPG_PASSPHRASE' not in os.environ:
    #     print("Please set the GPG_PASSPHRASE environment variable.")
    #     sys.exit(1)
    if "PIPELINE_CONFIG" not in os.environ:
         print("Please set the PIPELINE_CONFIG environment variable.")
         sys.exit(1)

    # config_file = 'config.yml.gpg'
    # config_file = 'config.yml'

    # orchestrator = Orchestrator(config_file)
    orchestrator = Orchestrator()
    orchestrator.run(repl=True)
