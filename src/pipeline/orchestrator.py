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
    """
    Launches and manages filter processes for the pipeline.

    The Orchestrator is responsible for starting, stopping, and monitoring
    individual filter processes that make up the pipeline stages. Each filter
    runs as a separate subprocess, allowing for parallel processing and
    fault isolation.

    Attributes:
        processes (list): List of tuples containing (filter_name, subprocess.Popen)
        pipeline (Pipeline): Pipeline instance for bucket management
    """
    def __init__(self) -> None:
        self.processes = []
        self.pipeline = Pipeline(config)

    def start_filters(self):
        """Start all configured filter processes.

        Iterates through the filters defined in the configuration and starts
        each one as a separate subprocess.
        """
        for filt in config.get("filters", []):
            self.start_filter(filt)

    def start_filter(self, filt):
        """Start a single filter process.

        Args:
            filt (dict): Filter configuration containing script path, arguments,
                        and pipe configuration.
        """
        extra_env = {}

        # Resolve bucket names to actual directory paths
        in_bucket = str(self.pipeline.bucket(filt["pipe"]["in"]))
        out_bucket = str(self.pipeline.bucket(filt["pipe"]["out"]))

        # Build command line for filter subprocess
        cmd = [
            sys.executable,
            filt["script"],
            "--input",
            in_bucket,
            "--output",
            out_bucket,
        ]

        # Add any filter-specific environment variables
        if filt.get("args"):
            for k, v in filt.get("args").items():
                extra_env[k] = v

        logging.info("Starting filter: %s", " ".join(cmd))

        # Start the filter process with combined environment
        proc = subprocess.Popen(cmd, env={**os.environ, **extra_env})
        # Track the process for lifecycle management
        self.processes.append((filt["name"], proc))


    def stop_filters(self):
        """Stop all running filter processes gracefully.

        Sends SIGTERM to each process and waits up to 5 seconds for graceful
        shutdown. If a process doesn't terminate, it will be killed with SIGKILL.
        """
        for name, proc in self.processes:
            logging.info("Stopping filter: %s", name)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.processes.clear()

    def status(self):
        """Print the status of all filter processes.

        Shows whether each filter is running or has exited, along with
        the exit code for terminated processes.
        """
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
        """Run the interactive command interface for orchestrator management.

        Provides commands for starting, stopping, restarting filters, checking
        status, and dynamically adding new filters.
        """
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

    def run(self, repl: bool = True):
        """Run the orchestrator with optional REPL interface.

        Args:
            repl (bool): Whether to start the interactive REPL interface.
                        Defaults to True.
        """
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
