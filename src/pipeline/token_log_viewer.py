import json
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from pipeline import logging_config
import logging

logging_config.configure_logging()
logger: logging.Logger = logging.getLogger(__name__)



def load_token(path):
    with open(path, 'r') as f:
        return json.load(f)


def display_log(token):
    console = Console()
    table = Table(title="Token Processing Log")

    table.add_column("Stage", style="cyan", no_wrap=True)
    table.add_column("Timestamp", style="green")
    table.add_column("Level", style="bold")
    table.add_column("Message")

    log_entries = token.get("log", [])
    for entry in log_entries:
        table.add_row(
            entry.get("stage", "?"),
            entry.get("timestamp", "?"),
            entry.get("level", "?"),
            entry.get("message", "")
        )

    console.print(table)


def main():
    if len(sys.argv) != 2:
        print("Usage: python token_log_viewer.py <path-to-token.json>")
        sys.exit(1)

    token_path = Path(sys.argv[1])
    if not token_path.exists():
        print(f"Token file not found: {token_path}")
        sys.exit(1)

    token = load_token(token_path)
    display_log(token)


if __name__ == '__main__':
    main()
