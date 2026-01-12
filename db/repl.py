# db/repl.py

import os
from db.catalog import Catalog
from db.storage import Storage
from db.parser import Parser, ParseError
from db.executor import Executor

def start_repl(data_dir: str = "data"):
    os.makedirs(data_dir, exist_ok=True)
    catalog_path = os.path.join(data_dir, "catalog.json")
    catalog = Catalog(catalog_path)
    storage = Storage(catalog, data_dir)
    parser = Parser()
    executor = Executor(catalog, storage)

    print("Mini RDBMS REPL")
    print("End commands with ';'")
    print("Type EXIT; to quit.\n")

    buffer = ""  # For multi-line queries

    while True:
        try:
            line = input("db> ").strip()
            if not line:
                continue  # skip empty lines
            # accumulate multi-line commands if needed
            command_buffer += " " + line
            if ";" not in line:
                continue  # wait for semicolon

            # full command is ready
            full_command = command_buffer.strip()
            command_buffer = ""  # reset buffer

            if full_command.upper() == "EXIT;":
                print("Bye!")
                break

            # remove trailing semicolon
            if full_command.endswith(";"):
                full_command = full_command[:-1]

            # parse and execute
            parsed = parser.parse(full_command)
            result = executor.execute(parsed)
            print(result)

        except ParseError as e:
            print(f"Parse error: {e}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    start_repl()
