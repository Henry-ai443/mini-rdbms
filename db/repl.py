# db/repl.py

from db.engine import DatabaseEngine

def start_repl():
    engine = DatabaseEngine()

    print("Mini RDBMS REPL")
    print("End commands with ';'")
    print("Type EXIT; to quit.\n")

    command_buffer = ""

    while True:
        try:
            line = input("db> ").strip()
            if not line:
                continue

            command_buffer += " " + line

            if ";" not in line:
                continue

            sql = command_buffer.strip()
            command_buffer = ""

            if sql.upper() == "EXIT;":
                print("Bye!")
                break

            if sql.endswith(";"):
                sql = sql[:-1]

            response = engine.execute_sql(sql)

            if response["status"] == "ok":
                print(response["result"])
            else:
                print(f"{response['error_type'].capitalize()} error: {response['message']}")

        except KeyboardInterrupt:
            print("\nBye!")
            break

if __name__ == "__main__":
    start_repl()
