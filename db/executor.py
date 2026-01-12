# db/executor.py

from typing import Dict, Any, List

from db.catalog import Catalog, TableSchema, SchemaError
from db.storage import Storage
from db.parser import ParseError

class Executor:
    def __init__(self, catalog: Catalog, storage: Storage):
        self.catalog = catalog
        self.storage = storage

    def execute(self, command: Dict[str, Any]) -> Any:
        cmd_type = command.get("type")
        if cmd_type == "CREATE_TABLE":
            return self._execute_create_table(command)
        elif cmd_type == "INSERT":
            return self._execute_insert(command)
        elif cmd_type == "SELECT":
            return self._execute_select(command)
        elif cmd_type == "UPDATE":
            return self._execute_update(command)
        elif cmd_type == "DELETE":
            return self._execute_delete(command)
        elif cmd_type == "DROP_TABLE":
            return self._execute_drop_table(command)
        else:
            raise ValueError(f"Unsupported command type: {cmd_type}")

    # -------------------------
    # CREATE TABLE
    # -------------------------
    def _execute_create_table(self, command: Dict[str, Any]) -> str:
        try:
            schema = TableSchema(
                name=command["table_name"],
                columns=command["columns"],
                primary_key=command["primary_key"],
                unique_columns=command.get("unique_columns", [])
            )

            # 1. Register schema
            self.catalog.create_table(schema)

            # 2. Create physical storage
            self.storage.create_table(schema.name)

            return f"Table '{schema.name}' created successfully."

        except SchemaError as e:
            return f"Error creating table: {e}"
        except Exception as e:
            return f"Storage error: {e}"

    # -------------------------
    # INSERT
    # -------------------------
    def _execute_insert(self, command: Dict[str, Any]) -> str:
        table_name = command["table_name"]
        values = command["values"]

        try:
            table_schema = self.catalog.get_table(table_name)

            if len(values) != len(table_schema.columns):
                return f"Error: Expected {len(table_schema.columns)} values, got {len(values)}."

            row = {col_name: val for col_name, val in zip(table_schema.columns.keys(), values)}

            self.storage.insert_row(table_name, row)

            return f"1 row inserted into '{table_name}'."

        except Exception as e:
            return f"Error inserting row: {e}"

    # -------------------------
    # SELECT
    # -------------------------
    def _execute_select(self, command: Dict[str, Any]) -> List[Dict[str, Any]]:
        table_name = command["table_name"]
        columns = command["columns"]
        where = command.get("where", {})

        try:
            rows = self.storage.query_rows(table_name, where)

            if columns == ["*"]:
                return rows

            filtered_rows = [{col: row[col] for col in columns if col in row} for row in rows]

            return filtered_rows

        except Exception as e:
            raise RuntimeError(f"Error executing SELECT: {e}")

    # -------------------------
    # UPDATE
    # -------------------------
    def _execute_update(self, command: Dict[str, Any]) -> str:
        table_name = command["table_name"]
        updates = command["updates"]
        where = command.get("where", {})

        try:
            updated_count = self.storage.update_rows(table_name, updates, where)
            return f"{updated_count} row(s) updated in '{table_name}'."

        except Exception as e:
            return f"Error updating rows: {e}"

    # -------------------------
    # DELETE
    # -------------------------
    def _execute_delete(self, command: Dict[str, Any]) -> str:
        table_name = command["table_name"]
        where = command.get("where", {})

        try:
            deleted_count = self.storage.delete_rows(table_name, where)
            return f"{deleted_count} row(s) deleted from '{table_name}'."

        except Exception as e:
            return f"Error deleting rows: {e}"

    # -------------------------
    # DROP TABLE
    # -------------------------
    def _execute_drop_table(self, command: Dict[str, Any]) -> str:
        table_name = command["table_name"]
        try:
            # 1. Remove physical storage
            self.storage.drop_table(table_name)

            # 2. Remove schema from catalog
            # (already handled in storage.drop_table, but double-checking is fine)
            if self.catalog.table_exists(table_name):
                self.catalog.drop_table(table_name)

            return f"Table '{table_name}' dropped successfully."

        except Exception as e:
            return f"Error dropping table: {e}"

