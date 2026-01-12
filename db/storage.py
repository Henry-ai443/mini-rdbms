import os
import json
from typing import List, Dict, Any

from db.catalog import Catalog, TableSchema, SchemaError, ColumnNotFoundError

class Storage:
    def __init__(self, catalog: Catalog, data_dir: str):
        self.catalog = catalog
        self.data_dir = data_dir
        self.tables_data: Dict[str, List[Dict[str, Any]]] = {}

        os.makedirs(data_dir, exist_ok=True)

        self.load_all_tables()

    # -------------------------
    # Load tables from catalog
    # -------------------------
    def load_all_tables(self) -> None:
        for table_name in self.catalog.tables:
            self.tables_data[table_name] = self.load_table(table_name)

    def load_table(self, table_name: str) -> List[Dict[str, Any]]:
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        if not os.path.exists(table_file):
            return []
        with open(table_file, "r") as f:
            return json.load(f)

    def save_table(self, table_name: str) -> None:
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        with open(table_file, "w") as f:
            json.dump(self.tables_data[table_name], f, indent=4)

    # -------------------------
    # Create a new table in storage
    # -------------------------
    def create_table(self, table_name: str) -> None:
        if table_name in self.tables_data:
            raise ValueError(f"Table '{table_name}' already exists in storage.")
        self.tables_data[table_name] = []
        self.save_table(table_name)

    # -------------------------
    # Insert row
    # -------------------------
    def insert_row(self, table_name: str, row: Dict[str, Any]) -> None:
        if table_name not in self.tables_data:
            raise ValueError(f"Table '{table_name}' does not exist in storage.")

        table_schema = self.catalog.get_table(table_name)

        new_row = {}
        for col_name, col_type in table_schema.columns.items():
            if col_name not in row:
                raise SchemaError(f"Missing value for column '{col_name}'.")
            value = row[col_name]
            self.catalog.validate_value(table_name, col_name, value)
            new_row[col_name] = value

        for unique_col in table_schema.unique_columns:
            for existing_row in self.tables_data[table_name]:
                if existing_row[unique_col] == new_row[unique_col]:
                    raise SchemaError(
                        f"Unique constraint violation on column '{unique_col}'"
                    )

        self.tables_data[table_name].append(new_row)
        self.save_table(table_name)

    # -------------------------
    # Query rows
    # -------------------------
    def query_rows(
        self,
        table_name: str,
        filters: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        if table_name not in self.tables_data:
            raise ValueError(f"Table '{table_name}' does not exist in storage.")

        rows = self.tables_data[table_name]
        if not filters:
            return rows.copy()

        result = []
        for row in rows:
            match = True
            for col, val in filters.items():
                if col not in row:
                    raise ColumnNotFoundError(f"Column '{col}' does not exist in table '{table_name}'.")
                if row[col] != val:
                    match = False
                    break
            if match:
                result.append(row)
        return result

    # -------------------------
    # Update rows
    # -------------------------
    def update_rows(
        self,
        table_name: str,
        updates: Dict[str, Any],
        filters: Dict[str, Any]
    ) -> int:
        if table_name not in self.tables_data:
            raise ValueError(f"Table '{table_name}' does not exist in storage.")

        table_schema = self.catalog.get_table(table_name)
        rows = self.tables_data[table_name]
        updated_count = 0

        for row in rows:
            match = True
            for col, val in filters.items():
                if col not in row:
                    raise ColumnNotFoundError(f"Column '{col}' does not exist in table '{table_name}'.")
                if row[col] != val:
                    match = False
                    break

            if match:
                # Validate updated values
                for col, val in updates.items():
                    if col not in table_schema.columns:
                        raise ColumnNotFoundError(f"Column '{col}' does not exist in table '{table_name}'.")
                    self.catalog.validate_value(table_name, col, val)
                    # Check unique constraints
                    if col in table_schema.unique_columns:
                        for other_row in rows:
                            if other_row is not row and other_row[col] == val:
                                raise SchemaError(
                                    f"Unique constraint violation on column '{col}'"
                                )
                    row[col] = val
                updated_count += 1

        self.save_table(table_name)
        return updated_count

    # -------------------------
    # Delete rows
    # -------------------------
    def delete_rows(
        self,
        table_name: str,
        filters: Dict[str, Any]
    ) -> int:
        if table_name not in self.tables_data:
            raise ValueError(f"Table '{table_name}' does not exist in storage.")

        rows = self.tables_data[table_name]
        remaining_rows = []
        deleted_count = 0

        for row in rows:
            match = True
            for col, val in filters.items():
                if col not in row:
                    raise ColumnNotFoundError(f"Column '{col}' does not exist in table '{table_name}'.")
                if row[col] != val:
                    match = False
                    break
            if match:
                deleted_count += 1
            else:
                remaining_rows.append(row)

        self.tables_data[table_name] = remaining_rows
        self.save_table(table_name)
        return deleted_count

    def drop_table(self, table_name: str) -> None:
    # Remove table from in-memory data
        if table_name in self.tables_data:
            del self.tables_data[table_name]

        # Remove the table file
        file_path = os.path.join(self.data_dir, f"{table_name}.json")
        if os.path.exists(file_path):
            os.remove(file_path)
