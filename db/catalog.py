import json
import os
from typing import Dict, List, Any

# -------------------------
# Supported data types
# -------------------------
SUPPORTED_TYPES = {
    "INT": int,
    "TEXT": str,
    "BOOL": bool,
}

# -------------------------
# Exceptions
# -------------------------
class CatalogError(Exception):
    pass

class SchemaError(CatalogError):
    pass

class TableNotFoundError(CatalogError):
    pass

class ColumnNotFoundError(CatalogError):
    pass

# -------------------------
# Table Schema
# -------------------------
class TableSchema:
    def __init__(
        self,
        name: str,
        columns: Dict[str, str],
        primary_key: str,
        unique_columns: List[str],
    ):
        self.name = name.strip()
        if not self.name:
            raise SchemaError("Table name cannot be empty.")

        if not columns:
            raise SchemaError("Table must have at least one column.")

        # Remove whitespace in column names and normalize types
        columns = {col.strip(): col_type.strip().upper() for col, col_type in columns.items()}

        # Check for duplicate column names
        if len(columns) != len(set(columns.keys())):
            raise SchemaError("Duplicate column names are not allowed.")

        # Check supported types
        for col, col_type in columns.items():
            if col_type not in SUPPORTED_TYPES:
                raise SchemaError(f"Unsupported data type '{col_type}' for column '{col}'.")

        # Validate primary key
        if primary_key not in columns:
            raise SchemaError(f"Primary key '{primary_key}' must be a column in the table.")

        # Validate unique columns
        for uc in unique_columns:
            if uc not in columns:
                raise SchemaError(f"Unique column '{uc}' must be a column in the table.")

        # Ensure primary key is unique implicitly
        if primary_key not in unique_columns:
            unique_columns.append(primary_key)

        self.columns = columns
        self.primary_key = primary_key
        self.unique_columns = unique_columns

# -------------------------
# Catalog
# -------------------------
class Catalog:
    def __init__(self, catalog_path: str):
        self.catalog_path = catalog_path
        self.tables: Dict[str, "TableSchema"] = {}
        self.load()  # Load existing tables from catalog.json

    # ---- persistence ----
    def load(self) -> None:
        if not os.path.exists(self.catalog_path):
            self.tables = {}
            return

        # Check if file is empty
        if os.path.getsize(self.catalog_path) == 0:
            self.tables = {}
            return

        with open(self.catalog_path, "r") as f:
            data = json.load(f)

        self.tables = {}
        for table_name, table_info in data.items():
            name = table_name
            columns = table_info["columns"]
            primary_key = table_info["primary_key"]
            unique_columns = table_info.get("unique_columns", [])
            schema = TableSchema(
                name=name,
                columns=columns,
                primary_key=primary_key,
                unique_columns=unique_columns
            )
            self.tables[name] = schema

    def save(self) -> None:
        data = {}
        for table_name, schema in self.tables.items():
            data[table_name] = {
                "columns": schema.columns,
                "primary_key": schema.primary_key,
                "unique_columns": schema.unique_columns
            }

        os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
        with open(self.catalog_path, "w") as f:
            json.dump(data, f, indent=4)

    # ---- schema management ----
    def create_table(self, schema: "TableSchema") -> None:
        if self.table_exists(schema.name):
            raise SchemaError(f"Table '{schema.name}' already exists.")
        self.tables[schema.name] = schema
        self.save()


    def drop_table(self, table_name: str) -> None:
        if table_name in self.tables_data:
            del self.tables_data[table_name]

        file_path = os.path.join(self.data_dir, f"{table_name}.json")
        if os.path.exists(file_path):
            os.remove(file_path)

        if self.catalog.table_exists(table_name):
            self.catalog.drop_table(table_name)


    def table_exists(self, table_name: str) -> bool:
        return table_name in self.tables

    def get_table(self, table_name: str) -> "TableSchema":
        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' does not exist.")
        return self.tables[table_name]

    # ---- validation ----
    def validate_value(self, table_name: str, column_name: str, value: Any) -> None:
        table = self.get_table(table_name)

        if column_name not in table.columns:
            raise ColumnNotFoundError(f"Column '{column_name}' does not exist in table '{table_name}'.")

        expected_type = SUPPORTED_TYPES[table.columns[column_name]]
        if not isinstance(value, expected_type):
            raise SchemaError(
                f"Invalid type for column '{column_name}'. "
                f"Expected {table.columns[column_name]}, got {type(value).__name__}."
            )
