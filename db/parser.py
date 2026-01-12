# db/parser.py

from typing import Dict, Any, List
import re

class ParseError(Exception):
    pass

class Parser:
    def __init__(self):
        pass

    def parse(self, query: str) -> Dict[str, Any]:
        # Normalize whitespace and remove trailing semicolon
        query = query.strip()
        if query.endswith(";"):
            query = query.rstrip(";").strip()

        upper_query = query.upper()
        if upper_query.startswith("CREATE TABLE"):
            return self.parse_create_table(query)
        elif upper_query.startswith("INSERT INTO"):
            return self.parse_insert(query)
        elif upper_query.startswith("SELECT"):
            return self.parse_select(query)
        elif upper_query.startswith("UPDATE"):
            return self.parse_update(query)
        elif upper_query.startswith("DELETE FROM"):
            return self.parse_delete(query)
        elif upper_query.startswith("DROP TABLE"):
            return self.parse_drop_table(query)
        else:
            raise ParseError("Unsupported query type.")

    # -------------------------
    # CREATE TABLE
    # -------------------------
    def parse_create_table(self, query: str) -> Dict[str, Any]:
        pattern = r"CREATE TABLE\s+(\w+)\s*\((.+)\)"
        match = re.match(pattern, query.strip(), re.IGNORECASE | re.DOTALL)
        if not match:
            raise ParseError("Invalid CREATE TABLE syntax.")

        table_name = match.group(1)
        columns_part = match.group(2)

        columns = {}
        primary_key = None
        unique_columns = []

        # Split on commas outside parentheses
        column_defs = [c.strip() for c in re.split(r',\s*(?![^\(]*\))', columns_part)]

        for col_def in column_defs:
            parts = col_def.split()
            if len(parts) < 2:
                raise ParseError(f"Invalid column definition: {col_def}")

            col_name = parts[0]
            col_type = parts[1].upper()
            is_primary = any(p.upper() == "PRIMARY" for p in parts)
            is_unique = any(p.upper() == "UNIQUE" for p in parts)

            columns[col_name] = col_type

            if is_primary:
                if primary_key is not None:
                    raise ParseError("Multiple primary keys are not allowed.")
                primary_key = col_name
            if is_unique:
                unique_columns.append(col_name)

        if primary_key is None:
            raise ParseError("No primary key defined in CREATE TABLE.")

        if primary_key not in unique_columns:
            unique_columns.append(primary_key)

        return {
            "type": "CREATE_TABLE",
            "table_name": table_name,
            "columns": columns,
            "primary_key": primary_key,
            "unique_columns": unique_columns
        }

    # -------------------------
    # INSERT
    # -------------------------
    def parse_insert(self, query: str) -> Dict[str, Any]:
        pattern = r"INSERT INTO\s+(\w+)\s+VALUES\s*\((.+)\)"
        match = re.match(pattern, query.strip(), re.IGNORECASE | re.DOTALL)
        if not match:
            raise ParseError("Invalid INSERT INTO syntax.")

        table_name = match.group(1)
        values_part = match.group(2).strip()

        # Split values by comma outside quotes
        raw_values = [v.strip() for v in re.split(r",(?![^']*')", values_part)]
        values: List[Any] = [self._parse_value(v) for v in raw_values]

        return {
            "type": "INSERT",
            "table_name": table_name,
            "values": values
        }

    # -------------------------
    # SELECT
    # -------------------------
    def parse_select(self, query: str) -> Dict[str, Any]:
        pattern = r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?"
        match = re.match(pattern, query.strip(), re.IGNORECASE | re.DOTALL)
        if not match:
            raise ParseError("Invalid SELECT syntax.")

        columns_part = match.group(1).strip()
        table_name = match.group(2)
        where_part = match.group(3)

        columns = ["*"] if columns_part == "*" else [col.strip() for col in columns_part.split(",")]
        where = self._parse_where(where_part) if where_part else {}

        return {
            "type": "SELECT",
            "table_name": table_name,
            "columns": columns,
            "where": where
        }

    # -------------------------
    # UPDATE
    # -------------------------
    def parse_update(self, query: str) -> Dict[str, Any]:
        pattern = r"UPDATE\s+(\w+)\s+SET\s+(.+?)\s+WHERE\s+(.+)"
        match = re.match(pattern, query.strip(), re.IGNORECASE | re.DOTALL)
        if not match:
            raise ParseError("Invalid UPDATE syntax.")

        table_name = match.group(1)
        set_part = match.group(2).strip()
        where_part = match.group(3).strip()

        updates = {}
        for item in [i.strip() for i in set_part.split(",")]:
            set_match = re.match(r"(\w+)\s*=\s*(.+)", item)
            if not set_match:
                raise ParseError(f"Invalid SET expression: '{item}'")
            col, val = set_match.group(1), set_match.group(2).strip()
            updates[col] = self._parse_value(val)

        where = self._parse_where(where_part)
        return {
            "type": "UPDATE",
            "table_name": table_name,
            "updates": updates,
            "where": where
        }

    # -------------------------
    # DELETE
    # -------------------------
    def parse_delete(self, query: str) -> Dict[str, Any]:
        pattern = r"DELETE FROM\s+(\w+)\s+WHERE\s+(.+)"
        match = re.match(pattern, query.strip(), re.IGNORECASE | re.DOTALL)
        if not match:
            raise ParseError("Invalid DELETE syntax.")

        table_name = match.group(1)
        where_part = match.group(2).strip()
        where = self._parse_where(where_part)

        return {
            "type": "DELETE",
            "table_name": table_name,
            "where": where
        }

    # -------------------------
    # DROP TABLE
    # -------------------------
    def parse_drop_table(self, query: str) -> Dict[str, Any]:
        pattern = r"DROP TABLE\s+(\w+)"
        match = re.match(pattern, query.strip(), re.IGNORECASE)
        if not match:
            raise ParseError("Invalid DROP TABLE syntax.")

        table_name = match.group(1)
        return {
            "type": "DROP_TABLE",
            "table_name": table_name
        }

    # -------------------------
    # Helpers
    # -------------------------
    def _parse_where(self, where_part: str) -> Dict[str, Any]:
        if where_part is None:
            return {}
        where_part = where_part.strip().rstrip(";")
        match = re.match(r"(\w+)\s*=\s*(.+)", where_part)
        if not match:
            raise ParseError("Unsupported WHERE clause. Only 'col = value' is supported.")
        col, val = match.group(1), match.group(2).strip()
        return {col: self._parse_value(val)}

    def _parse_value(self, val: str) -> Any:
        val = val.strip().rstrip(";")
        if re.match(r"^'.*'$", val):
            return val[1:-1]
        elif val.upper() == "TRUE":
            return True
        elif val.upper() == "FALSE":
            return False
        elif re.match(r"^-?\d+$", val):
            return int(val)
        else:
            raise ParseError(f"Cannot parse value: {val}")
