# db/engine.py

from db.catalog import Catalog
from db.storage import Storage
from db.parser import Parser, ParseError
from db.executor import Executor

class DatabaseEngine:
    def __init__(self, data_dir: str = "data"):
        import os
        os.makedirs(data_dir, exist_ok=True)

        catalog_path = os.path.join(data_dir, "catalog.json")
        self.catalog = Catalog(catalog_path)
        self.storage = Storage(self.catalog, data_dir)
        self.parser = Parser()
        self.executor = Executor(self.catalog, self.storage)

    def execute_sql(self, sql: str):
        try:
            parsed = self.parser.parse(sql)
            result = self.executor.execute(parsed)

            return {
                "status": "ok",
                "command": parsed["type"],
                "result": result
            }

        except ParseError as e:
            return {
                "status": "error",
                "error_type": "parse",
                "message": str(e)
            }
        except Exception as e:
            return {
                "status": "error",
                "error_type": "execution",
                "message": str(e)
            }
