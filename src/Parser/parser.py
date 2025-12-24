import json
from SQL.schema.schema import Table, Column, format_data

def retrieve_schema(file):
    with open(file, "r") as f:
        schema = json.load(f)
        
    return schema

def table_from_json(schema: dict):
    columns = []
    
    for col_def in schema["columns"]:
        columns.append(Column(**col_def))
        
    return Table(
        name=schema["name"],
        columns=columns
    )
    
def tables_from_json(schema):
    tables = []

    for table_def in schema["tables"]:
        table = table_from_json(table_def)
        tables.append(table)

    return tables

def get_table_from_json(schema: dict, name: str) -> Table:
    tables = tables_from_json(schema)

    for table in tables:
        if table.name == name:
            return table

    raise ValueError(f"Table '{name}' not found in schema")
