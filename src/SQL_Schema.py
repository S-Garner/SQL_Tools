        
import sqlite3

class Column:
    def __init__(
        self,
        name,
        type,
        primary=False,
        unique=False,
        not_null=False,
        autoincrement=False,
        default=None
    ):
        self.name = name
        self.type = type
        self.primary = primary
        self.unique = unique
        self.not_null = not_null
        self.autoincrement = autoincrement
        self.default = default

    def to_sql(self):
        parts = [self.name, self.type]

        if self.primary:
            parts.append("PRIMARY KEY")
        if self.unique:
            parts.append("UNIQUE")
        if self.not_null:
            parts.append("NOT NULL")
        if self.autoincrement:
            parts.append("AUTOINCREMENT")
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")

        return " ".join(parts)

class Table:
    def __init__(
        self,
        name,
        columns: Column
    ):
        self.name = name
        self.columns = columns
        
    def sql_create(self):
        create_table = [
            f"CREATE TABLE {self.name} ("
        ]

        for col in self.columns:
            create_table.append(f"    {col.to_sql()},")

        create_table[-1] = create_table[-1].rstrip(",")
        create_table.append(");")

        return "\n".join(create_table)
    
    def sql_add_column(self, column: Column):
        if column.name in {c.name for c in self.columns}:
            raise ValueError("Column already exists")

        self.columns.append(column)
        return f"ALTER TABLE {self.name} ADD COLUMN {column.to_sql()};"
    
    def sql_insert(self, values: dict):
        columns = []
        placeholders = []
        params = []

        for col in self.columns:
            if col.name in values:
                columns.append(col.name)
                placeholders.append("?")
                params.append(values[col.name])

        sql = (
            f"INSERT INTO {self.name} "
            f"({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)});"
        )

        return sql, params
    

def format_data(data: dict):
    return {k: v for k, v in data.items() if v is not None}
