
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
        
    def sql_create(self) -> str:
        """ Generate a CREATE TABLE statement for schema

        Returns:
            str: Returns a string that is an SQL create table command
        """
        create_table = [
            f"CREATE TABLE {self.name} ("
        ]

        for col in self.columns:
            create_table.append(f"    {col.to_sql()},")

        create_table[-1] = create_table[-1].rstrip(",")
        create_table.append(");")

        return "\n".join(create_table)
    
    def sql_add_column(self, column: Column):
        """Generate SQL to alter an existing table

        Args:
            column (Column): The column to be added

        Raises:
            ValueError: If column already exists

        Returns:
            str: A string SQL command to alter table
        """
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
    
    def get_id(self, conn, column_name: str, column_value):
        if column_name not in {c.name for c in self.columns}:
            raise ValueError(f"Unknown column: {column_name}")

        sql = f"SELECT id FROM {self.name} WHERE {column_name} = ?;"
        cur = conn.execute(sql, (column_value,))
        row = cur.fetchone()

        if row is None:
            return None

        return row[0]
    
    def get_or_create(self, conn, column_name: str, value):
        existing_id = self.get_id(conn, column_name, value)
        if existing_id is not None:
            return existing_id

        sql, params = self.sql_insert({column_name: value})
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid
    
    def get_column_from_table(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column
        raise ValueError(f"Column '{name}' not found in table '{table.name}'")
    

def format_data(data: dict) -> dict:
    formatted = {}

    for key, value in data.items():
        if value is not None:
            formatted[key] = value

    return formatted

class MTM:
    def __init__(
        self,
        join_table: Table,
        left_fk: str,
        right_fk: str,
    ):
        self.join_table = join_table
        self.left_fk = left_fk
        self.right_fk = right_fk
        
    def insert_many(
        self,
        conn,
        left_ids: list[int],
        right_ids: list[int],
    ):
        cur = conn.cursor()

        for left_id in left_ids:
            for right_id in right_ids:
                sql, params = self.join_table.sql_insert({
                    self.left_fk: left_id,
                    self.right_fk: right_id,
                })
                cur.execute(sql, params)

        conn.commit()               