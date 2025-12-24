from SQL.schema.schema import Table, Column, format_data

def get_value(conn, table: Table, column_name: str, row_id):
    if column_name not in {c.name for c in table.columns}:
        raise ValueError(f"Unknown column: {column_name}")

    sql = f"SELECT {column_name} FROM {table.name} WHERE id = ?;"
    cur = conn.execute(sql, (row_id,))
    row = cur.fetchone()

    if row is None:
        return None

    return row[0]

def get_id(table, conn, column_name: str, column_value):
    if column_name not in {c.name for c in table.columns}:
        raise ValueError(f"Unknown column: {column_name}")

    sql = f"SELECT id FROM {table.name} WHERE {column_name} = ?;"
    cur = conn.execute(sql, (column_value,))
    row = cur.fetchone()

    if row is None:
        return None

    return row[0]

def get_or_create(table, conn, column_name: str, value):
    existing_id = get_id(table, conn, column_name, value)
    if existing_id is not None:
        return existing_id
    
    sql, params = table.sql_insert({column_name: value})
    cur = conn.execute(sql, params)
    conn.commit()
    return cur.lastrowid
