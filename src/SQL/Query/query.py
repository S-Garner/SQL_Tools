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
