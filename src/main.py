from SQL.schema.schema import Table, Column, format_data
import sqlite3
import Parser.parser as pars
import SQL.Query.query as que

schema = pars.retrieve_schema("data/init.json")

#print(schema)

tTags = pars.get_table_from_json(schema, "Tags")
#print(tags.sql_create())

tBooks = pars.get_table_from_json(schema, "Books")
#print(books.sql_create())

cAuthor_column = tBooks.get_column_from_table("author")
print(cAuthor_column.name, "column name")
print(cAuthor_column.type, "column type")

tBook_tags = pars.get_table_from_json(schema, "BookTags")
#print(book_tags.sql_create())

conn = sqlite3.connect(":memory:")
cur = conn.cursor()

cur.execute(tBooks.sql_create())
cur.execute(tTags.sql_create())
cur.execute(tBook_tags.sql_create())

conn.commit()

eAlice = format_data({
    "name": "Alice in Wonderland",
    "author": "Lewis Carroll",
    "publish_date": "1865",
    "category": "Fantasy"
})

print(eAlice["name"], "Entry name")

sql, params = tBooks.sql_insert(eAlice)
cur.execute(sql, params)
book_id = cur.lastrowid

Alice_id = que.get_id(tBooks, conn, "name", "Alice in Wonderland")
print(Alice_id)

tag_names = ["youth", "adventure", "mystical"]
tag_ids = []

for tag in tag_names:
    tag_id = que.get_or_create(tTags, conn, "name", tag)
    tag_ids.append(tag_id)
    
for tag_id in tag_ids:
    sql, params = tBook_tags.sql_insert({
        "book_id": book_id,
        "tag_id": tag_id
    })
    cur.execute(sql, params)

conn.commit()

print(que.get_value(conn=conn,
                    table=tBooks,
                    column_name="name",
                    row_id=Alice_id),)

print(que.get_value(conn=conn,
                    table=tBooks,
                    column_name="category",
                    row_id=Alice_id),)

cur.execute("""
    SELECT Tags.name
    FROM Tags
    JOIN BookTags ON Tags.id = BookTags.tag_id
    WHERE BookTags.book_id = ?
""", (book_id,))

print([row[0] for row in cur.fetchall()])