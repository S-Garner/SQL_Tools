from SQL.schema.schema import Table, Column, format_data
import sqlite3
import Parser.parser as pars

schema = pars.retrieve_schema("data/init.json")

#print(schema)

tags = pars.get_table_from_json(schema, "Tags")
#print(tags.sql_create())

books = pars.get_table_from_json(schema, "Books")
#print(books.sql_create())

book_tags = pars.get_table_from_json(schema, "BookTags")
#print(book_tags.sql_create())

conn = sqlite3.connect(":memory:")
cur = conn.cursor()

cur.execute(books.sql_create())
cur.execute(tags.sql_create())
cur.execute(book_tags.sql_create())

conn.commit()

book_data = format_data({
    "name": "Alice in Wonderland",
    "author": "Lewis Carroll",
    "publish_date": "1865",
    "category": "Fantasy"
})

sql, params = books.sql_insert(book_data)
cur.execute(sql, params)
book_id = cur.lastrowid

Alice_id = books.get_id(conn, "name", "Alice in Wonderland")
print(Alice_id)

tag_names = ["youth", "adventure", "mystical"]
tag_ids = []

for tag in tag_names:
    tag_id = tags.get_or_create(conn, "name", tag)
    tag_ids.append(tag_id)
    
for tag_id in tag_ids:
    sql, params = book_tags.sql_insert({
        "book_id": book_id,
        "tag_id": tag_id
    })
    cur.execute(sql, params)

conn.commit()

cur.execute("""
    SELECT Tags.name
    FROM Tags
    JOIN BookTags ON Tags.id = BookTags.tag_id
    WHERE BookTags.book_id = ?
""", (book_id,))

print([row[0] for row in cur.fetchall()])