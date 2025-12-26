from SQL.schema.schema import Table, Column, format_data, MTM
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

cBook_id = tBooks.get_column_from_table("id")
print(cBook_id.name, "column name")
print(cBook_id.type, "column type")

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

cBook_fk = tBook_tags.get_column_from_table("book_id")
cTag_fk = tBook_tags.get_column_from_table("tag_id")

mtm_book_tags = MTM(
    join_table=tBook_tags,
    left_fk=cBook_fk.name,
    right_fk=cTag_fk.name
)

mtm_book_tags.insert_many(
    conn,
    left_ids=[book_id],
    right_ids=tag_ids
)

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