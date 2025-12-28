import sqlite3

from SQL.schema.schema import Table, Column, MTM, format_data
import Parser.parser as pars
import SQL.Query.query as que

schema = pars.retrieve_schema("data/bookmarks.json")

tBookmarks = pars.get_table_from_json(schema, "Bookmarks")
tTags = pars.get_table_from_json(schema, "Tags")
tBookMarks_Tags = pars.get_table_from_json(schema, "Bookmarks_Tags")

conn = sqlite3.connect(":memory:")
cur = conn.cursor()

cur.execute(tBookmarks.sql_create_table())
cur.execute(tTags.sql_create_table())
cur.execute(tBookMarks_Tags.sql_create_table())

conn.commit()

cBookmark_fk = tBookMarks_Tags.get_column_from_table("BOOKMARK_ID")
cTag_fk = tBookMarks_Tags.get_column_from_table("TAG_ID")

mBook_Tags = MTM(
    join_table=tBookMarks_Tags,
    left_fk=cBookmark_fk.name,
    right_fk=cTag_fk.name
)

"""format Types:
    Article,
    Video,
    Image,
    Comic,
    Audio,
    Book,
    Paper,
    Presentation,
    Dataset,
    Code,
    Game,
    Tool,
    Page
"""

eTest = format_data({
    
})

eEntry = format_data({
    "NAME":         "Python for Beginners",
    "URL":          "https://www.python.org/about/gettingstarted/",
    "FORMAT":       "article",
    "AUTHOR":       "N/A",
    "CATEGORY":     "Programming",
    "DESCRIPTION":  "The official documentation for beginners",
    "NOTE":         "This is very helpful for beginning, and returning python programmers"
})

eEntry2 = format_data({
    "NAME":         "Python download page",
    "URL":          "https://www.python.org/downloads/",
    "FORMAT":       "page",
    "AUTHOR":       "N/A",
    "CATEGORY":     "Downloads",
    "DESCRIPTION":  "The official download page for the Python foundation",
    "NOTE":         "Useful for getting the downloads for python"
})

eEntry_id = que.insert_entry(cur, tBookmarks, eEntry)
eEntry_id2 = que.insert_entry(cur, tBookmarks, eEntry2)

eTags = [
         "python", 
         "programming", 
         "educational",
         "beginner_friendly",
         "reference"]

eTags2 = [
    "python",
    "programming",
    "downloads",
]

eTags_ids = []
eTags_ids2 = []


for tag in eTags:
    tag_id = que.get_or_create(tTags, conn, "NAME", tag)
    eTags_ids.append(tag_id)
    
for tag in eTags2:
    tag_id = que.get_or_create(tTags, conn, "NAME", tag)
    eTags_ids2.append(tag_id)
    
python_tag_id = que.get_or_create(tTags, conn, "NAME", "python")
print(python_tag_id, "This is the tag ID")

mBook_Tags.insert_many(
    conn,
    left_ids=[eEntry_id],
    right_ids=eTags_ids
)

mBook_Tags.insert_many(
    conn,
    left_ids=[eEntry_id2],
    right_ids=eTags_ids2
)

tags_table = tTags.name
print(f"This is tTags.name", tags_table)
join_table = tBookMarks_Tags.name

tag_name_col = tTags.get_column_from_table("NAME").name
tag_id_col   = tTags.get_column_from_table("ID").name

join_tag_fk  = tBookMarks_Tags.get_column_from_table("TAG_ID").name
join_book_fk = tBookMarks_Tags.get_column_from_table("BOOKMARK_ID").name

#sql = f"""
#    SELECT {tags_table}.{tag_name_col}
#    FROM {tags_table}
#    JOIN {join_table}
#    ON {tags_table}.{tag_id_col} = {join_table}.{join_tag_fk}
#    WHERE {join_table}.{join_book_fk} = ?
#    """
#
#cur.execute(sql, (eEntry_id,))

sql = f"""
    SELECT {tags_table}.name
    
"""

#cur.execute("""
#    SELECT Tags.name
#    FROM Tags
#    JOIN Bookmarks_Tags ON Tags.id = Bookmarks_Tags.tag_id
#    WHERE Bookmarks_Tags."BOOKMARK_ID" = ?
#    """, (eEntry_id,))

cur.execute("""
    SELECT Bookmarks.NAME, Bookmarks.URL, Bookmarks.CREATE_DATE
    FROM Bookmarks
    JOIN Bookmarks_Tags
      ON Bookmarks.id = Bookmarks_Tags.BOOKMARK_ID
    WHERE Bookmarks_Tags.TAG_ID = ?
""", (python_tag_id,))

for row in cur.fetchall():
    print(row)
    
cur.execute("""
            SELECT Bookmarks.NAME
            FROM Bookmarks
            """)

for row in cur.fetchall():
    print(row)

#print([row[0] for row in cur.fetchall()])