from SQL_Schema import Table, Column, format_data

#db = Database("test.db")
#
#categories = Table("Categories")
#
#categories.add_column(
#    Column(
#        name="id",
#        type="INTEGER",
#        primary=True,
#        autoincrement=True
#    )
#)
#
#categories.add_column(
#    Column(
#        name="name",
#        type="TEXT",
#        not_null=True,
#        unique=True
#    )
#)
#
#db.execute(categories.create_sql())
#
#sql, params = categories.insert_sql({
#    "name": "sci-fi"
#})
#
#books = Table("Books")
#
#books.add_column(
#    Column("id", "INTEGER", primary=True, autoincrement=True)
#)
#
#books.add_column(
#    Column("title", "TEXT", not_null=True)
#)
#
#books.add_column(
#    Column("category_id", "INTEGER", not_null=True)
#)
#
#books.add_foreign_key(
#    ForeignKey("category_id", "Categories", "id")
#)
#
#db.execute(books.create_sql())
#
#category_id = db.get_or_create(
#    table="Categories",
#    key_column="name",
#    value="sci-fi"
#)
#
#sql, params = books.insert_sql({
#    "title": "Dune",
#    "category_id": category_id
#})
#
#db.execute(sql, params)

column = Column(
    name = "Test",
    type = "INTEGER",
    autoincrement=True
)

column2 = Column(
    name="Test2",
    type="TEXT",
    not_null=True
)

columns = [
    column,
    column2
]

print(column.to_sql())

table = Table(
    name="TestTable",
    columns=[column, column2]
)

print(table.sql_create())

for p in table.columns:
    print(p.name)
    
raw = {
    column.name: "First Column",
    column2.name: "Second Column"
}

column3 = Column(
    name="Test3",
    type="TEXT",
    not_null=True
)

raw[column3.name] = "Third Column"

data = format_data(raw)

print(f"{data}")

sql, params = table.sql_insert(data)
#cursor.execute(sql, params)

print(f"{sql}")
print(f"{params}")