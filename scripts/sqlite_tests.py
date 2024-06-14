import sqlite3
import pandas as pd

conn = sqlite3.connect("res_database.db")
cur = conn.cursor()

cur.execute("""
            DROP TABLE projects
; """)

cur.execute("""
            CREATE TABLE projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            name text NOT NULL, 
            begin_date TEXT, end_date TEXT
); """)


cur.execute("INSERT INTO projects (name, begin_date, end_date) values ('test','2029-01-01','2029-01-02')")

cur.execute('select * from projects')
rows = cur.fetchall()
## committing data to sqlite database
conn.commit()

## Read straight to PD
out = pd.read_sql('select * from projects', conn)
out = pd.concat([out,out])

## Writing to db
out.drop(columns='id').to_sql('projects', con = conn, if_exists='append', index=False)

# Reading from db again
out = pd.read_sql('select * from projects', conn)
out = pd.concat([out,out])

## Writing to db
conn.commit()

## Grabbing all of the results from the last query 
rows = cur.fetchall()

## grabbing schema data
cur.execute('select * from sqlite_schema')
rows = cur.fetchall()

cur.execute("""
            DROP TABLE projects
; """)

conn.close()
