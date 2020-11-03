import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv('./dev.env')

HOST = os.getenv("POSTGRESQL_HOST")
USER = os.getenv("POSTGRESQL_USER")
PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
DATABASE = os.getenv("POSTGRESQL_DATABASE")

# Format to list columns for an INSERT
def FormatInsert(columns):
    listColumns = "("
    for index in range(len(columns)-1):
        listColumns += columns[index]+","
    listColumns += columns[-1]+")"
    return listColumns

# Format the columns for the update exception of the PostgreSQL upsert
# See PostgreSQL upsert syntax to understand
def FormatUpdate(columns):
    listColumns = ""
    for index in range(len(columns)-1):
        listColumns += columns[index]+" = EXCLUDED."+ columns[index]+", "
    listColumns += columns[-1]+" = EXCLUDED."+ columns[-1]
    return listColumns

# PostgreSQL upsert : insert a new row into the table, PostgreSQL will update the row if it already exists
def InsertOrUpdate(tableName, columns, values):
    valuesTuples = [tuple(value) for value in values] # Convert to an array of tuple for batch import

    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    # Open a cursor to send SQL commands
    cur = conn.cursor()

    try:
        psycopg2.extras.execute_batch(cur, "INSERT INTO "+tableName+" "+FormatInsert(columns)+" VALUES ("+"%s,"*(len(columns)-1)+"%s) ON CONFLICT ON CONSTRAINT id DO UPDATE SET "+FormatUpdate(columns)+";", valuesTuples)
        conn.commit()
    except error:
        print(error)

    # Close connection
    conn.close()

def Select(query):

    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    # Open a cursor to send SQL commands
    cur = conn.cursor()

    res = []
    try:
        cur.execute(query)
        conn.commit()
        res = cur.fetchall()
    except Exception as error:
        print(error)

    # Close connection
    conn.close()
    return res