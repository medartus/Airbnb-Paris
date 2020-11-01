import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv('./dev.env')

HOST = os.getenv("POSTGRESQL_HOST")
USER = os.getenv("POSTGRESQL_USER")
PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
DATABASE = os.getenv("POSTGRESQL_DATABASE")

# def Insert():
#     # Open connection
#     conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))

#     # Open a cursor to send SQL commands
#     cur = conn.cursor()

#     # Execute a SQL INSERT command
#     sql = "INSERT INTO test VALUES ('Hello World',1)"
#     cur.execute(sql)

#     # Commit (transactionnal mode is by default)
#     conn.commit()

#     # Testing
#     sql = "SELECT * FROM test"
#     cur.execute(sql)
#     print(cur.fetchall())

#     # Close connection
#     conn.close()

# def Select():
#     # Open connection
#     conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))

#     # Open a cursor to send SQL commands
#     cur = conn.cursor()

#     # Execute a SQL SELECT command
#     sql = "SELECT * FROM test"
#     cur.execute(sql)

#     # Fetch data line by line
#     raw = cur.fetchone()
#     while raw:
#         print (raw[0])
#         print (raw[1])
#         raw = cur.fetchone()
    
#     # Close connection
#     conn.close()

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

def ExecQueryBatch(query, values):
    params_list = [tuple(value) for value in values]

    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    cur = conn.cursor()

    try:
        execute_batch(cur, query, params_list)
        conn.commit()
    except Exception as error:
        print(error)

    conn.close()

# PostgreSQL upsert : insert a new row into the table, PostgreSQL will update the row if it already exists
def InsertOrUpdate(tableName, columns, values):
    query = "INSERT INTO "+tableName+" "+FormatInsert(columns)+" VALUES ("+"%s,"*(len(columns)-1)+"%s) ON CONFLICT ON CONSTRAINT id DO UPDATE SET "+FormatUpdate(columns)+";"
    ExecQueryBatch(query, values)

def CalendarUpdaterDeleteLines(data):
    query_to_delete = "DELETE FROM calendars WHERE listing_id = %s, start_date = %s, end_date = %s"
    ExecQueryBatch(query_to_delete, data)

def CalendarUpdaterInsertLines(data):
    size = len(data[0])
    columns = ["listing_id","available","start_date","end_date","num_day","minimum_nights","maximum_nights","label","validation"]
    query_to_insert = "INSERT INTO calendars "+FormatInsert(columns)+" VALUES ("+"%s,"*(size-1)+"%s);"
    ExecQueryBatch(query_to_insert, data)


