import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv('./dev.env')

HOST = os.getenv("POSTGRESQL_HOST")
USER = os.getenv("POSTGRESQL_USER")
PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
DATABASE = os.getenv("POSTGRESQL_DATABASE")


'''
Format to list columns for an INSERT
'''
def FormatInsert(columns):
    listColumns = ""
    for index in range(len(columns)-1):
        listColumns += columns[index]+","
    listColumns += columns[-1]
    return listColumns


'''
Format the columns for the update exception of the PostgreSQL upsert
See PostgreSQL upsert syntax to understand
'''
def FormatExcludeUpdate(columns):
    listColumns = ""
    for index in range(len(columns)-1):
        listColumns += columns[index]+" = EXCLUDED."+ columns[index]+", "
    listColumns += columns[-1]+" = EXCLUDED."+ columns[-1]
    return listColumns

'''
Format the columns for the update 
See PostgreSQL upsert syntax to understand
'''
def FormatUpdate(columns):
    listColumns = ""
    for index in range(len(columns)-1):
        listColumns += columns[index]+" = %s, "
    listColumns += columns[-1]+" = %s"
    return listColumns

'''
PostgreSQL upsert : Insert a new row into the table, PostgreSQL will update the row if it already exists
'''
def ExecQueryBatch(query, params_list):

    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))
    cur = conn.cursor()

    try:
        psycopg2.extras.execute_batch(cur, query, params_list)
        conn.commit()
    except Exception as error:
        print(error)

    conn.close()

# PostgreSQL upsert : insert a new row into the table, PostgreSQL will update the row if it already exists
def InsertOrUpdate(tableName, columns, values):
    query = "INSERT INTO "+tableName+" ("+FormatInsert(columns)+") VALUES ("+"%s,"*(len(columns)-1)+"%s) ON CONFLICT ON CONSTRAINT id DO UPDATE SET "+FormatExcludeUpdate(columns)+";"
    params_list = [tuple(value) for value in values]
    ExecQueryBatch(query, values)

def CalendarDelete(data):
    query_to_delete = "DELETE FROM calendars WHERE cal_key = %s"
    ExecQueryBatch(query_to_delete, data)

def Insert(data,tableName,columns):
    query_to_insert = "INSERT INTO "+tableName+" ("+FormatInsert(columns)+") VALUES ("+"%s,"*(len(columns)-1)+"%s);"
    params_list = [tuple(value) for value in data]
    ExecQueryBatch(query_to_insert, params_list)

def UpdateValidation(df):
    df = df[['validation','proba','ext_validation','cal_key']]
    query_to_update = "UPDATE calendars SET validation=%s, proba=%s, ext_validation=%s WHERE cal_key = %s"
    params_list = [tuple(value) for value in df.values.tolist()]
    ExecQueryBatch(query_to_update, params_list)

'''
Get the result of an execute query to the database
'''
def Execute(query):

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

if __name__ == "__main__":
    print(FormatUpdate(["validation","ext_validation"]))