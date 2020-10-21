import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('./dev.env')

HOST = os.getenv("POSTGRESQL_HOST")
USER = os.getenv("POSTGRESQL_USER")
PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
DATABASE = os.getenv("POSTGRESQL_DATABASE")

def Insert():
    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))

    # Open a cursor to send SQL commands
    cur = conn.cursor()

    # Execute a SQL INSERT command
    sql = "INSERT INTO test VALUES ('Hello World',1)"
    cur.execute(sql)

    # Commit (transactionnal mode is by default)
    conn.commit()

    # Testing
    sql = "SELECT * FROM test"
    cur.execute(sql)
    print(cur.fetchall())

    # Close connection
    conn.close()

def Select():
    # Open connection
    conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (HOST, DATABASE, USER, PASSWORD))

    # Open a cursor to send SQL commands
    cur = conn.cursor()

    # Execute a SQL SELECT command
    sql = "SELECT * FROM test"
    cur.execute(sql)

    # Fetch data line by line
    raw = cur.fetchone()
    while raw:
        print (raw[0])
        print (raw[1])
        raw = cur.fetchone()

    # Close connection
    conn.close()

Select()