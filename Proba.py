import random
import numpy as np
import psycopg2
import DatabaseConnector as dbc
import datetime
from dotenv import load_dotenv

load_dotenv('./dev.env')

HOST = os.getenv("POSTGRESQL_HOST")
USER = os.getenv("POSTGRESQL_USER")
PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
DATABASE = os.getenv("POSTGRESQL_DATABASE")



"""
This function calculates the number of days between start_date and end_date
Provides the same result as the column num_day
"""
#def SizeWindow(d1, d2):
#    d1 = datetime.strptime(d1, "%Y-%m-%d")
#    d2 = datetime.strptime(d2, "%Y-%m-%d")
#    return abs((d2 - d1).days)


"""
This function goes through the whole calendar to add probability
Function we used : https://www.geogebra.org/calculator/rch5ctnn
"""
def AddingProba(calendar):  
    for index, row in calendar.iterrows():
        tempProba = 0
        if row["label"] != "A" or row["label"] != "MIN" or row["num_day"] < 200:
            tempProba = 935/(142*np.sqrt(2*np.pi))*np.exp(-0.5*np.power((row["num_day"]+202)/142,2))
            tempProba = AdjustProba(row, tempProba)     
        row['proba'] = tempProba  
    #calendar['Proba'] = [ random.randint(0,100)/100  for k in calendar.index]
    return calendar


"""
This function adjusts the probability of one row based on the validation from Booking.com's reviews
"""
def AdjustProba(row, tempProba):
    resultQuery = dbc.Execute("SELECT * FROM listings WHERE id = '" + row['listing_id'] + "' and last_review BETWEEN '" + row['end_date'] + "' and '" + GetMaxReviewDate(row['end_date']) + "'")
    #test = dbc.Execute("SELECT * FROM listings WHERE id = '2577' and last_review BETWEEN '2019-02-02' and '" + GetMaxReviewDate("2019-02-02") + "'")
    if (len(resultQuery) > 0):
        adjustedProba = tempProba + (1-tempProba)*0.5
    return adjustedProba


"""
We consider that the last period, if a closed one, has a high probability of being closed voluntarily by the land lord
Therefore, we set it at 0 aswell
08/11/2020 : This rule is considered an optional rule
"""
def FindLastPeriodClosed():
    resultQuery = dbc.Execute("SELECT distinct on (listing_id) end_date, listing_id FROM calendars WHERE label != 'A' ORDER BY listing_id, end_date DESC")
    for row in resultQuery:
        dbc.Execute("UPDATE calendars SET proba = 0 WHERE end_date = " + row[0] + " AND listing_id = " + row[1])


"""
A customer can review a rent max 15 days after it ended
This function takes a date and return it +15 days.
"""
def GetMaxReviewDate(leftScale):
    leftScale = datetime.datetime.strptime(leftScale, "%Y-%m-%d")
    rightScale = leftScale + datetime.timedelta(days=365)
    return datetime.datetime.strftime(rightScale, "%Y-%m-%d")



#AddingProba(calendar)
#AdjustProba(row, tempProba)
#FindLastPeriodClosed()
#GetMaxReviewDate("2020-02-05")