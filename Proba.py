import random
import numpy as np
import psycopg2
import DatabaseConnector as dbc
import datetime


"""
This function goes through the whole calendar to add probability.
Function we used : https://www.geogebra.org/calculator/rch5ctnn
"""
def AddingProba(calendar, listing):
    calendar = calendar.sort_values(["listing_id","start"])         #To be sure that the calendar is sorted
    for index, row in calendar.iterrows():
        tempProba = 0
        if row["label"] != "A" or row["label"] != "MIN" or row["num_day"] < "MAX":
            tempProba = 935/(142*np.sqrt(2*np.pi))*np.exp(-0.5*np.power((row["num_day"]+202)/142,2))
            tempProba = AdjustProbaByValidation(row, tempProba) 
            tempProba = AdjustProbaInstantBooking(row, listing, tempProba)
        if CheckIfLastPeriodIsClosed(calendar, index) == True:
            tempProba = AdjustProbaLastPeriod(tempProba)
        row['proba'] = tempProba  
    return calendar


"""
This function adjusts the probability of one row based on the validation from Airbnb's reviews.
"""
def AdjustProbaByValidation(row, tempProba):
    adjustedProba = tempProba
    if (row["validation"] == True):
        adjustedProba += (1-tempProba)*0.5              #The formula is likely to change
    return adjustedProba


"""
This function checks if the reservation is available for instant booking.
"""
def AdjustProbaInstantBooking(row, listing, tempProba):
    adjustedProba = tempProba
    index = listing["instant_bookable"]
    if index[row["listing_id"]] == 't':
        adjustedProba += (1-tempProba)*0.3              #The formula is likely to change
    return adjustedProba


"""
If the reservation is available for instant booking, the probabilty increases.
"""
def AdjustProbaLastPeriod(tempProba):
    return tempProba*0.15                               #The formula is likely to change


"""
We consider that the last period of the calendar, if closed, is likely to be closed by the landlord.
As the calendar is sorted by listing_id and start_date, we can assume the last period for each listing_id is the last period with that ID.
"""
def CheckIfLastPeriodIsClosed(calendar, index):
    check = False
    if calendar[index]['listing_id'] != calendar[index+1]['listing_id']:
        check = True
    return check



#AddingProba(calendar)
#AdjustProbaByValidation(row, tempProba)
#FindLastPeriodClosed()
#GetMaxReviewDate("2020-02-05")