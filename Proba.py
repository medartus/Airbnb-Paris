import pandas as pd
import numpy as np
import datetime
import ImportListing


"""
This function goes through the whole calendar to add probability.
Function we used : https://www.geogebra.org/calculator/rch5ctnn
"""
def AddingProba(calendar,filename):
    listing = ImportListing.RetrieveListings(filename)
    listing = listing.set_index('id')
    calendar = calendar.sort_values(["listing_id","start_date"])         #To be sure that the calendar is sorted
    probaCalendar = pd.DataFrame(columns=["proba"])
    for index, row in calendar.iterrows():
        tempProba = 0
        if row["label"] != "A" or row["label"] != "MIN" or row["num_day"] < "MAX":
            tempProba = 935/(142*np.sqrt(2*np.pi))*np.exp(-0.5*np.power((row["num_day"]+202)/142,2))
            tempProba = AdjustProbaInstantBooking(row, listing, tempProba)
            tempProba = AdjustProbaByValidation(row, tempProba) 
        if CheckIfLastPeriodIsClosed(calendar, index) == True:
            tempProba = AdjustProbaLastPeriod(tempProba)
        probaCalendar = probaCalendar.append({'proba':tempProba}, ignore_index=True, sort=False)
    calendar = pd.concat([calendar, probaCalendar], axis=1)
    return calendar


"""
This function adjusts the probability of one row based on the validation from Airbnb's reviews.
"""
def AdjustProbaByValidation(row, adjustedProba):
    if row["validation"] == True:
        adjustedProba = 1            #The formula is likely to change
    return adjustedProba


"""
This function checks if the reservation is available for instant booking.
"""
def AdjustProbaInstantBooking(row, listing, adjustedProba):
    try:
        if listing.loc[row["listing_id"]]["instant_bookable"] == 't':
            adjustedProba += (1-adjustedProba)*0.3              #The formula is likely to change
    except:
        pass
        # print(row["listing_id"])
    return adjustedProba


"""
If the reservation is available for instant booking, the probabilty increases.
"""
def AdjustProbaLastPeriod(adjustedProba):
    return adjustedProba*0.15                               #The formula is likely to change


"""
We consider that the last period of the calendar, if closed, is likely to be closed by the landlord.
As the calendar is sorted by listing_id and start_date, we can assume the last period for each listing_id is the last period with that ID.
"""
def CheckIfLastPeriodIsClosed(calendar, index):
    if index+1 == calendar.shape[0]:
        return True
    if calendar.iloc[index]['listing_id'] != calendar.iloc[index+1]['listing_id']:
        return True
    return False



# calendar = pd.read_csv("./datasets/altered/validated_calendar_periods.csv")
# del calendar['Proba']
# res = AddingProba(calendar,"listings-2020-09.csv")
# print(res)
#AdjustProbaByValidation(row, tempProba)
#FindLastPeriodClosed()
#GetMaxReviewDate("2020-02-05")