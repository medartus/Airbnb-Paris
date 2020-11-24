import pandas as pd
import numpy as np
import datetime
import ImportListing
import time
import os

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
        if listing.at[row["listing_id"],"instant_bookable"] == 't':
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
    if calendar.at[index,'listing_id'] != calendar.at[index+1,'listing_id']:
        return True
    return False

"""
Function we used : https://www.geogebra.org/calculator/rch5ctnn
"""
def CalculateProba(row,listing,calendar):
    tempProba = 0
    if row["label"] != "A" or row["label"] != "MIN" or row["num_day"] < "MAX":
        tempProba = 935/(142*np.sqrt(2*np.pi))*np.exp(-0.5*np.power((row["num_day"]+202)/142,2))
        tempProba = AdjustProbaInstantBooking(row, listing, tempProba)
        # tempProba = AdjustProbaByValidation(row, tempProba) 
    if CheckIfLastPeriodIsClosed(calendar, row.name) == True:
        tempProba = AdjustProbaLastPeriod(tempProba)
    return round(tempProba,2)

"""
This function goes through the whole calendar to add probability.
Function we used : https://www.geogebra.org/calculator/rch5ctnn
"""
def AddingProba(calendar,filename):
    listing = ImportListing.RetrieveListings(filename)
    listing = listing.set_index('id')
    calendar = calendar.sort_values(["listing_id","start_date"])         #To be sure that the calendar is sorted
    calendar['proba'] = calendar.apply(lambda x : CalculateProba(x,listing,calendar), axis=1)
    
    calendar['validation'] = False
    calendar['ext_validation'] = 0

    return calendar


def ProcessAndSave(fileNameDate,SavedName,calendar):
    exists = os.path.isfile(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used ./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = AddingProba(calendar,fileNameDate)
        print(f'--- Proba {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index = False)
        return df

# calendar = pd.read_csv("./datasets/saved/2017-01/validated_calendar-2017-01.csv")
# start_time = time.time()
# res = AddingProba(calendar,"2017-01")
# print("---  %s seconds ---" % (time.time() - start_time))
# print(res)
#AdjustProbaByValidation(row, tempProba)
#FindLastPeriodClosed()
#GetMaxReviewDate("2020-02-05")