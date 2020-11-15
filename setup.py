import time
import ImportListing
import OptimizeCalendar
import LabelizePeriods
import DownloadDatasets
import Validation
import MergeCalendar
import DatabaseConnector
import Proba
import datetime
import pandas as pd
from dateutil import relativedelta

DATABASE_CALENDARS_COLUMNS = [
    "listing_id",
    "available",
    "start_date",
    "end_date",
    "num_day",
    "minimum_nights",
    "maximum_nights",
    "label",
    "validation",
	"proba",
    "ext_validation"
]


'''
Process all the datasets and save te results in the database
'''
def ProcessDatasets(date):
    fileNameDate = str(date)[:7]

    # start_time = time.time()
    # print('------- Start of listings process -------')
    # ImportListing.ImportListings(f'listings-{fileNameDate}')
    # print('------- End of listings process -------')
    # print("------------ %s seconds ------------" % (time.time() - start_time))

    # start_time = time.time()
    # print('------- Start of calendar process -------')
    # optimizedCalendar = OptimizeCalendar.OptimizeCalendar('calendar-'+fileNameDate)
    # optimizedCalendar.to_csv(f"./datasets/saved/opti_calendar-{fileNameDate}.csv")
    # mergedCalendar = MergeCalendar.Merging(date,optimizedCalendar)
    # labelizedCalendar = LabelizePeriods.labelize(mergedCalendar)
    # labelizedCalendar.to_csv(f"./datasets/saved/label_calendar-{fileNameDate}.csv")
    # print('------- End of calendar process -------')
    # print("------------ %s seconds ------------" % (time.time() - start_time))

    
    labelizedCalendar = pd.read_csv("./datasets/saved/label_calendar-2017-01.csv")
    
    start_time = time.time()
    print('------- Start of reviews process -------')
    validatedCalendar = Validation.ValidateWithReviews(labelizedCalendar,'reviews-'+fileNameDate)
    validatedCalendar.to_csv(f"./datasets/saved/valid_calendar-{fileNameDate}.csv")
    probaCalendar = Proba.AddingProba(validatedCalendar,f'listings-{fileNameDate}')
    probaCalendar.to_csv(f"./datasets/saved/proba_calendar-{fileNameDate}.csv")
    extValidatedCalendar = probaCalendar # A remove
    # extValidatedCalendar =  ValidateWithExternalReviews(probaCalendar)
    listExtValidatedCalendar = extValidatedCalendar.values.tolist()
    DatabaseConnector.Insert(listExtValidatedCalendar,'calendars',DATABASE_CALENDARS_COLUMNS)
    print('------- End of reviews process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))


'''
Process a specific date and download the datasets if they don't exist
'''
def ProcessDate(date):
    # Verify if datasets exists
    isListings = DownloadDatasets.VerifyDatasetExists('listings',date)
    isCalendar = DownloadDatasets.VerifyDatasetExists('calendar',date)
    isReviews = DownloadDatasets.VerifyDatasetExists('reviews',date)
    hasDatasets = isListings and isCalendar # and isReviews

    if not hasDatasets: # Download if datasets don't exist
        hasDatasets = DownloadDatasets.DownloadDate(date)
    elif hasDatasets:
        ProcessDatasets(date)
    else:
        print(f'Cannot process this date : {date.strftime("%Y-%m-%d")}')


'''
Process the daily importation
'''
def ProcessDaily():
    date = datetime.date.today()
    ProcessDate(date)

def ProcessDateRange(startDate,endDate):
    startDate = datetime.datetime.strptime(startDate,"%Y-%m-%d").date()
    endDate = datetime.datetime.strptime(endDate,"%Y-%m-%d").date()
    while startDate < endDate:
        start_time = time.time()
        # ProcessDate(startDate)
        print(f'------------------------ {startDate.strftime("%Y-%m-%d")} processing time : {(time.time() - start_time)} seconds -------------------------')
        startDate = startDate + relativedelta.relativedelta(months=1)

ProcessDateRange('2017-01-01','2017-04-01')