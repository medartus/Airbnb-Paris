import time
import ImportListing
import OptimizeCalendar
import LabelizePeriods
import DownloadDatasets
import Validation
import MergeCalendar
import DatabaseConnector
import Proba

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

    start_time = time.time()
    print('------- Start of listings process -------')
    ImportListing.ImportListings('listings-'+fileNameDate)
    print('------- End of listings process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))

    start_time = time.time()
    print('------- Start of calendar process -------')
    optimizedCalendar = OptimizeCalendar.OptimizeCalendar('calendar-'+fileNameDate)
    mergedCalendar = MergeCalendar.Merging(date,optimizedCalendar)
    labelizedCalendar = LabelizePeriods.labelize(mergedCalendar)
    print('------- End of calendar process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))
    
    start_time = time.time()
    print('------- Start of reviews process -------')
    validatedCalendar = Validation.ValidateWithReviews(labelizedCalendar,'reviews-'+fileNameDate)
    probaCalendar = Proba.AddingProba(validatedCalendar,'listings-'+fileNameDate+'.csv')
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
    hasDatasets = isListings and isCalendar and isReviews

    if not hasDatasets: # Download if datasets don't exist
        hasDatasets = DownloadDatasets.DownloadDate(date)
    elif hasDatasets:
        ProcessDatasets(date)
    else:
        print(f'Cannot process this date : {date}')


'''
Process the daily importation
'''
def ProcessDaily():
    date = datetime.date.today()
    ProcessDate(date)


# import pandas as pd
# start_time = time.time()
# print('------- Start of calendar process -------')
# optimizedCalendar =  pd.read_csv("./datasets/altered/calendar_periods.csv")
# mergedCalendar = MergeCalendar.Merging('2020-09-30',optimizedCalendar)
# labelizedCalendar = LabelizePeriods.labelize(mergedCalendar)
# print('------- End of calendar process -------')
# print("------------ %s seconds ------------" % (time.time() - start_time))

# start_time = time.time()
# print('------- Start of reviews process -------')
# validatedCalendar = Validation.ValidateWithReviews(labelizedCalendar,'reviews-2020-09')
# probaCalendar = Proba.AddingProba(validatedCalendar)
# DatabaseConnector.CalendarInsert(probaCalendar.values.tolist())
# print('------- End of reviews process -------')
# print("------------ %s seconds ------------" % (time.time() - start_time))