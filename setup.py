import time
import ImportListing
import LabelizePeriods
import DownloadDatasets

def ProcessDatasets(date):
    fileNameDate = str(date)[:7]

    start_time = time.time()
    print('------- Start of listings process -------')
    ImportListing.ImportListings('listings-'+fileNameDate)
    print('------- End of listings process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))

    start_time = time.time()
    print('------- Start of calendar process -------')
    optimizedCalendar = OptimizeCalendar('calendar-'+fileNameDate)
    # Merging function
    labelizedCalendar = LabelizePeriods.labelize(calendar_per)
    print('------- End of calendar process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))
    
    start_time = time.time()
    print('------- Start of reviews process -------')
    ValidateWithReviews(labelizedCalendar,'reviews-'+fileNameDate)
    print('------- End of reviews process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))

def ProcessDate(date):
    isListings = DownloadDatasets.VerifyDatasetExists('listings',date)
    isCalendar = DownloadDatasets.VerifyDatasetExists('calendar',date)
    isReviews = DownloadDatasets.VerifyDatasetExists('reviews',date)
    hasDatasets = isListings and isCalendar and isReviews
    if not hasDatasets:
        hasDatasets = DownloadDatasets.DownloadDate(date)
    elif hasDatasets:
        ProcessDatasets(date)
    else:
        print(f'Cannot process this date : {date}')

def ProcessDaily():
    date = datetime.date.today()
    ProcessDate(date)
