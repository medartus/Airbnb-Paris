import time
import ImportListing
import LabelizePeriods
import DownloadDatasets

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
    optimizedCalendar = OptimizeCalendar('calendar-'+fileNameDate)
    # Retrieve data from the database
    # Merging function
    labelizedCalendar = LabelizePeriods.labelize(calendar_per)
    print('------- End of calendar process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))
    
    start_time = time.time()
    print('------- Start of reviews process -------')
    ValidateWithReviews(labelizedCalendar,'reviews-'+fileNameDate)
    # Proba function
    # Update data in the database
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
