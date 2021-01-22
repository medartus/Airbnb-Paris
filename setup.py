import os
import gzip
import time
import shutil
import pandas as pd
from dateutil import relativedelta
from datetime import datetime, timedelta

import ImportListing
import OptimizeCalendar
import LabelizePeriods
import DownloadDatasets
import Validation
import MergeCalendar
import DatabaseConnector
import ConvertReviews
import Proba
from dotenv import load_dotenv

load_dotenv('./dev.env')

DatasetsFolderPath = os.getenv("DATASETS_FOLDER_PATTH")

datasets = ['listings','reviews','calendar']

DATAFRAME_CALENDARS_COLUMNS = [
    "listing_id",
    "available",
    "start_date",
    "end_date",
    "num_day",
    "minimum_nights",
    "maximum_nights",
    "label",
	"proba",
    "validation",
    "ext_validation",
    "cal_key"
]


'''
Create folder for saving .csv
'''
def CreateFolder(folderName):
    try:
        os.mkdir(f'{DatasetsFolderPath}/{folderName}')
    except:
        pass

def UnzipFiles(date):
    for i in range(2):
        newDate = date-relativedelta.relativedelta(months=i)
        fileNameDate = str(newDate)[:7]
        for folderName in datasets:
            if newDate == date or (newDate < date and folderName == "reviews") :
                fileName = folderName + '-' +fileNameDate
                with gzip.open(f'{DatasetsFolderPath}/{folderName}/{fileName}.csv.gz', 'rb') as f_in:
                    with open(f'{DatasetsFolderPath}/{folderName}/{fileName}.csv', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
    fileNameDate = str(date)[:7]
    if os.path.isfile(f'{DatasetsFolderPath}/saved/{fileNameDate}.zip'):
        shutil.unpack_archive(f'{DatasetsFolderPath}/saved/{fileNameDate}.zip', f'{DatasetsFolderPath}/saved/{fileNameDate}', 'zip')  
    
def CleanProcess(date):
    for i in range(2):        
        newDate = date-relativedelta.relativedelta(months=i)
        fileNameDate = str(newDate)[:7]
        for folderName in datasets:
            if newDate == date or (newDate < date and folderName == "reviews") :
                fileName = folderName + '-' +fileNameDate
                os.remove(f'{DatasetsFolderPath}/{folderName}/{fileName}.csv')
    fileNameDate = str(date)[:7]
    shutil.make_archive(f'{DatasetsFolderPath}/saved/{fileNameDate}', 'zip', f'{DatasetsFolderPath}/saved/{fileNameDate}')
    shutil.rmtree(f'{DatasetsFolderPath}/saved/{fileNameDate}')


'''
Process all the datasets and save te results in the database
'''
def ProcessDatasets(date):
    fileNameDate = str(date)[:7]

    CreateFolder('saved')
    CreateFolder(f'saved/{fileNameDate}')

    UnzipFiles(date)

    start_time = time.time()
    print('------- Start of listings process -------')
    ImportListing.ImportListings(fileNameDate)
    print('------- End of listings process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))

    start_time = time.time()
    print('------- Start of calendar process -------')
    optimizedCalendar = OptimizeCalendar.ProcessAndSave(fileNameDate,'optimized_calendar')
    mergedCalendar = MergeCalendar.ProcessAndSave(fileNameDate,'merged_calendar',optimizedCalendar)
    labelizedCalendar = LabelizePeriods.ProcessAndSave(fileNameDate,'labelized_calendar',mergedCalendar)
    probaCalendar = Proba.ProcessAndSave(fileNameDate,'probalized_calendar',labelizedCalendar)
    probaCalendar['cal_key'] = probaCalendar['listing_id'].astype(str) + '_' + probaCalendar['start_date'].astype(str)
    DatabaseConnector.Insert(probaCalendar.values.tolist(),'calendars',DATAFRAME_CALENDARS_COLUMNS)
    print('------- End of calendar process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))

    start_time = time.time()
    print('------- Start of reviews process -------')
    validatedCalendar = Validation.ProcessAndSave(fileNameDate,'validated_calendar',date)
    extValidatedCalendar =  ConvertReviews.ProcessAndSave(fileNameDate,'ext_validated_calendar',validatedCalendar)
    DatabaseConnector.UpdateValidation(extValidatedCalendar)
    print('------- End of reviews process -------')
    print("------------ %s seconds ------------" % (time.time() - start_time))

    CleanProcess(date)


'''
Process a specific date and download the datasets if they don't exist
'''
def CreateFolder(date):
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


def ProcessDateRange(startDate,endDate):
    startDate = datetime.strptime(startDate,"%Y-%m-%d").date()
    endDate = datetime.strptime(endDate,"%Y-%m-%d").date()
    while startDate < endDate:
        start_time = time.time()
        ProcessDatasets(startDate)
        print(f'------------------------ {startDate.strftime("%Y-%m-%d")} processing time : {(time.time() - start_time)} seconds -------------------------')
        startDate = startDate + relativedelta.relativedelta(months=1)


if __name__ == "__main__":
    ProcessDateRange('2017-05-01','2017-06-01')

    # period = "2017-02" 
    # date = datetime.strptime(period, "%Y-%m")

    # validatedCalendar = Validation.ProcessAndSave(period,'validated_calendar',date)
    # extValidatedCalendar =  ConvertReviews.ProcessAndSave(period,'ext_validated_calendar',validatedCalendar)
    # DatabaseConnector.UpdateValidation(extValidatedCalendar)
