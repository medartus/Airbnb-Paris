import os
import gzip
import shutil
import urllib
import datetime
import urllib.request
from dateutil import relativedelta

datasets = ['listings','reviews','calendar']

'''
Verify if a file exists in a specific folder
'''   
def VerifyDatasetExists(datesetType,date):
    fileNameDate = str(date)[:7]
    fileName = datesetType+"-"+fileNameDate
    exists = os.path.isfile("./datasets/"+datesetType+"/"+fileName+".csv.gz") 
    if exists:
        print(f'------ {fileName} already exists ------')
    return exists


'''
Format the download URL to download a specific dataset
'''   
def FormatUrl(date,fileName):
    return "http://data.insideairbnb.com/france/ile-de-france/paris/"+date+"/data/"+fileName+".csv.gz"


'''
Download a dataset, unzip it, rename it and move it to a specific folder with the same other datasets
'''  
def DownloadFile(date,folderName):
    date = str(date)
    url = FormatUrl(date,folderName)
    fileName = folderName+'-'+str(date)[:7]

    try:
        urllib.request.urlretrieve (url, "./datasets/"+folderName+"/"+fileName+".csv.gz")
        return True
    except:
        return False

'''
Download yesterday dataset
''' 
def DownloadDaily(days=1,startDate = None):
    if not startDate:
        startDate = datetime.date.today()
    date = startDate - datetime.timedelta(days)
    print(f'--- Download file for {date} ---')
    return DownloadDate(date)

'''
Download a dataset for a specific date if it doesn't exist
''' 
def DownloadDate(date):
    dateHaveDataset = False
    for fileName in datasets:
        if not VerifyDatasetExists(fileName,date):
            downloadResult = DownloadFile(date,fileName)
            if downloadResult:
                dateHaveDataset = True
    return dateHaveDataset

'''
Create the dataset folder if doesn't exists
''' 
def CreateFolder():
    for fileName in datasets:
        try:
            os.mkdir('./datasets/'+fileName)
        except:
            pass

'''
Download all the datasets from a specific starting date
''' 
def DownloadAllDatesets(startDate,endDate=None):
    CreateFolder()
    startDate = datetime.datetime.strptime(startDate,"%Y-%m-%d").date()
    if endDate:
        endDate = datetime.datetime.strptime(endDate,"%Y-%m-%d").date()
    else:
        endDate = datetime.date.today()
    while startDate <= endDate:
        print(f'--- Download file for {startDate.strftime("%Y-%m-%d")} ---')
        DownloadDate(startDate)
        startDate = startDate + relativedelta.relativedelta(days=1)


if __name__ == "__main__":
    DownloadAllDatesets('2018-08-01','2018-09-01')