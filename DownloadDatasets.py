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
    exists = os.path.isfile("./datasets/"+datesetType+"/"+fileName+".csv") 
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
def DownloadFile(date,fileName):
    date = str(date)
    url = FormatUrl(date,fileName)

    try:
        urllib.request.urlretrieve (url, "./datasets/"+fileName+"/"+fileName+".csv.gz")
        with gzip.open('./datasets/'+fileName+'/'+fileName+'.csv.gz', 'rb') as f_in:
            with open('./datasets/'+fileName+'/'+fileName+'-'+str(date)[:7]+'.csv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove('./datasets/'+fileName+'/'+fileName+'.csv.gz')
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
def createFolder():
    for filename in datasets:
        try:
            os.mkdir('./datasets/'+fileName)
        except:
            pass

'''
Download all the datasets from a specific starting date
''' 
def DownloadAllDatesets(startDate,endDate=None):
    createFolder()
    startDate = datetime.datetime.strptime(startDate,"%Y-%m-%d").date()
    if endDate:
        endDate = datetime.datetime.strptime(endDate,"%Y-%m-%d").date()
    else:
        endDate = datetime.date.today()
    while startDate <= endDate:
        print(f'--- Download file for {startDate.strftime("%Y-%m-%d")} ---')
        DownloadDate(startDate)
        startDate = startDate + relativedelta.relativedelta(days=1)

DownloadAllDatesets('2017-01-01','2017-04-01')