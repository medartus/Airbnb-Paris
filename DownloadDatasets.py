import os
import gzip
import setup
import shutil
import urllib
import datetime
import urllib.request

datasets = ['listings','reviews','calendar']

def VerifyDatasetExists(datesetType,date):
    fileNameDate = str(date)[:7]
    fileName = datesetType+"-"+fileNameDate
    exists = os.path.isfile("./datasets/"+datesetType+"/"+fileName+".csv") 
    print(f'------ {fileName} already exists ------')
    return exists

def FormatUrl(date,fileName):
    return "http://data.insideairbnb.com/france/ile-de-france/paris/"+date+"/data/"+fileName+".csv.gz"

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

def DownloadDaily(days=1):
    date = datetime.date.today() - datetime.timedelta(days)
    print(f'--- Download file for {date} ---')
    return DownloadDate(date)

def DownloadDate(date):
    dateHaveDataset = False
    for fileName in datasets:
        if not VerifyDatasetExists(fileName,date):
            downloadResult = DownloadFile(date,fileName)
            if downloadResult:
                dateHaveDataset = True
    return dateHaveDataset

def createFolder():
    for filename in datasets:
        try:
            os.mkdir('./datasets/'+fileName)
        except:
            pass

def DownloadAllDatesets(startDate):
    createFolder()
    numberDays = datetime.date.today() - datetime.datetime.strptime(startDate,"%Y-%m-%d").date()
    for day in range(numberDays.days + 1):
        DownloadDaily(day)

DownloadAllDatesets('2017-01-01')