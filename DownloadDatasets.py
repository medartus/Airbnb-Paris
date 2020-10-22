import urllib
import urllib.request
import gzip
import shutil
import os
import datetime

datasets = ['listings','reviews','calendar']

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
    except:
        pass

def DownloadDaily(days=1):
    date = datetime.date.today() - datetime.timedelta(days)
    print(date)
    for fileName in datasets:
        DownloadFile(date,fileName)

def DownloadAllDatesets(startDate):
    numberDays = datetime.date.today() - datetime.datetime.strptime(startDate,"%Y-%m-%d").date()
    for day in range(numberDays.days):
        DownloadDaily(day)

def createFolder():
    for filename in datasets:
        try:
            os.mkdir('./datasets/'+fileName)
        except:
            pass

createFolder()
DownloadAllDatesets('2017-01-01')