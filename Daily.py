import DownloadDatasets
import Setup

'''
Process the daily importation
'''
def ProcessDaily():
    if DownloadDatasets.DownloadDaily():
        date = datetime.date.today()
        Setup.ProcessDatasets(date)    


if __name__ == "__main__":
    ProcessDaily()