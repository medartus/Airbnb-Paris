import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv('./dev.env')

DatasetsFolderPath = os.getenv("DATASETS_FOLDER_PATTH")

'''
Apply labelization to a given line/period
'''
def set_label(period):
    # Available
    if period['available'] == 't':
        return "A"
    # Closed
    else:
        # Closed outside min or max:
        if period['num_day'] < period['minimum_nights']:
            return "MIN"
        elif period['num_day'] > period['maximum_nights']:
            return "MAX"
        # Closed inside min or max
        else:
            # Closed inside min or max, less than 7 days
            if period['num_day'] < 7:
                return "L7"
            # Closed inside min or max, less than 14 days ( between 7 and 14)
            elif period['num_day'] < 14:
                return "L14"
            # Closed inside min or max, less than 14 days ( between 14 and 21)
            elif period['num_day'] < 21:
                return "L21"
            # Closed inside min or max, more than 21 days
            else:
                return "M21"


'''
Apply labelization the each line of the dataset
'''
def labelize(calendar_per):
    calendar_per['label'] = calendar_per.apply(set_label, axis=1)
    return calendar_per

def ProcessAndSave(fileNameDate,SavedName,calendar):
    exists = os.path.isfile(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used {DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = labelize(calendar)
        print(f'--- Labelized {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index = False)
        return df

# calendar_per = pd.read_csv(f"{DatasetsFolderPath}/altered/calendar_periods.csv")
# start_time = time.time()
# labelized = labelize(calendar_per)
# print("------------ %s seconds ------------" % (time.time() - start_time))
# print(labelized)
# labelized.to_csv(f"{DatasetsFolderPath}/altered/labelized_calendar_periods.csv")

# print(labelized)