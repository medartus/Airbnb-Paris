import numpy as np
import pandas as pd
import time
import os

'''
Create the result for period grouping
'''
def groupDate(group):
    return [group.iloc[0]['date'], group.iloc[-1]['date'], group.shape[0],  group.iloc[0]['minimum_nights'],  group.iloc[0]['maximum_nights']]

'''
Import the calendar dataset and group by open/closed period to remove useless data
'''
def OptimizeCalendar(filename):
    calendar = pd.read_csv('./datasets/calendar/calendar-'+filename+'.csv',sep=",")

    calendar = calendar.sort_values(by=["listing_id", "date"])
    calendar = calendar.reset_index()
    
    # drop useless tables
    calendar = calendar.drop(list(set(['price','index','adjusted_price']) & set(calendar.columns)),axis=1)

    if 'minimum_nights' not in calendar.columns:
        calendar['minimum_nights'] = 0
    if 'maximum_nights' not in calendar.columns:
        calendar['maximum_nights'] = 1125

    # Group periods
    adj_check = (calendar.available != calendar.available.shift()).cumsum() # Avoid to grop no consecutive rows
    newData = pd.DataFrame(calendar.groupby(['listing_id','available',adj_check], as_index=False, sort=False).apply(groupDate))

    # Convert data into columns
    newData[['start_date','end_date','num_day','minimum_nights','maximum_nights']] = pd.DataFrame(newData[0].to_list(), index=newData.index)
    
    # Remove index to have access to listing_id and available columns
    newData.index = newData.index.set_names(['listing_id', 'available','foo'])
    newData = newData.reset_index()
    del newData['foo']
    del newData[0]
    
    return newData

def ProcessAndSave(fileNameDate,SavedName):
    exists = os.path.isfile(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used ./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = OptimizeCalendar(fileNameDate)
        print(f'--- Optimizing {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index = False)
        return df

# start_time = time.time()
# newData = OptimizeCalendar("calendar-2017-01")
# print("---  %s seconds ---" % (time.time() - start_time))
# newData.to_csv("./datasets/altered/calendar_periods.csv",index=False)