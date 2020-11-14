import numpy as np
import pandas as pd
import time

'''
Create the result for period grouping
'''
def groupDate(group):
    return [group.iloc[0]['date'], group.iloc[-1]['date'], group.shape[0],  group.iloc[0]['minimum_nights'],  group.iloc[0]['maximum_nights']]

'''
Import the calendar dataset and group by open/closed period to remove useless data
'''
def OptimizeCalendar(filename):
    calendar = pd.read_csv('./datasets/calendar/'+filename+'.csv',sep=",")

    calendar = calendar.sort_values(by=["listing_id", "date"])
    calendar = calendar.reset_index()

    # drop useless tables
    calendar = calendar.drop(['price', 'index','adjusted_price'])

    # Group periods
    adj_check = (calendar.available != calendar.available.shift()).cumsum() # Avoid to grop no consecutive rows
    newData = pd.DataFrame(calendar.groupby(['listing_id','available',adj_check], as_index=False, sort=False).apply(groupDate))

    # Convert data into columns
    newData[['start','end','num_day','minimum_nights','maximum_nights']] = pd.DataFrame(newData[0].to_list(), index= newData.index)

    # # Convert type
    # newData["start"] = pd.to_datetime(newData["start"])
    # newData["end"] = pd.to_datetime(newData["end"])
    
    # Remove index to have access to listing_id and available columns
    newData.index = newData.index.set_names(['listing_id', 'available','foo'])
    newData = newData.reset_index()
    del newData['foo']
    del newData[0]
    
    return newData

# start_time = time.time()
# newData = OptimizeCalendar("calendar-2020-08")
# print("---  %s seconds ---" % (time.time() - start_time))
# newData.to_csv("./datasets/altered/calendar_periods.csv",index=False)