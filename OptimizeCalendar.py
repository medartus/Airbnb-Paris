import numpy as np
import pandas as pd

def groupDate(group):
    return [group.iloc[0]['date'], group.iloc[-1]['date'], str(group.iloc[0]['listing_id']),  group.iloc[0]['available'],group.shape[0],  group.iloc[0]['minimum_nights'],  group.iloc[0]['maximum_nights']]

def OptimizeCalendar(filename):
    calendar = pd.read_csv(filename,sep=",")

    calendar = calendar.sort_values(by=["listing_id", "date"])
    calendar = calendar.reset_index()

    del calendar['price']
    del calendar['index']
    del calendar['adjusted_price']

    adj_check = (calendar.available != calendar.available.shift()).cumsum()
    newData = pd.DataFrame(calendar.groupby(['listing_id','available',adj_check], as_index=False, sort=False).apply(groupDate))
    newData[['start','end','listing_id','available','num_day','minimum_nights','maximum_nights']] = pd.DataFrame(newData[0].to_list(), index= newData.index)
    newData["start"] = pd.to_datetime(newData["start"])
    newData["end"] = pd.to_datetime(newData["end"])
    
    del newData[0]
    newData = newData.reset_index(drop=True)
    
    return newData

newData = OptimizeCalendar("../datasets/calendar08.csv")