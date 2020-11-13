import pandas as pd
import numpy as np 
import datetime as dt
import time
from datetime import datetime, timedelta

def PreprocessCalendar(df):
	date_format  = "%Y-%m-%d"
	new_result = []
	for rows in test:
	    vector = rows[3].replace("{","")
	    vector = vector.replace(",","")
	    vector = vector.replace("}","")
	    actual_date = rows[2].to_pydatetime()
	    listing_id = rows[0]
	    nb_days = 0
	    value = vector[0]
	    for x in vector:
	        if x == value :
	            nb_days+=1
	        if x != value :
	            start = actual_date.strftime(date_format)
	            end = (actual_date+timedelta(days=nb_days)).strftime(date_format)
	            if x == '0':
	                available = 't'
	            else:
	                available = 'f'
	            new_result.append([listing_id,available,start,end,nb_days+1,1,1000])
	            actual_date = actual_date+timedelta(days=nb_days+1)
	            nb_days = 1
	            value = x
	    start = actual_date.strftime(date_format)
	    end = (actual_date+timedelta(days=nb_days)).strftime(date_format)
	    if x == '0':
	        available = 't'
	    else:
	        available = 'f'
	    new_result.append([listing_id,available,start,end,nb_days,1,1000])
	    real_df = pd.DataFrame(new_result, columns=["listing_id","available","start","end","num_day","minimum_nights","maximum_nights"])
	return new_result

df = pd.read_excel("airbnb_calendriers.xlsx")
New_Calendar = PreprocessCalendar(df)

#appliquer LabelizePeriods.py ? #Comment appliquer un label sans min et max nights? Pourquoi y a -t-il plus de 365 jours ? 
#listing_id bien récpuéré? est-ce les mêmes que les notre? Est-ce que ça change ? 
#Trasnforme 82000 lignes en 546000 lignes...
#Solution pour listing 