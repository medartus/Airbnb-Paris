import pandas as pd
import numpy as np 
import datetime as dt
from dateutil import relativedelta
import time
import os
from datetime import datetime, timedelta
import DatabaseConnector

import csv 
def save_to_file(data, name):
    with open(name, 'w') as f: 
        # using csv.writer method from CSV package 
        write = csv.writer(f) 
        write.writerows(data) 

# List of columns kept in the database for Calendar dataset
date_format  = "%Y-%m-%d"
DATABASE_CALENDARS_COLUMNS = [
    "cal_key",
    "listing_id",
    "available",
    "start_date",
    "end_date",
    "num_day",
    "minimum_nights",
    "maximum_nights",
    "label",
    "validation",
    "proba",
    "ext_validation"
]

to_insert = []
to_delete = []
#Pour PostGre, cherche deux fonction , une pour faire les insertions, une pour les delete cf importlistings
#Créer deux listes : Une pour insérer , une pour delete

def Merging(date,new_calendar):
    #on réinitialise les deux variables globales to_insert et to_delete
    global to_insert
    global to_delete
    to_insert = []
    to_delete = []
    
    lastYearDate =  date - relativedelta.relativedelta(years=1)

    #get data from db and format
    res = DatabaseConnector.Execute("SELECT * FROM calendars where end_date >= '" + str(lastYearDate) + "'")
    old_calendar = pd.DataFrame(res, columns=DATABASE_CALENDARS_COLUMNS)

    #if first calendar
    if old_calendar.empty:
        to_insert = new_calendar
    #typical use of the function
    else:
        print("Merging...")
        to_insert,to_delete = MergeTwoCalendars(old_calendar,new_calendar)  
        print("Merging OK !")
        print("Deleting...")    
        DatabaseConnector.CalendarDelete(to_delete)
        print("Deleting OK ! ")
        to_insert = pd.DataFrame(to_insert)

    return to_insert


def MergeTwoCalendars(old_calendar,new_calendar):
    old_calendar["state"] = "old"
    new_calendar["state"] = "new"

    #rename columns so concat of both cal can be done
    new_calendar = new_calendar.rename(columns = {"start":"start_date", "end":"end_date"})

    #joining both calendars
    concat_cal = pd.concat([old_calendar,new_calendar])
    concat_cal = concat_cal[["cal_key","listing_id","available","start_date","end_date","num_day","minimum_nights","maximum_nights","label","state"]]

    #Sort par annonce et date 
    concat_cal = concat_cal.sort_values(["listing_id","start_date"])

    #On enlève les dates "old" et "new" qui n'ont pas changé car on ne les utilise pas pour actualiser notre calendrier
    concat_cal = concat_cal.drop_duplicates(subset=["listing_id","start_date","end_date"], keep=False)

    #Application de la fonction de détection des changements de date pour chaque listing_id. 
    #On retourne une liste que l'on va traiter par la suite
    concat_cal.cal_key = concat_cal.cal_key.fillna(0)
    concat_cal.cal_key = concat_cal.cal_key.astype(int)

    concat_cal.groupby("listing_id").apply(UpdateByListingGroup)

    return to_insert,to_delete


##returns absolute number of days between to dates (inclusive) 
##e.g. monday to wednesday = 3
def nb_days(date1, date2):    
    if (type(date1) == str):
        date1 = dt.datetime.strptime(date1,date_format).date()
    if (type(date2) == str):
        date2 = dt.datetime.strptime(date2,date_format).date()
        
    return abs((date1-date2).days + 1)


##Main function, splits the new periods according to our model
def UpdateByListingGroup(group):
    #maximum number of days before we start considering the modification of period as a new period
    MAX_NUMBER_OF_DAYS_WHEN_EXTENDING = 2   

    #setting our global variables which are lists where we insert all modifications to be done on the database
    #to_delete contains the lines of the db we want to delete
    #to_insert contains the lines of the db we need to insert after deletion to replace them
    global to_insert
    global to_delete

    #Boolean variable to check wether the old dates are intersecting with new dates or not. If not, they aren't deleted in the database
    has_intersect = True

    #getting the number of old values that we need to update
    number_of_lines_to_update = group[group.state == "old"].shape[0]

    #converting to list
    group = group.values.tolist()

    #If this is a new announce, we can't compare old with new, we just append all in the result ! 
    #################################################
    if(number_of_lines_to_update == 0):
        for u in range(len(group)):
            temp = group[u][1:-1] + ["f"]
            to_insert.append(temp)
        return None    
    #################################################   


    #Otherwise we iterate through all of the old to check if there's a         
    for i in range(number_of_lines_to_update):
        j = 0
            
        while(group[j][9] == "new"):
            j+=1
        
        old_to_update = group.pop(j)
        #converting date strings to date objects
        if (type(old_to_update[3]) == str):
            old_date_start = dt.datetime.strptime(old_to_update[3],date_format).date()
        else:
            old_date_start = old_to_update[3]

        if (type(old_to_update[4]) == str):
            old_date_end = dt.datetime.strptime(old_to_update[4],date_format).date()
        else:
            old_date_end = old_to_update[4]


        new_periods = []
        if(len(group) == 0):
            break
        has_intersect = False

        #for every new period
        for k in range(len(group)):
            #converting date strings to date objects
            if (type(group[k][3]) == str):
                new_date_start = dt.datetime.strptime(group[k][3],date_format).date()
            else:
                new_date_start = group[k][3]

            if (type(group[k][4]) == str):
                new_date_end = dt.datetime.strptime(group[k][4],date_format).date()
            else:
                new_date_end = group[k][4]

            """new_date_start = dt.datetime.strptime(group[k][2],date_format)
            new_date_end = dt.datetime.strptime(group[k][3],date_format)"""
            number_days_new_period = nb_days(new_date_start,new_date_end)
            number_days_old_period = nb_days(old_date_start, old_date_end)
            
            #si period match pas
            if (new_date_start > old_date_end):
                break
            
            #period dans les bornes
            if(new_date_start <= old_date_end and group[k][2] == "f"):
                has_intersect = True
                if (number_days_new_period - number_days_old_period > MAX_NUMBER_OF_DAYS_WHEN_EXTENDING
                    and number_days_new_period - number_days_old_period > 0
                   ):
                    # creating 2 periods from one
                    # first period from old_start to old_end
                    # second period from old_end to new_end
                                    
                    #case right extend
                    if(new_date_start == old_date_start):
                        
                        first_period = old_to_update.copy()
                        second_period = group[k].copy()
                        second_period[3] = (old_date_end+timedelta(days=1)).strftime(date_format)
                        
                        second_period[5] = nb_days(second_period[4],second_period[3])
                                                
                        new_periods.append(first_period[1:-1] + ["f"])
                        new_periods.append(second_period[1:-1] + ["f"])
                        
                    #case left extend
                    else:
                        first_period = group[k].copy()

                        first_period[4] = (old_date_start-timedelta(days=1)).strftime(date_format)
                        first_period[5] = nb_days(first_period[3],first_period[4])
                        second_period = old_to_update.copy()

                        new_periods.append(first_period[1:-1] + ["f"])
                        new_periods.append(second_period[1:-1] + ["f"])

                #Case where there's an extension with no similar bounds for both start and end date
                elif(new_date_end > old_date_end):
                    first_period = group[k].copy()
                    first_period[4] = old_date_end.strftime(date_format)
                    first_period[5] = nb_days(first_period[3],first_period[4])
                    
                    
                    second_period = group[k].copy()
                    second_period[3] = (old_date_end+timedelta(days=1)).strftime(date_format)
                    second_period[5] = nb_days(second_period[4],second_period[3])
                    
                    new_periods.append(first_period[1:-1] + ["f"])
                    group.append(second_period)
                else:
                    new_periods.append(group[k][1:-1] +["f"])
                
        #Append all old dates to delete that had an intersection with new dates
        if has_intersect:
            to_delete.append([str(old_to_update[0])])
            
        #append all new periods we created
        for period in new_periods:
            to_insert.append(period[:-1] + ["f"])
        for l in range(k):
            group.pop(0)

    #Verifier qu'on ne finit pas en laissant des dates "new" qui sont simplement en dehors de toute intersection avec les dates "old"
    #Cas : Old : du 20 janvier au 21 janvier , New : du 22 janvier au 23 janvier, Old : du 24 janvier au 26 janvier
    for remaining_date in group:
        to_insert.append(remaining_date[1:-1] + ["f"])
    return None

def ProcessAndSave(fileNameDate,SavedName,date,newCalendar):
    exists = os.path.isfile(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used ./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = Merging(date,newCalendar)
        print(f'--- Merging {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index_col = False)
        return df

#EXEMPLE
#start_time = time.time()
#Merging('./datasets/c09.csv')
#print("---  %s seconds ---" % (time.time() - start_time))