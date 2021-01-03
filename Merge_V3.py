import pandas as pd
import numpy as np 
import datetime as dt
from dateutil import relativedelta
import time
import os
from datetime import datetime, timedelta
import DatabaseConnector

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
    "label"
]

to_insert = []
to_delete = []
# Pour PostGre, cherche deux fonction , une pour faire les insertions, une pour les delete cf importlistings
# Créer deux listes : Une pour insérer , une pour delete

def Merging(new_calendar):
    #on réinitialise les deux variables globales to_insert et to_delete
    global to_insert
    global to_delete
    to_insert = []
    to_delete = []

    minDate = new_calendar['start_date'].min()
    
    #get data from db and format
    requestedColumns = DatabaseConnector.FormatInsert(DATABASE_CALENDARS_COLUMNS)
    res = DatabaseConnector.Execute(f"SELECT {requestedColumns} FROM calendars where end_date >= '" + str(minDate) + "'")
    old_calendar = pd.DataFrame(res, columns=DATABASE_CALENDARS_COLUMNS)

    #if first calendar
    if old_calendar.empty:
        to_insert = new_calendar
    #typical use of the function
    else:
        to_insert,to_delete = MergeTwoCalendars(old_calendar,new_calendar)
        DatabaseConnector.CalendarDelete(to_delete)
        to_insert = pd.DataFrame(to_insert,columns=DATABASE_CALENDARS_COLUMNS[1:])

    return to_insert


def MergeTwoCalendars(old_calendar,new_calendar):
    global to_insert
    global to_delete
    to_insert = []
    to_delete = []
    old_calendar["state"] = "old"
    new_calendar["state"] = "new"

    #Converting dates    
    old_calendar['start_date'] = pd.to_datetime(old_calendar.start_date)
    old_calendar['end_date'] = pd.to_datetime(old_calendar.end_date)
    new_calendar['start_date'] = pd.to_datetime(new_calendar.start_date)
    new_calendar['end_date'] = pd.to_datetime(new_calendar.end_date)

    #joining both calendars
    concat_cal = pd.concat([old_calendar,new_calendar],sort=False)
    concat_cal = concat_cal[DATABASE_CALENDARS_COLUMNS + ["state"]]

    #Sort par annonce et date 
    concat_cal = concat_cal.sort_values(["listing_id","start_date"])
    #On enlève les dates "old" et "new" qui n'ont pas changé car on ne les utilise pas pour actualiser notre calendrier
    concat_cal = concat_cal.drop_duplicates(subset=["listing_id","start_date","end_date","available"], keep=False)
    #Application de la fonction de détection des changements de date pour chaque listing_id. 
    #On retourne une liste que l'on va traiter par la suite
    concat_cal.cal_key = concat_cal.cal_key.fillna(0)
    concat_cal.cal_key = concat_cal.cal_key.astype(int)
    concat_cal.groupby("listing_id").apply(UpdateByListingGroup)

    return to_insert,to_delete


##returns absolute number of days between to dates (inclusive) 
##e.g. monday to wednesday = 3

def nb_days(date1, date2):    
        
    return abs((date1-date2).days) + 1


##Main function, splits the new periods according to our model
def UpdateByListingGroup(group):
    #maximum number of days before we start considering the modification of period as a new period
    MAX_NUMBER_OF_DAYS_WHEN_EXTENDING = 2   
    #setting our global variables which are lists where we insert all modifications to be done on the database
    #to_delete contains the lines of the db we want to delete
    #to_insert contains the lines of the db we need to insert after deletion to replace them
    global to_insert
    global to_delete

    #getting the number of old values that we need to update
    number_of_lines_to_update = group[group.state == "old"].shape[0]
    number_of_new_lines = group[group.state == "new"].shape[0]
    no_new_lines = False
    #converting to list
    group = group.values.tolist()

    #If this is a new announce, we can't compare old with new, we just append all in the result ! 
    #################################################
    if(number_of_lines_to_update == 0):
        for u in range(len(group)):
            temp = group[u][1:-1]
            to_insert.append(temp)
        return None    
    if(number_of_new_lines == 0):
        return None
    #################################################  
    
    #we remove all new periods that do not intersect in any way with our olds, and append them directly
    last_old_end_date = None
    for i in reversed(range(len(group))):
        if (group[i][9] == "old"):
            last_old_end_date = group[i][4]
            break

    for i in reversed(range(len(group))):
        if (group[i][9] == "new" and group[i][3] > last_old_end_date):
            to_insert.append(group[i][1:-1])
            del group[i]
            

    if len(group) == 2 and group[0][2] == group[1][2]:
        group[1][3] = group[0][3]
        group[1][5] = nb_days(group[1][3],group[1][4])
        to_delete.append(group[0][0])
        to_insert.append(group[1][1:-1])
        return None



    #get start date of the new calendar
    # additionally, we get the first old period and first new period to 
    beginning_date_of_new_calendar = None
    first_old = None
    first_new = None
    first_old_index = None
    first_new_index = None

    for i in range(len(group)):
        if (group[i][9] == "new"):
            first_new = group[i]
            beginning_date_of_new_calendar = group[i][3]
            first_new_index = i
            break

    if(beginning_date_of_new_calendar != None):

        #remove all periods that end before the new cal
        for i in reversed(range(len(group))):
            ##legacy code : This is a case where we have a starting date of new calendar > the date we input in our Merging parameter
            # In this case, it means that we have a  ~ 1-2 days closing outisde our intersection, that has to be dropped
            if (group[i][4] < beginning_date_of_new_calendar):
                group.pop(i)
                number_of_lines_to_update -= 1
                first_new_index-=1
            ##

        #get index of first old date
        for i in reversed(range(len(group))):
            if group[i][9] == "old":
                first_old = group[i].copy()
                first_old_index = i
        # if end dates are the same and open type is the same --> we want to push the old
        # else, we create a buffer period into general case
        if (first_old[4] == first_new[4] and first_old[2] == first_new[2]):
            if first_new_index > first_old_index:
                group.pop(first_new_index)
                group.pop(first_old_index)
            else:
                group.pop(first_old_index)
                group.pop(first_new_index)
            
            number_of_lines_to_update -= 1
            number_of_new_lines -= 1
        elif(first_old[3] < first_new[3]):

            buffer_period = first_new.copy()
            buffer_period[2] = first_old[2]
            buffer_period[3] = first_old[3]
            buffer_period[4] = first_new[3]-timedelta(days=1)
            buffer_period[5] = nb_days(buffer_period[3],buffer_period[4])
            to_insert.append(buffer_period[1:-1])
    
    #################################################
    if(number_of_lines_to_update == 0):
        for u in range(len(group)):
            temp = group[u][1:-1]
            to_insert.append(temp)
        return None    
    if(number_of_new_lines == 0):
        return None
    #################################################


    #Otherwise we iterate through all of the old to compare old and the rest of the dates (new)         
    for i in range(number_of_lines_to_update):
        j = 0
        while(group[j][9] == "new"):
            j+=1
        
        old_to_update = group.pop(j)
        #converting date strings to date objects
        old_date_start = old_to_update[3]
        old_date_end = old_to_update[4]


        new_periods = []
        if(len(group) == 0):
            to_delete.append(old_to_update[0])
            break

        #for every new period
        for k in range(len(group)):
            #converting date strings to date objects
            new_date_start = group[k][3]
            new_date_end = group[k][4]
            

            number_days_new_period = nb_days(new_date_start,new_date_end)
            number_days_old_period = nb_days(old_date_start, old_date_end)
            #si period match pas
            if (new_date_start > old_date_end):
                #On prend le cas théorique où toutes les dates se suivent, il n'y a pas de trous.
                #Dans le cas contraire, il faudrait ecrire pass. (problème d'input)
                break
            if(new_date_end<old_date_start):
                break
            
            #if we only have 1 new left and 1 old left, we append it as is
            if(i == number_of_lines_to_update-1 and k == len(group)-1):
                break

            #period dans les bornes
            if(group[k][2] == "f"):
                if (new_date_start <= old_date_end):
                    if (nb_days(old_date_end,new_date_end)>MAX_NUMBER_OF_DAYS_WHEN_EXTENDING and new_date_end>=old_date_end):


                        # creating 2 periods from one
                        # first period from old_start to old_end
                        # second period from old_end to new_end

                        #case right extend
                        if(new_date_start == old_date_start):
                            first_period = old_to_update.copy()
                            first_period[2] = group[k][2]
                            second_period = group[k].copy()
                            second_period[3] = old_date_end+timedelta(days=1)

                            second_period[5] = nb_days(second_period[4],second_period[3])  
                            new_periods.append(first_period[1:-1])
                            group.insert(k+1,second_period)

                        #Case where there's an extension with no similar bounds for both start and end date
                        else:

                            first_period = group[k].copy()
                            first_period[4] = old_date_end
                            first_period[5] = nb_days(first_period[3],first_period[4])                 
                            second_period = group[k].copy()

                            second_period[3] = old_date_end+timedelta(days=1)
                            second_period[5] = nb_days(second_period[4],second_period[3])
                            new_periods.append(first_period[1:-1])
                            group.insert(k+1,second_period)
                    else:
                        
                        new_periods.append(group[k][1:-1])
            else:
                new_periods.append(group[k][1:-1])
                
        to_delete.append(old_to_update[0])
            
        #append all new periods we created
        for period in new_periods:
            to_insert.append(period)
        for l in range(k):
            group.pop(0)

    #Verifier qu'on ne finit pas en laissant des dates "new" qui sont simplement en dehors de toute intersection avec les dates "old"
    #Cas : Old : du 20 janvier au 21 janvier , New : du 22 janvier au 23 janvier, Old : du 24 janvier au 26 janvier
    for remaining_date in group:
        if(remaining_date[1:-1] not in to_insert):
            to_insert.append(remaining_date[1:-1])
    return None


def ProcessAndSave(fileNameDate,SavedName,newCalendar):
    exists = os.path.isfile(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used ./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = Merging(newCalendar)
        print(f'--- Merging {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index = False)
        return df

#EXEMPLE
#start_time = time.time()
#Merging('./datasets/c09.csv')
#print("---  %s seconds ---" % (time.time() - start_time))