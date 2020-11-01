import pandas as pd
import numpy as np 
import datetime as dt
import time
from datetime import datetime, timedelta
from DatabaseConnector import *

# List of columns kept in the database for Calendar dataset
date_format  = "%Y-%m-%d"
DATABASE_CALENDAR_COLUMNS = [
    "listing_id",
    "available",
    "start_date",
    "end_date",
    "num_day",
    "minimum_nights",
    "maximum_nights",
    "label",
    "validation"
]
to_insert = []
to_delete = []
#Pour PostGre, cherche deux fonction , une pour faire les insertions, une pour les delete cf importlistings
#Créer deux listes : Une pour insérer , une pour delete

def RetrieveCalendars(filename1,filename2):
    start_time = time.time()
    New_Calendar = pd.read_csv('./datasets/'+filename1,sep = ",")
    Old_Calendar = pd.read_csv('./datasets/'+filename2,sep = ",")

    #On prend en entrée un  calendrier issu de la BDD (ou le premier calendrier) qu'on nomme old et le calendrier à merge "new"
    #On prend en entrée notre liste de changements à effectuer sur la database
    To_Insert,To_Delete = MergeTwoCalendars(Old_Calendar,New_Calendar)
    CalendarUpdaterInsertLines(To_Insert)
    #On ajoute ensuite la fonction de validation 


    print("---  %s seconds ---" % (time.time() - start_time))


def MergeTwoCalendars(Old_calendar,New_Calendar):
    Old_calendar["state"] = "old"
    New_Calendar["state"] = "new"
    unique_id = list(set(Old_calendar.listing_id))
    sub_calendar1 = pd.DataFrame()
    sub_calendar2 = pd.DataFrame()
    sub_calendar1 = Old_calendar
    sub_calendar2 = New_Calendar
    New_calendar = pd.concat([sub_calendar1,sub_calendar2])

    New_calendar = New_calendar[["listing_id","available","start","end","num_day","minimum_nights","maximum_nights","label","state"]]
    #Sort par annonce et date 
    New_calendar = New_calendar.sort_values(["listing_id","start"])
    #On enlève les dates "old" et "new" qui n'ont pas changé car on ne les utilise juste pas pour actualiser notre calendrier
    New_calendar = New_calendar.drop_duplicates(subset=["listing_id","start","end"], keep=False)
    #Application de la fonction de détection des changements de date pour chaque listing_id. On retourne une liste que l'on va traiter par la suite
    #Calendar_Output = New_calendar.groupby("listing_id").apply(update_by_listing_group)
    New_calendar.groupby("listing_id").apply(UpdateByListingGroup)

    return to_insert,to_delete

###
def UpdateByListingGroup(group):
    global to_insert
    global to_delete

    First_iter_switch = True
    only_new_periods = []
    number_of_lines_to_update = group[group.state == "old"].shape[0]
    group = group.values.tolist()
    #If this is a new announce, we can't compare old with new, we just append all in the result ! 
    #################################################
    if(number_of_lines_to_update == 0):
        for u in range(len(group)):
            only_new_periods.append(group[u])
        #result.append((group[0][0],0,0,only_new_periods))
        to_insert.append(only_new_periods[:-1] + ["f"])
        return None
    #################################################   
    #Otherwise we iterate through all of the old to check if there's a         
    for i in range(number_of_lines_to_update):
        j = 0
        while(group[j][8] == "new"):
            j+=1
        
        old_to_update = group.pop(j)
        old_date_start = dt.datetime.strptime(old_to_update[2],date_format)
        old_date_end = dt.datetime.strptime(old_to_update[3],date_format)

        new_periods = []
        if(len(group) == 0):
            break
        for k in range(len(group)):
            new_date_start = dt.datetime.strptime(group[k][2],date_format)
            new_date_end = dt.datetime.strptime(group[k][3],date_format)
            #print(new_date)
            #input()
            #.strftime("%H:%M:%S")
            if (old_date_end < new_date_start):
                break
                #################################################
            if (old_date_start != new_date_start and First_iter_switch):
                filling_period = old_to_update.copy()
                filling_period[1] = 't'
                filling_period[3] = (new_date_start - timedelta(days=1)).strftime(date_format)
                #print(filling_period[4])
                #input()
                new_periods.append(filling_period)
                First_iter_switch = False    
            new_periods.append(group[k])
        for l in range(k):
            group.pop(0)
        
        #result.append((old_to_update[0], old_to_update[2], old_to_update[3], new_periods))
        if len(new_periods) == 0:
            pass
        else:
            to_delete.append(old_to_update[:-1] + ["f"])
            for new_date in new_periods:
                to_insert.append(new_date[:-1] + ["f"])

    #Verifier qu'on ne finit pas en laissant des dates "new" qui sont simplement en dehors de toute intersection avec les dates "old"
    #Cas : Old : du 20 janvier au 21 janvier , New: du 22 janvier au 23 janvier, Old : du 24 janvier au 26 janvier
    for remaining_date in group:
        #result.append((old_to_update[0],0,0,remaining_date))
        to_insert.append(new_date[:-1] + ["f"])
    return None


RetrieveCalendars("c08.csv","c09.csv")


