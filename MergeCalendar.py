import pandas as pd
import numpy as np 
import datetime as dt
from dateutil import relativedelta
import time
import os
from datetime import datetime, timedelta
import DatabaseConnector


#col name = {key nom de colonne: value index de colonne}
#Nico/Med plus besoin de gerer les dates en décalage, seul les edge cases sur les deux bornes où on a une fragmentation de la date est à gérer

# List of columns kept in the database for Calendar dataset
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
    # #on réinitialise les deux variables globales to_insert et to_delete
    # global to_insert
    # global to_delete
    # to_insert = []
    # to_delete = []

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
    #on réinitialise les deux variables globales to_insert et to_delete
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
    
    #get start date of the new calendar
    # additionally, we get the first old period and first new period to see if there isn't a cut reservation
    first_old = None
    first_new = None
    last_old = None
    last_new = None
    passed_old = False
    passed_new = False
    ending_date_of_new_calendar = None
    counter = 0 

    #Taking care of new dates that don't have intersection with olds ==> Last month delay 
    for i in reversed(range(len(group))):
        if (group[i][9] == "old"):
            ending_date_of_new_calendar = group[i][4]
            break

    for i in range(len(group)):
        if(group[i][9] == "new" and not passed_new):
            passed_new = True
            idx_new  = i
        if(group[i][9] == "old" and not passed_old):
            passed_old = True
            idx_old = i
        if(ending_date_of_new_calendar != None):
            if(group[i][3]>ending_date_of_new_calendar):
                to_insert.append(group[i][1:-1])
                counter+=1
    for i in range(counter):
        group.pop(-1)

    first_old = group[idx_old]
    first_new = group[idx_new]
    if(abs(first_old[3].month - first_new[3].month) == 1 and first_new[4]>=first_old[4]):
        if(first_new[3]<=first_old[4]):
            if(first_new[4] != first_old[4]):                
                if(first_old[3] != first_new[3]):
                    if(nb_days(first_old[4],first_new[4])>20):
                        first_new[3] = first_old[3]
                        first_new[5] = nb_days(first_new[3],first_new[4])
                        to_insert.append(first_new[1:-1])
                        to_delete.append(first_old[0])
                    else:      
                        buffer_period = first_new.copy()
                        buffer_period[3] = first_old[3]
                        buffer_period[4] = first_old[4]
                        buffer_period[5] = nb_days(buffer_period[3],buffer_period[4])
                        second_period = first_new.copy()
                        second_period[3] = first_old[3] + timedelta(days=1)
                        second_period[5] = nb_days(second_period[3],second_period[4])
                        group.append(second_period)
                        to_insert.append(buffer_period[1:-1])
                        to_delete.append(first_old[0])
                else:
                    to_insert.append(first_new[1:-1])
                    to_delete.append(first_old[0])
            elif(first_new[2] != first_old[2]):
                if(first_new[3] == first_old[3]):
                    to_insert.append(first_new[1:-1])
                    to_delete.append(first_old[0])
                else:
                    buffer_period = first_new.copy()
                    buffer_period[3] = first_old[3]
                    buffer_period[5] = nb_days(buffer_period[3],buffer_period[4])
                    to_insert.append(buffer_period[1:-1])
                    to_delete.append(first_old[0])
        group.pop(idx_old)
        number_of_lines_to_update -= 1
        if(idx_old<idx_new):
            group.pop(idx_new-1)
        else:
            group.pop(idx_new)
        number_of_new_lines -=1



    if(len(group) == 0):
        return None
    if(number_of_lines_to_update == 0):
        if(number_of_new_lines == 0):
            return None
        else:
            for i in range(len(group)):
                to_insert.append(group[i][1:-1])

    #Otherwise we iterate through all of the old to compare old and the rest of the dates (new)         
    for i in range(number_of_lines_to_update):
        deleted = False
        extended = False

        j = 0

        while(group[j][9] == "new"):
            j+=1
        
        old_to_update = group.pop(j)

        date_of_extension = old_to_update[4]

        #converting date strings to date objects
        old_date_start = old_to_update[3]
        old_date_end = old_to_update[4]

        if(len(group) == 0):
            to_delete.append(old_to_update[0])
            break 

        new_periods = []
        #for every new period
        for k in range(len(group)):
            #converting date strings to date objects
            new_date_start = group[k][3]
            new_date_end = group[k][4]
        
            number_days_new_period = nb_days(new_date_start,new_date_end)
            number_days_old_period = nb_days(old_date_start, old_date_end)

            if(old_to_update[9]==group[k][9]):

                break          
            #si period match pas
            if (new_date_start > old_date_end):
                if(not extended):
                    if(not deleted):
                        if(old_date_start>date_of_extension):   
                            date_of_extension = old_date_start     
                            deleted = True 
                #On prend le cas théorique où toutes les dates se suivent, il n'y a pas de trous.
                #Dans le cas contraire, il faudrait ecrire pass. (problème d'input)
                break
            if(new_date_end<old_date_start and old_to_update[9]==group[k][9]):
                
                if(not extended):
                    deleted = True   
                break

            #period dans les bornes


            if(group[k][2] == "f"):
                if (new_date_start <= old_date_end):
                    #Case this is the first old and new : Either extension on the left, or both extension left and right
                    if(old_date_start<new_date_start and i == 0 and k == 0):
                        if(new_date_end>=old_date_end and old_to_update[2] != group[k][2]):
                            deleted = True
                            buffer_period = old_to_update.copy()
                            buffer_period[4] = new_date_start - timedelta(days=1)
                            buffer_period[5] = nb_days(buffer_period[4],buffer_period[3])
                            first_period = group[k].copy()
                            first_period[4] = old_date_end
                            first_period[5] = nb_days(first_period[4],first_period[3])
                            new_periods.append(buffer_period[1:-1])
                            new_periods.append(first_period[1:-1])
                            if(new_date_end != old_date_end):
                                second_period = group[k].copy()
                                second_period[3] = old_date_end+timedelta(days=1)
                                second_period[5] = nb_days(second_period[4],second_period[3])
                                group.insert(k+1,second_period)  
                    elif (nb_days(old_date_end,new_date_end)>MAX_NUMBER_OF_DAYS_WHEN_EXTENDING and new_date_end>=old_date_end):

                        # creating 2 periods from one
                        # first period from old_start to old_end
                        # second period from old_end to new_end
                        #case right extend

                        if(new_date_start == old_date_start):


                            if(i == number_of_lines_to_update - 1 and len(group) == 1):
                                deleted = True 
                                pass                          
                            else:
                                #Cas d'un old fermé et une extension de fermeture =>  On doit supprimer la date suivante old 
                                if(old_to_update[2] == "f"):
                                    #first_period = old_to_update.copy()
                                    second_period = group[k].copy()
                                    second_period[3] = old_date_end+timedelta(days=1)

                                    second_period[5] = nb_days(second_period[4],second_period[3])  
                                    #new_periods.append(first_period[1:-1])
                                    group.insert(k+1,second_period)  
                                    extended = True
                                    
                                else:
                                    #cas d'un old ouvert et une extension de fermeture par la gauche de la date suivante : on modifie juste la première date
                                    first_period = old_to_update.copy()
                                    first_period[2] = "f"
                                    new_periods.append(first_period[1:-1])
                                    date_of_extension = new_date_end

                                    second_period = group[k].copy()
                                    second_period[3] = old_date_end+timedelta(days=1)
                                    second_period[5] = nb_days(second_period[4],second_period[3])

                                    group.insert(k+1,second_period)  
                                    deleted = True
                                    

                        #Case where there's an extension with no similar bounds for both start and end date and this is the last date
                        
                        #this means this is the regular extension for each end of calendar
                        elif(k == len(group)-1):
                            new_periods.append(group[k][1:-1])
                            deleted = True  

                        else:
                            #Case this is a left extension from the next old date : we just cut the date and let the second date being handled by the next old date    

                            first_period = group[k].copy()
                            first_period[4] = old_date_end
                            first_period[5] = nb_days(first_period[3],first_period[4])                 
                            second_period = group[k].copy()
                            second_period[3] = old_date_end+timedelta(days=1)
                            second_period[5] = nb_days(second_period[4],second_period[3])
                            new_periods.append(first_period[1:-1])
                            group.insert(k+1,second_period)
                            extended = True
                    #case this is an extension <=2  OR deletion or old closing and new closed with smaller dates    
                    else:

                        if(group[k][1:-2] == old_to_update[1:-2]):
                            extended = True
                            k+=1
                            break

                        else:
                            deleted = True 
                            new_periods.append(group[k][1:-1])
            
            
            #case : not a closed date 
            else:
                #Case this is the first old and new for opened date : Either extension on the left, or both extension left and right
                if(old_date_start<new_date_start and i == 0 and k == 0):
                        if(new_date_end>old_date_end):
                            deleted = True
                            buffer_period = old_to_update.copy()
                            buffer_period[4] = new_date_start - timedelta(days=1)
                            buffer_period[5] = nb_days(buffer_period[4],buffer_period[3])
                            first_period = group[k].copy()
                            first_period[4] = old_date_end
                            first_period[5] = nb_days(first_period[4],first_period[3])
                            new_periods.append(buffer_period[1:-1])
                            new_periods.append(first_period[1:-1])

                            second_period = group[k].copy()
                            second_period[3] = old_date_end+timedelta(days=1)
                            second_period[5] = nb_days(second_period[4],second_period[3])
                            group.insert(k+1,second_period)

                if(old_to_update[4]<=new_date_end):
                    deleted = True 
                    new_periods.append(group[k][1:-1])
                elif(old_to_update[4]>new_date_end and new_date_end>old_to_update[3]):
                    deleted = True 
                    new_periods.append(group[k][1:-1])
                else:
                    #not Sure
                    new_periods.append(group[k][1:-1])
        
        if(k == 0 and not deleted and not extended):
            deleted = True       
        if(deleted): 
            to_delete.append(old_to_update[0])
        #append all new periods we created
        for period in new_periods:
            to_insert.append(period)
        for l in range(k):
            group.pop(0)

    #Verifier qu'on ne finit pas en laissant des dates "new" qui sont simplement en dehors de toute intersection avec les dates "old"
    #Cas : Old : du 20 janvier au 21 janvier , New : du 22 janvier au 23 janvier, Old : du 24 janvier au 26 janvier
    #Le cas d'extension par la droite du au décalage de calendrier OLD/NEW est géré par le remaining date ainsi que la condition Case Right Extend> IF
    for remaining_date in group:
        if(remaining_date[1:-1] not in to_insert):
            to_insert.append(remaining_date[1:-1])
    return None



def ProcessAndSave(fileNameDate,SavedName,date,newCalendar):
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

#Old version to get first date old and new
    '''beginning_date_of_new_calendar = None

    for i in range(len(group)):
        if (group[i][9] == "new"):
            first_new = group[i]
            beginning_date_of_new_calendar = group[i][3]
            break
    if(beginning_date_of_new_calendar != None):
        #remove all periods that end before the new cal
        for i in reversed(range(len(group))):
            print(group[i])
            if (group[i][4] < beginning_date_of_new_calendar):
                group.pop(i)
                number_of_lines_to_update -= 1
            elif group[i][9] == "old":
                first_old = group[i].copy()

        if (first_old[3] != first_new[3]):
            buffer_period = first_new.copy()
            buffer_period[3] = first_old[3]
            buffer_period[4] = first_new[3]-timedelta(days=1)
            buffer_period[5] = nb_days(buffer_period[3],buffer_period[4])
            to_insert.append(buffer_period[1:-1])'''