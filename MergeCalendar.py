import pandas as pd
import numpy as np 
import datetime as dt
from dateutil import relativedelta
import time
import os
from datetime import datetime, timedelta
import DatabaseConnector
from dotenv import load_dotenv

load_dotenv('./dev.env')

DatasetsFolderPath = os.getenv("DATASETS_FOLDER_PATTH")

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

#la définition de nos variables globales qui sont des listes dans lesquelles nous insérons toutes les modifications à faire sur la base de données
#to_delete contient les lignes qu'on devra supprimer en bdd
#to_insert contient les lignes à modifier en bdd
#cf importlistings pour son utilisation
to_insert = []
to_delete = []

listOfStr = DATABASE_CALENDARS_COLUMNS + ['state']
key_index= { listOfStr[i] : i for i in range(0, len(listOfStr) ) }
#{'cal_key': 0, 'listing_id': 1, 'available': 2, 'start_date': 3, 'end_date': 4, 'num_day': 5, 'minimum_nights': 6, 'maximum_nights': 7, 'label': 8, 'state': 9}



def Merging(new_calendar):
    #on réinitialise les deux variables globales to_insert et to_delete
    global to_insert
    global to_delete
    to_insert = []
    to_delete = []

    minDate = new_calendar['start_date'].min()
    
    #Extraction de la data issue de la DB Postgre
    requestedColumns = DatabaseConnector.FormatInsert(DATABASE_CALENDARS_COLUMNS)
    res = DatabaseConnector.Execute(f"SELECT {requestedColumns} FROM calendars where end_date >= '" + str(minDate) + "'")
    old_calendar = pd.DataFrame(res, columns=DATABASE_CALENDARS_COLUMNS)

    #Si c'est le premier calendrier, pas de comparaison à faire
    if old_calendar.empty:
        to_insert = new_calendar
    #Utilisation typique de la fonction de merging
    else:
        to_insert,to_delete = MergeTwoCalendars(old_calendar,new_calendar)
        pd.DataFrame(to_delete,columns=['cal_key']).to_csv(f"./test.csv", index = False)
        DatabaseConnector.CalendarDelete(to_delete)
        time.sleep(10)
        to_insert = pd.DataFrame(to_insert,columns=DATABASE_CALENDARS_COLUMNS[1:])
    
    to_delete = pd.DataFrame(to_delete,columns=['cal_key'])
    return to_insert, to_delete


def MergeTwoCalendars(old_calendar,new_calendar):
    global to_insert
    global to_delete
    global key_index
    to_insert = []
    to_delete = []
    old_calendar["state"] = "old"
    new_calendar["state"] = "new"

    #Conversion des dates    
    old_calendar['start_date'] = pd.to_datetime(old_calendar.start_date)
    old_calendar['end_date'] = pd.to_datetime(old_calendar.end_date)
    new_calendar['start_date'] = pd.to_datetime(new_calendar.start_date)
    new_calendar['end_date'] = pd.to_datetime(new_calendar.end_date)

    #On fusionne le calendrier old et New
    concat_cal = pd.concat([old_calendar,new_calendar],sort=False)
    concat_cal = concat_cal[DATABASE_CALENDARS_COLUMNS + ["state"]]

    #Tri par annonce et date 
    concat_cal = concat_cal.sort_values(["listing_id","start_date"])
    #On enlève les dates "old" et "new" qui n'ont pas changé car on ne les utilise pas pour actualiser notre calendrier
    concat_cal = concat_cal.drop_duplicates(subset=["listing_id","start_date","end_date","available"], keep=False)
    #Application de la fonction de détection des changements de date pour chaque listing_id. 
    #On retourne une liste que l'on va traiter par la suite
    concat_cal.cal_key = concat_cal.cal_key.fillna(0)

    concat_cal.groupby("listing_id").apply(UpdateByListingGroup)

    return to_insert,to_delete


##On retourne le nombre de jours qui sépare les deux dates (inclusive) 
##e.g. Lundi à mercredi = 3

def nb_days(date1, date2):    
        
    return abs((date1-date2).days) + 1


##fonction principale de séparation des dates fermées et ouvertes
def UpdateByListingGroup(group):
    #Nombre maximum de jours au dela duquel on considère que la modification d'une période équivaut à un ajout d'une nouvelle location
    MAX_NUMBER_OF_DAYS_WHEN_EXTENDING = 2   
    global to_insert
    global to_delete
    global key_index

    #Nombre de valeurs old, pour pouvoir connaître le nombre d'itérations à faire sur la liste
    number_of_lines_to_update = group[group.state == "old"].shape[0]
    number_of_new_lines = group[group.state == "new"].shape[0]
    no_new_lines = False
    #Conversion en list
    group = group.values.tolist()

    #Si c'est une nouvelle annonce, on ne compare rien, on ajoute  simplement les lignes associées
    #################################################
    if(number_of_lines_to_update == 0):
        for u in range(len(group)):
            temp = group[u][1:-1]
            to_insert.append(temp)
        return None    
    if(number_of_new_lines == 0):
        return None
    #################################################  
    #Cas décalage à droite
    #nous supprimons toutes les nouvelles périodes qui ne se recoupent en aucun cas avec nos anciennes dates, et nous les ajoutons directement
    last_old_end_date = None
    for i in reversed(range(len(group))):
        if (group[i][key_index['state']] == "old"):
            last_old_end_date = group[i][key_index['end_date']]
            break

    for i in reversed(range(len(group))):
        if (group[i][key_index['state']] == "new" and group[i][key_index['start_date']] > last_old_end_date):
            to_insert.append(group[i][1:-1])
            del group[i]
            
    #On obtient les dates de début du nouveau et ancien calendrier
    beginning_date_of_new_calendar = None
    first_old = None
    first_new = None
    first_old_index = None
    first_new_index = None

    for i in range(len(group)):
        if (group[i][key_index['state']] == "new"):
            first_new = group[i]
            beginning_date_of_new_calendar = group[i][key_index['start_date']]
            first_new_index = i
            break

    if(beginning_date_of_new_calendar != None):

        #remove all periods that end before the new cal
        for i in reversed(range(len(group))):
            #Cas du décalage par la gauche des anciennes dates
            if (group[i][key_index['end_date']] < beginning_date_of_new_calendar):
                group.pop(i)
                number_of_lines_to_update -= 1
                first_new_index-=1
            ##
        #On vérifie qu'après les sections par la droite puis par la gauche, il ne reste pas deux dates, ce qui reviendrait
        # à gérer le cas du n°27
        if len(group) == 2 and group[0][key_index['available']] == group[1][key_index['available']]:
            group[1][key_index['start_date']] = group[0][key_index['start_date']]
            group[1][5] = nb_days(group[1][key_index['start_date']],group[1][key_index['end_date']])
            to_delete.append(group[0][key_index['cal_key']])
            to_insert.append(group[1][key_index['listing_id']:key_index['state']])
            return None

        for i in reversed(range(len(group))):
            if group[i][key_index['state']] == "old":
                first_old = group[i].copy()
                first_old_index = i
        # Si les dates de fin sont les mêmes, avec le même état (fermé ou ouvert), on garde simplement old car
        # c'est un hachage de l'ancienne date du au scrapping du nouveau mois
        # Sinon, on crée une période qui complète le décalage de la gauche
        if (first_old[key_index['end_date']] == first_new[key_index['end_date']] and first_old[key_index['available']] == first_new[key_index['available']]):
            if first_new_index > first_old_index:
                group.pop(first_new_index)
                group.pop(first_old_index)
            else:
                group.pop(first_old_index)
                group.pop(first_new_index)
            
            number_of_lines_to_update -= 1
            number_of_new_lines -= 1
        elif(first_old[key_index['start_date']] < first_new[key_index['start_date']]):        
            buffer_period = first_new.copy()
            buffer_period[key_index['available']] = first_old[key_index['available']]
            buffer_period[key_index['start_date']] = first_old[key_index['start_date']]
            buffer_period[key_index['end_date']] = first_new[key_index['start_date']]-timedelta(days=1)
            buffer_period[key_index['num_day']] = nb_days(buffer_period[key_index['start_date']],buffer_period[key_index['end_date']])
            to_insert.append(buffer_period[key_index['listing_id']:key_index['state']])
    
    #################################################
    if(number_of_lines_to_update == 0):
        for u in range(len(group)):
            temp = group[u][key_index['listing_id']:key_index['state']]
            to_insert.append(temp)
        return None    
    if(number_of_new_lines == 0):
        return None
    #################################################


    #Itération sur chaque old, et pour chaque old, on le compare avec tous les new       
    for i in range(number_of_lines_to_update):
        j = 0
        while(group[j][key_index['state']] == "new"):
            j+=1
        
        old_to_update = group.pop(j)
        #Conversion des strings en date
        old_date_start = old_to_update[key_index['start_date']]
        old_date_end = old_to_update[key_index['end_date']]


        new_periods = []
        if(len(group) == 0):
            to_delete.append(old_to_update[key_index['cal_key']])
            break

        #Pour chaque nouvelle période
        for k in range(len(group)):
            #Conversion des strings en date
            new_date_start = group[k][key_index['start_date']]
            new_date_end = group[k][key_index['end_date']]
            

            number_days_new_period = nb_days(new_date_start,new_date_end)
            number_days_old_period = nb_days(old_date_start, old_date_end)
            #si periode match pas
            if (new_date_start > old_date_end):
                #On prend le cas théorique où toutes les dates se suivent, il n'y a pas de trous.
                #Dans le cas contraire, il faudrait ecrire pass. (problème d'input)
                #Il n'y a pas eu de problème jusque là sur les deux années de calendrier.
                break
            if(new_date_end<old_date_start):
                break
            
            #Si il ne reste qu'un old et un new, on est sur le cas théorique sur décalage par la droite, donc on garde old
            if(i == number_of_lines_to_update-1 and k == len(group)-1):
                break

            #periode dans les bornes
            if(group[k][key_index['available']] == "f"):
                if (new_date_start <= old_date_end):
                    if (nb_days(old_date_end,new_date_end)>MAX_NUMBER_OF_DAYS_WHEN_EXTENDING and new_date_end>=old_date_end):

                        #Cas d'extension par la droite d'une fermeture
                        # Création de deux périodes
                        # première periode de old_start à old_end
                        # deuxième periode de old_end à new_end
                        if(new_date_start == old_date_start):
                            first_period = old_to_update.copy()
                            first_period[key_index['available']] = group[k][key_index['available']]
                            second_period = group[k].copy()
                            second_period[key_index['start_date']] = old_date_end+timedelta(days=1)

                            second_period[key_index['num_day']] = nb_days(second_period[key_index['end_date']],second_period[key_index['start_date']])  
                            new_periods.append(first_period[key_index['listing_id']:key_index['state']])
                            group.insert(k+1,second_period)

                        #Case d'extension par la droite sans dates similaires
                        else:

                            first_period = group[k].copy()
                            first_period[key_index['end_date']] = old_date_end
                            first_period[key_index['num_day']] = nb_days(first_period[key_index['start_date']],first_period[key_index['end_date']])                 
                            second_period = group[k].copy()

                            second_period[key_index['start_date']] = old_date_end+timedelta(days=1)
                            second_period[key_index['num_day']] = nb_days(second_period[key_index['end_date']],second_period[key_index['start_date']])
                            new_periods.append(first_period[key_index['listing_id']:key_index['state']])
                            group.insert(k+1,second_period)
                    #extension de deux jours ou moins => On considère que la location s'est étendue et que ce n'est pas une nouvelle location
                    else:       
                        new_periods.append(group[k][key_index['listing_id']:key_index['state']])
            #Période ouverte, on ne s'en occupe pas et on l'ajoute directement
            else:
                new_periods.append(group[k][key_index['listing_id']:key_index['state']])
                
        to_delete.append(old_to_update[key_index['cal_key']])
            
        #Insertion des nouvelles dates modifiées
        for period in new_periods:
            to_insert.append(period)
        for l in range(k):
            group.pop(0)

    #Verifier qu'on ne finit pas en laissant des dates "new" qui sont simplement en dehors de toute intersection avec les dates "old"
    #Cas : Old : du 20 janvier au 21 janvier , New : du 22 janvier au 23 janvier, Old : du 24 janvier au 26 janvier
    for remaining_date in group:
        if(remaining_date[key_index['listing_id']:key_index['state']] not in to_insert):
            to_insert.append(remaining_date[key_index['listing_id']:key_index['state']])
    return None


def ProcessAndSave(fileNameDate,SavedName,newCalendar):
    existsInsert = os.path.isfile(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv") 
    existsDelete = os.path.isfile(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv") 
    if existsInsert and existsDelete:
        print(f'--- Used {DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv ---')
        delete = pd.read_csv(f'{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv',sep=",")
        if not delete.empty:
            DatabaseConnector.CalendarDelete(delete['cal_key'].to_list())
            time.sleep(10)
        print(f'--- Used {DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv ---')
        return pd.read_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        insert, delete = Merging(newCalendar)
        print(f'--- Merging {fileNameDate} : {time.time() - start_time} ---')
        insert.to_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv", index = False)
        delete.to_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv", index = False)
        return insert

#EXEMPLE of Individual execution of merging
#start_time = time.time()
#Merging(f'{DatasetsFolderPath}/NameoftheFile')
#print("---  %s seconds ---" % (time.time() - start_time))




#Alternative code for ligne 195 to 200
#In case you want to avoid cutting a date 
'''            if(first_old[key_index['end_date']] < first_new[key_index['end_date']] 
                and first_old[key_index['available']] == first_new[key_index['available']]
                and first_old[key_index['available']] == "f"):
                first_new[key_index['start_date']] = first_old[key_index['end_date']]+timedelta(days =1)
                first_new[key_index['num_day']] = nb_days(first_new[key_index['start_date']],first_new[key_index['end_date']])
                group.pop(first_old_index)
                number_of_lines_to_update -= 1
                
            else:          
                buffer_period = first_new.copy()
                buffer_period[key_index['available']] = first_old[key_index['available']]
                buffer_period[key_index['start_date']] = first_old[key_index['start_date']]
                buffer_period[key_index['end_date']] = first_new[key_index['start_date']]-timedelta(days=1)
                buffer_period[key_index['num_day']] = nb_days(buffer_period[key_index['start_date']],buffer_period[key_index['end_date']])
                to_insert.append(buffer_period[key_index['listing_id']:key_index['state']])'''
#Making this change will make test 29 and 30 fail. This is normal, since the output should be different.