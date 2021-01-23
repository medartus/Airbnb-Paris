import pandas as pd
import time
import os
from datetime import timedelta
import DatabaseConnector
from dotenv import load_dotenv

load_dotenv('./dev.env')

DatasetsFolderPath = os.getenv("DATASETS_FOLDER_PATTH")

# List of columns kept in the database for Calendar dataset
date_format = "%Y-%m-%d"
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

# la définition de nos variables globales qui sont des listes dans lesquelles nous insérons toutes les modifications à
# faire sur la base de données
# to_delete contient les lignes qu'on devra supprimer en db
# to_insert contient les lignes à modifier en db
# cf importlistings pour son utilisation
to_insert = []
to_delete = []

listOfStr = DATABASE_CALENDARS_COLUMNS + ['state']

# acutal values
# {'cal_key': 0, 'listing_id': 1, 'available': 2, 'start_date': 3, 'end_date': 4, 'num_day': 5, 'minimum_nights': 6,
# 'maximum_nights': 7, 'label': 8, 'state': 9}
keys_indexes = {listOfStr[i]: i for i in range(0, len(listOfStr))}

def Merging(new_calendar):
    # On réinitialise les deux variables globales to_insert et to_delete
    global to_insert
    global to_delete
    to_insert = []
    to_delete = []

    minDate = new_calendar['start_date'].min()

    # Extraction de la data issue de la DB Postgre
    requestedColumns = DatabaseConnector.FormatInsert(DATABASE_CALENDARS_COLUMNS)
    res = DatabaseConnector.Execute(
        f"SELECT {requestedColumns} FROM calendars where end_date >= '" + str(minDate) + "'")
    old_calendar = pd.DataFrame(res, columns=DATABASE_CALENDARS_COLUMNS)

    # Si c'est le premier calendrier, pas de comparaison à faire
    if old_calendar.empty:
        to_insert = new_calendar
    # Utilisation typique de la fonction de merging
    else:
        to_insert, to_delete = MergeTwoCalendars(old_calendar, new_calendar)
        # pour debug
        # pd.DataFrame(to_delete, columns=['cal_key']).to_csv(f"./test.csv", index=False)
        DatabaseConnector.CalendarDelete(to_delete)
        time.sleep(10)
        to_insert = pd.DataFrame(to_insert, columns=DATABASE_CALENDARS_COLUMNS[1:])

    to_delete = pd.DataFrame(to_delete, columns=['cal_key'])
    return to_insert, to_delete


def MergeTwoCalendars(old_calendar, new_calendar):
    global to_insert
    global to_delete
    global keys_indexes
    to_insert = []
    to_delete = []
    old_calendar["state"] = "old"
    new_calendar["state"] = "new"

    # Conversion des dates
    old_calendar['start_date'] = pd.to_datetime(old_calendar.start_date)
    old_calendar['end_date'] = pd.to_datetime(old_calendar.end_date)
    new_calendar['start_date'] = pd.to_datetime(new_calendar.start_date)
    new_calendar['end_date'] = pd.to_datetime(new_calendar.end_date)

    # On fusionne le calendrier old et New
    concat_cal = pd.concat([old_calendar, new_calendar], sort=False)
    concat_cal = concat_cal[DATABASE_CALENDARS_COLUMNS + ["state"]]

    # Tri par annonce et date
    concat_cal = concat_cal.sort_values(["listing_id", "start_date"])
    # On enlève les dates "old" et "new" qui n'ont pas changé car on ne les utilise pas pour actualiser notre calendrier
    concat_cal = concat_cal.drop_duplicates(subset=["listing_id", "start_date", "end_date", "available"], keep=False)
    # Application de la fonction de détection des changements de date pour chaque listing_id.
    # On retourne une liste que l'on va traiter par la suite
    concat_cal.cal_key = concat_cal.cal_key.fillna(0)

    concat_cal.groupby("listing_id").apply(UpdateListingPeriods)

    return to_insert, to_delete


## On retourne le nombre de jours qui sépare les deux dates (inclusive)
## e.g. Lundi à mercredi = 3
def nb_days(date1, date2):
    return abs((date1 - date2).days) + 1


## fonction principale de séparation des dates fermées et ouvertes
def UpdateListingPeriods(group):
    # Nombre maximum de jours au dela duquel on considère que la modification d'une période équivaut à un ajout d'une
    # nouvelle location
    MAX_NUMBER_OF_DAYS_WHEN_EXTENDING = 2
    global to_insert
    global to_delete
    global keys_indexes

    # Nombre de valeurs old, pour pouvoir connaître le nombre d'itérations à faire sur la liste
    number_of_lines_to_update = group[group.state == "old"].shape[0]
    number_of_new_lines = group[group.state == "new"].shape[0]

    # Conversion en list
    group = group.values.tolist()

    # Si c'est une nouvelle annonce, on ne compare rien, on ajoute  simplement les lignes associées
    #################################################
    if number_of_lines_to_update == 0:
        for u in range(len(group)):
            temp = group[u][keys_indexes["listing_id"]:keys_indexes['state']]
            to_insert.append(temp)
        return None
    if number_of_new_lines == 0:
        return None
    # ################################################ Cas décalage à droite
    # nous supprimons toutes les nouvelles périodes qui ne se recoupent en aucun cas avec nos anciennes dates, et nous
    # les ajoutons directement
    last_old_end_date = None
    for i in reversed(range(len(group))):
        if group[i][keys_indexes['state']] == "old":
            last_old_end_date = group[i][keys_indexes['end_date']]
            break

    for i in reversed(range(len(group))):
        if group[i][keys_indexes['state']] == "new" and group[i][keys_indexes['start_date']] > last_old_end_date:
            to_insert.append(group[i][keys_indexes["listing_id"]:keys_indexes['state']])
            del group[i]

    # On obtient les dates de début du nouveau et ancien calendrier
    beginning_date_of_new_calendar = None
    first_old = None
    first_new = None
    first_old_index = None
    first_new_index = None

    for i in range(len(group)):
        if group[i][keys_indexes['state']] == "new":
            first_new = group[i]
            beginning_date_of_new_calendar = group[i][keys_indexes['start_date']]
            first_new_index = i
            break

    if beginning_date_of_new_calendar is not None:
        # on supprime toutes les periodes old qui se terminent avant le début de la premiere periode new
        # car si on a pas de new, on ne veut pas/on n'a pas besoin de les updates
        for i in reversed(range(len(group))):
            # Cas du décalage par la gauche des anciennes dates
            if group[i][keys_indexes['end_date']] < beginning_date_of_new_calendar:
                group.pop(i)
                number_of_lines_to_update -= 1
                first_new_index -= 1

        # On vérifie qu'après les extensions par la droite puis par la gauche, il ne reste pas QUE deux dates,
        # ce qui reviendrait à gérer le cas du test n°27
        if len(group) == 2 and group[0][keys_indexes['available']] == group[1][keys_indexes['available']]:
            group[1][keys_indexes['start_date']] = group[0][keys_indexes['start_date']]
            group[1][keys_indexes['num_day']] = nb_days(group[1][keys_indexes['start_date']], group[1][keys_indexes['end_date']])
            to_delete.append(group[0][keys_indexes['cal_key']])
            to_insert.append(group[1][keys_indexes['listing_id']:keys_indexes['state']])
            return None

        # On récupère le premier old
        for i in reversed(range(len(group))):
            if group[i][keys_indexes['state']] == "old":
                first_old = group[i].copy()
                first_old_index = i

        # Cas où la premiere periode old et la premiere period new finissent en même temps
        # Si les dates de fin sont les mêmes, avec le même état (fermé ou ouvert), on garde simplement old car
        # c'est un hachage de l'ancienne date dû au scrapping du nouveau mois
        if first_old[keys_indexes['end_date']] == first_new[keys_indexes['end_date']] and \
                first_old[keys_indexes['available']] == first_new[keys_indexes['available']]:
            # on pop d'abord l'index le plus grand, pour éviter de modifier l'index le plus bas
            if first_new_index > first_old_index:
                group.pop(first_new_index)
                group.pop(first_old_index)
            else:
                group.pop(first_old_index)
                group.pop(first_new_index)

            number_of_lines_to_update -= 1
            number_of_new_lines -= 1

        # Sinon, on crée une période qui complète le décalage de la gauche (qu'on appelle buffer period)
        elif first_old[keys_indexes['start_date']] < first_new[keys_indexes['start_date']]:
            buffer_period = first_new.copy()
            buffer_period[keys_indexes['available']] = first_old[keys_indexes['available']]
            buffer_period[keys_indexes['start_date']] = first_old[keys_indexes['start_date']]
            buffer_period[keys_indexes['end_date']] = first_new[keys_indexes['start_date']] - timedelta(days=1)
            buffer_period[keys_indexes['num_day']] = nb_days(buffer_period[keys_indexes['start_date']],
                                                             buffer_period[keys_indexes['end_date']])
            to_insert.append(buffer_period[keys_indexes['listing_id']:keys_indexes['state']])

    #################################################
    # on regarde s'il faut s'arreter, dans le cas où nos premières vérifications ont enlevées toutes les périodes old
    if number_of_lines_to_update == 0:
        for u in range(len(group)):
            temp = group[u][keys_indexes['listing_id']:keys_indexes['state']]
            to_insert.append(temp)
        return None
    if number_of_new_lines == 0:
        return None
    #################################################

    #### CAS GENERAL
    # Itération sur chaque old, et pour chaque old, on le compare avec tous les new
    for i in range(number_of_lines_to_update):
        new_periods = []
        j = 0
        k = 0

        # On avance le curseur j si jamais notre/nos premiere(s) date(s) sont des new, si jamais le sort a mis un ou des
        # new en premier
        while group[j][keys_indexes['state']] == "new":
            j += 1

        old_to_update = group.pop(j)
        # Conversion des strings en date
        old_date_start = old_to_update[keys_indexes['start_date']]
        old_date_end = old_to_update[keys_indexes['end_date']]

        # Pour chaque nouvelle période
        for k in range(len(group)):
            # Conversion des strings en date
            new_date_start = group[k][keys_indexes['start_date']]
            new_date_end = group[k][keys_indexes['end_date']]

            # RAPPEL /!\/!\ il n'est pas possible pour une période NEW, de commencer avant une période OLD, notre
            # algorithme est basé sur le fait qu'on considère d'abord une période OLD, puis les périodes new qui
            # commencent en même temps ou après. Les périodes NEW peuvent cependant finir après la période OLD
            # considérée, dans ce cas la, on opère une découpe de la période, sur le jour ou la période OLD se termine,
            # ce qui nous fait retomber exactement sur un cas général pour la période OLD suivante. /!\/!\

            # si periode match pas
            if new_date_start > old_date_end:
                # On prend le cas théorique où toutes les dates se suivent, il n'y a pas de trous.
                # Dans le cas contraire, il faudrait ecrire pass. (problème d'input)
                break
            if new_date_end < old_date_start:
                break

            # Si il ne reste qu'un OLD et un NEW, on est sur le cas théorique du décalage par la droite,
            # donc on garde OLD
            if i == number_of_lines_to_update - 1 and k == len(group) - 1:
                break

            # Cas ou la periode dans les bornes (cas par défaut)
            if group[k][keys_indexes['available']] == "f":
                if new_date_start <= old_date_end:
                    if nb_days(old_date_end, new_date_end) > MAX_NUMBER_OF_DAYS_WHEN_EXTENDING and \
                            new_date_end >= old_date_end:
                        # Cas d'extension par la droite d'une fermeture
                        # Création de deux périodes
                        # première periode de old_start à old_end
                        # deuxième periode de old_end à new_end
                        if new_date_start == old_date_start:
                            first_period = old_to_update.copy()
                            first_period[keys_indexes['available']] = group[k][keys_indexes['available']]

                            second_period = group[k].copy()
                            second_period[keys_indexes['start_date']] = old_date_end + timedelta(days=1)
                            second_period[keys_indexes['num_day']] = nb_days(second_period[keys_indexes['end_date']],
                                                                             second_period[keys_indexes['start_date']])
                            new_periods.append(first_period[keys_indexes['listing_id']:keys_indexes['state']])
                            group.insert(k + 1, second_period)

                        # Cas d'extension par la droite sans dates similaires
                        else:
                            first_period = group[k].copy()
                            first_period[keys_indexes['end_date']] = old_date_end
                            first_period[keys_indexes['num_day']] = nb_days(first_period[keys_indexes['start_date']],
                                                                            first_period[keys_indexes['end_date']])

                            second_period = group[k].copy()
                            second_period[keys_indexes['start_date']] = old_date_end + timedelta(days=1)
                            second_period[keys_indexes['num_day']] = nb_days(second_period[keys_indexes['end_date']],
                                                                             second_period[keys_indexes['start_date']])

                            # On ajoute la période aux ajouts futurs et on réinsère la période modifiée pour repasser
                            # en cas général
                            new_periods.append(first_period[keys_indexes['listing_id']:keys_indexes['state']])
                            group.insert(k + 1, second_period)

                    # extension de deux jours ou moins => On considère que la location s'est étendue et que ce n'est
                    # pas une nouvelle location
                    else:
                        new_periods.append(group[k][keys_indexes['listing_id']:keys_indexes['state']])

            # Période ouverte, on ne s'en occupe pas et on l'ajoute directement
            else:
                new_periods.append(group[k][keys_indexes['listing_id']:keys_indexes['state']])

        to_delete.append(old_to_update[keys_indexes['cal_key']])

        # Insertion des nouvelles dates modifiées
        for period in new_periods:
            to_insert.append(period)
        for l in range(k):
            group.pop(0)
    #### CAS GENERAL

    # Verifier qu'on ne finit pas en laissant des dates "new" qui sont simplement en dehors de toute intersection
    # et on les insère
    for remaining_date in group:
        if remaining_date[keys_indexes['listing_id']:keys_indexes['state']] not in to_insert and \
                remaining_date[keys_indexes["state"]] == "new":
            to_insert.append(remaining_date[keys_indexes['listing_id']:keys_indexes['state']])
    return None


def ProcessAndSave(fileNameDate, SavedName, newCalendar):
    existsInsert = os.path.isfile(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv")
    existsDelete = os.path.isfile(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv")
    if existsInsert and existsDelete:
        print(f'--- Used {DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv ---')
        delete = pd.read_csv(f'{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv',
                             sep=",")
        if not delete.empty:
            DatabaseConnector.CalendarDelete(delete['cal_key'].to_list())
            time.sleep(10)
        print(f'--- Used {DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv ---')
        return pd.read_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv", sep=",")
    else:
        start_time = time.time()
        insert, delete = Merging(newCalendar)
        print(f'--- Merging {fileNameDate} : {time.time() - start_time} ---')
        insert.to_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_insert-{fileNameDate}.csv", index=False)
        delete.to_csv(f"{DatasetsFolderPath}/saved/{fileNameDate}/{SavedName}_delete-{fileNameDate}.csv", index=False)
        return insert


# EXEMPLE of Individual execution of merging
# start_time = time.time()
# Merging(f'{DatasetsFolderPath}/NameoftheFile')
# print("---  %s seconds ---" % (time.time() - start_time))


# Alternative code for ligne 195 to 200
# In case you want to avoid cutting a date
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
# Making this change will make test 29 and 30 fail. This is normal, since the output should be different.
