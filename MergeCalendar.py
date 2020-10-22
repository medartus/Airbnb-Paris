import pandas as pd
import numpy as np 
import datetime as dt
from datetime import datetime, timedelta


New_Calendar = pd.read_csv("./datasets/c08.csv",sep = ",")
Old_calendar = pd.read_csv("./datasets/c09.csv",sep = ",")

#On prend en entrée un  calendrier issu de la BDD (ou le premier calendrier) qu'on nomme old et le calendrier à merge "new"
#On fait un test de notre fonction sur les 5premieres annonces du calendrier de la BDD

def merge_twocalendar(Old_calendar,New_Calendar):
	Old_calendar["state"] = "old"
	New_Calendar["state"] = "new"
	unique_id = list(set(Old_calendar.listing_id))
	sub_calendar1 = pd.DataFrame()
	sub_calendar2 = pd.DataFrame()
	for i in range(10):
	    sub_calendar1 = sub_calendar1.append(Old_calendar[Old_calendar.listing_id == unique_id[i]])
	    sub_calendar2 = sub_calendar2.append(New_Calendar[New_Calendar.listing_id == unique_id[i]])
	New_calendar = pd.concat([sub_calendar1,sub_calendar2])
	#Sort par annonce et date 
	New_calendar = New_calendar.sort_values(["listing_id","start"])
	#On enlève les dates "old" et "new" qui n'ont pas changé car on ne les utilise juste pas pour actualiser notre calendrier
	New_calendar = New_calendar.drop_duplicates(subset=["listing_id","start","end"], keep=False)
	Calendar_Output = New_calendar.groupby("listing_id").apply(update_by_listing_group)
	Temp = [pair for row in Calendar_Output for pair in row]
	Cleaned_result = pd.DataFrame(Temp)
	Cleaned_result.columns = ["listing_id","startdate","enddate","Newdates"]
	Uncleaned_result = Cleaned_result
	Cleaned_result = Cleaned_result[Cleaned_result['Newdates'].map(lambda d: len(d)) > 0]
	Result_list = Cleaned_result.values.tolist()
	#List Format : 
	#Result_list[X] = Iterate through differents modfications
	#Result_list[X][0] = Listing_id
	#Result_list[X][1] = Start Date of old calendar
	#Result_list[X][2] = End Date of old calendar
	#Result_list[X][3] = List of New ligns of date to implement
	#Result_list[X][3][Y] = Iterate through different ligns to implement
	#Result_list[X][3][Y][0] = Listing_id
	#Result_list[X][3][Y][1] = f or t (f for closed)
	#Result_list[X][3][Y][2] = Start date of New calendar
	#Result_list[X][3][Y][3] = New date of New calendar
	#Result_list[X][3][Y][4] = Numdays
	#Result_list[X][3][Y][5] = min nights
	#Result_list[X][3][Y][6] = max nights
	#Result_list[X][3][Y][7] = label
	#Result_list[X][3][Y][8] = state (old or new), not used to insert into database
	Uncleaned_list = Uncleaned_result.values.tolist()
	#Same than Result_list but if Uncleaned_list[X][3] is empty, then it's just an old date with no modifications
	return Result_list

def update_by_listing_group(group):
    First_iter_switch = True
    result = []
    only_new_periods = []
    number_of_lines_to_update = group[group.state == "old"].shape[0]
    group = group.values.tolist()
    #################################################
    #If this is a new announce, we can't compare old with new, we just append all in the result ! 
    if(number_of_lines_to_update == 0):
        for u in range(len(group)):
            only_new_periods.append(group[u])
        result.append((group[0][0],0,0,only_new_periods))
        return result
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
        result.append((old_to_update[0], old_to_update[2], old_to_update[3], new_periods))
    return result