import pandas as pd
import numpy as np
import datetime
import time
import os
import Validation

def RetrieveFile(filename):
    df = pd.read_csv('./datasets/'+filename+'.csv',sep=";")
    return df

def ProcessLink():
    link = RetrieveFile('link')
    link = link.drop(['URL', 'Nb photos total','URL.1','Nb photos identiques'], axis=1)
    link = link.rename({"Site":"src_site","Code":"src_id","Site.1":"comp_site","Code.1":"comp_id","% correspondance":"corresp"},axis=1)
    link = link[link['src_site'] != "Booking"]
    link = link[link['comp_site'] != "Booking"]
    link['corresp'] = link['corresp'].str[:-1].astype(int)
    return link

def VerifyMemo(memo, validationList, src_id, comp_site, comp_id, corresp):
    if src_id not in memo:
        memo[src_id] = {}
    if comp_id not in memo[src_id]:
        memo[src_id][comp_id] = True
        validationList = np.append(validationList,[src_id,comp_site,comp_id,corresp])
    return validationList

def CreateValidationTable(memo, validationList, row):
    if row['src_site'] == 'Airbnb':
        validationList = VerifyMemo(memo,validationList,row['src_id'],row['comp_site'],row['comp_id'],row['corresp'])
    if row['comp_site'] == 'Airbnb':
        validationList = VerifyMemo(memo,validationList,row['comp_id'],row['src_site'],row['src_id'],row['corresp'])
    return validationList
    
def GetValidationId():
    link = ProcessLink()
    memo = {}
    validationList = np.array([])
    start_time = time.time()
    for index, row in link.iterrows():
        validationList = CreateValidationTable(memo,validationList,row)
    validationList = validationList.reshape(int(len(validationList)/4),4)
    validationTable = pd.DataFrame(validationList,columns=['listing_id','src_type','src_id','corresp'])
    print("--- %s seconds ---" % (time.time() - start_time))
    return validationTable

    
def ProcessReviews(date):
    reviews = pd.read_csv('./datasets/reviews/reviews-2020-09.csv',sep=",")
    reviews = reviews.drop(['id','reviewer_id','reviewer_name','comments'], axis=1)
    reviews = reviews.rename({"listing_id":"id","date":"date_com"},axis=1)
    reviews['date_com'] =  pd.to_datetime(reviews['date_com'], format='%Y-%m-%d')
    reviews['id'] = reviews['id'].astype(str)
    reviews['src'] = 'Airbnb'
    reviews = reviews.set_index('id')
    return reviews[reviews['date_com'] >= date]


# def ProcessBooking(date):
#     booking = RetrieveFile('booking')
#     booking = booking.drop(['boo_url', 'contenu_commentaire','avatar','nationalite','score','room_info','dte_cre'], axis=1)
#     booking = booking.rename({"b_id":"id","date_commentaire":"date_com"},axis=1)
#     booking['date_com'] =  pd.to_datetime(booking['date_com'], format='%d/%m/%Y')
#     booking['id'] = booking['id'].astype(str)
#     booking['src'] = 'Booking'
#     booking = booking.set_index('id')
#     return booking[booking['date_com'] >= date]

def ProcessAbritel(date):
    abritel = RetrieveFile('abritel')
    abritel = abritel.drop(['abr_detailpageurl','com_datepublished', 'com_reviewlanguage','com_nickname','com_dte_cre','com_headline','com_body'], axis=1)
    abritel = abritel.rename({"com_listingid":"id","com_arrivaldate":"date_com"},axis=1) # Here date com is date of arrival
    abritel['date_com'] =  pd.to_datetime(abritel['date_com'], format='%d/%m/%Y')
    abritel['id'] = abritel['id'].astype(str)
    abritel['src'] = 'Abritel'
    abritel = abritel.set_index('id')
    return abritel[abritel['date_com'] >= date]

def RetrieveReviews(groupedList, dictCom, row):
    table = dictCom[row['src_type']]
    try:
        foundReviews = table.loc[row['src_id']]
    except:
        return groupedList
    if not isinstance(foundReviews, pd.DataFrame):
        temp = np.append(foundReviews.values,[row['listing_id'],row['corresp']])
        return np.append(groupedList,temp)
    
    temp = foundReviews.values
    temp = np.insert(temp, 2, values=row['listing_id'], axis=1)
    temp = np.insert(temp, 3, values=row['corresp'], axis=1)
    return np.append(groupedList,temp)

def GroupReviews(date):
    dictCom = {}
    dictCom['Airbnb'] = ProcessReviews(date)
    # dictCom['Booking'] = ProcessBooking(date)
    dictCom['Abritel'] = ProcessAbritel(date)

    validationTable = GetValidationId()
    groupedList = np.array([])
    
    start_time = time.time()
    for index, row in validationTable.iterrows():
        groupedList = RetrieveReviews(groupedList,dictCom,row)
        
    groupedList = groupedList.reshape(int(len(groupedList)/4),4)
    grouped = pd.DataFrame(groupedList,columns=['date','src','listing_id','corresp'])
    print("--- %s seconds ---" % (time.time() - start_time))

    return grouped

def ValidateWithExternalReviews(calendar):
    date = Validation.get_last_day(calendar)
    groupedReviews = GroupReviews(date)
    groupedReviews['listing_id'] = groupedReviews['listing_id'].astype(int)
    groupedReviews['corresp'] = groupedReviews['corresp'].astype(int)
    groupedReviews['corresp'] = groupedReviews['corresp']/100
    return Validation.validateExternalCalendar(calendar,groupedReviews)

def ProcessAndSave(fileNameDate,SavedName,calendar):
    exists = os.path.isfile(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used ./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = ValidateWithExternalReviews(calendar)
        df = calendar
        print(f'--- External validation {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index = False)
        return df

if __name__ == "__main__":
    calendar = pd.read_csv("./datasets/altered/validated_calendar_periods.csv")
    start_time = time.time()
    res = ValidateWithExternalReviews(calendar)
    print("------------ %s seconds ------------" % (time.time() - start_time))
    # print(res[res['ext_validation'] != 0])