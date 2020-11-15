import pandas as pd
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
    link['corresp'] = link['corresp'].str[:-1].astype(int)
    return link

def VerifyMemo(memo, validationTable, src_id, comp_site, comp_id, corresp):
    if src_id not in memo:
        memo[src_id] = {}
    if comp_id not in memo[src_id]:
        memo[src_id][comp_id] = True
        validationTable = validationTable.append({'listing_id': src_id,'src_type':comp_site,'src_id':comp_id,'corresp':corresp}, ignore_index=True,sort=False)
    return validationTable

def CreateValidationTable(memo, validationTable, row):
    if row['src_site'] == 'Airbnb':
        validationTable = VerifyMemo(memo,validationTable,row['src_id'],row['comp_site'],row['comp_id'],row['corresp'])
    if row['comp_site'] == 'Airbnb':
        validationTable = VerifyMemo(memo,validationTable,row['comp_id'],row['src_site'],row['src_id'],row['corresp'])
    return validationTable

def RetrieveReviews(grouped, dictCom, row):
    table = dictCom[row['src_type']]
    try:
        foundReviews = table.loc[row['src_id']]
        if not isinstance(foundReviews, pd.DataFrame):
            foundReviews = pd.DataFrame([foundReviews])
        foundReviews = foundReviews.reset_index(drop=True)
        foundReviews['listing_id'] = row['listing_id']
        foundReviews['corresp'] = row['corresp']/100
        return grouped.append(foundReviews, ignore_index=True,sort=False)
    except:
        return grouped
    
def GetValidationId():
    link = ProcessLink()
    memo = {}
    validationTable = pd.DataFrame(columns=['listing_id','src_type','src_id','corresp'])
    start_time = time.time()
    for index, row in link.iterrows():
        validationTable = CreateValidationTable(memo,validationTable,row)
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


def ProcessBooking(date):
    booking = RetrieveFile('booking')
    booking = booking.drop(['boo_url', 'contenu_commentaire','avatar','nationalite','score','room_info','dte_cre'], axis=1)
    booking = booking.rename({"b_id":"id","date_commentaire":"date_com"},axis=1)
    booking['date_com'] =  pd.to_datetime(booking['date_com'], format='%d/%m/%Y')
    booking['id'] = booking['id'].astype(str)
    booking['src'] = 'Booking'
    booking = booking.set_index('id')
    return booking[booking['date_com'] >= date]

def ProcessAbritel(date):
    abritel = RetrieveFile('abritel')
    abritel = abritel.drop(['abr_detailpageurl','com_arrivaldate', 'com_reviewlanguage','com_nickname','com_dte_cre','com_headline','com_body'], axis=1)
    abritel = abritel.rename({"com_listingid":"id","com_datepublished":"date_com"},axis=1)
    abritel['date_com'] =  pd.to_datetime(abritel['date_com'], format='%d/%m/%Y')
    abritel['id'] = abritel['id'].astype(str)
    abritel['src'] = 'Abritel'
    abritel = abritel.set_index('id')
    return abritel[abritel['date_com'] >= date]

def GroupReviews(date):
    dictCom = {}
    dictCom['Airbnb'] = ProcessReviews(date)
    dictCom['Booking'] = ProcessBooking(date)
    dictCom['Abritel'] = ProcessAbritel(date)

    validationTable = GetValidationId()

    grouped = pd.DataFrame(columns=['listing_id','src','date_com','corresp'])
    
    start_time = time.time()
    for index, row in validationTable.iterrows():
        grouped = RetrieveReviews(grouped,dictCom,row)
    grouped = grouped.rename({'date_com':'date'},axis=1)
    print("--- %s seconds ---" % (time.time() - start_time))

    return grouped

def ValidateWithExternalReviews(calendar):
    date = get_last_day(calendar)
    groupedReviews = GroupReviews(date)
    groupedReviews['listing_id'] = groupedReviews['listing_id'].astype(int)
    return Validation.validateExternalCalendar(calendar,groupedReviews)

def ProcessAndSave(fileNameDate,SavedName,calendar):
    exists = os.path.isfile(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used ./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = AddingProba(calendar,fileNameDate)
        print(f'--- External validation {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index_col = False)
        return df

# calendar = pd.read_csv("./datasets/altered/validated_calendar_periods.csv")
# res = ValidateWithExternalReviews(calendar)
# print(res[res['ext_validation'] != 0])