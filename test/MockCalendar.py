import pandas as pd

CALENDARS_COLUMNS = [
    "listing_id",
    "available",
    "start_date",
    "end_date",
    "num_day",
    "minimum_nights",
    "maximum_nights",
    "label"
]

DATBASE_COLUMNS = ['cal_key'] + CALENDARS_COLUMNS

def CreateDataFrame(data, isFromDatabase):
    if isFromDatabase:
        return pd.DataFrame(data,columns=DATBASE_COLUMNS)
    return pd.DataFrame(data,columns=CALENDARS_COLUMNS)

def CreateMock(newData, oldData, to_insert, to_delete):
    newDF = CreateDataFrame(newData,False)
    oldDF = CreateDataFrame(oldData,True)
    insertDF = CreateDataFrame(to_insert,False)

    newDF['start_date'] = pd.to_datetime(newDF.start_date)
    newDF['end_date'] = pd.to_datetime(newDF.end_date)
    oldDF['start_date'] = pd.to_datetime(oldDF.start_date)
    oldDF['end_date'] = pd.to_datetime(oldDF.end_date)

    insertDF['start_date'] = pd.to_datetime(insertDF.start_date)
    insertDF['end_date'] = pd.to_datetime(insertDF.end_date)
    to_insert = insertDF.values.tolist()

    return newDF, oldDF, to_insert, to_delete

'''
New : Empty
Old : Has data
Do nothing since no new data, so evrything is in the database
'''
def Mock1():
    old = [
        [132,9342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [133,9342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],
        [134,19342,'f','2017-07-18','2017-07-20',3,0.0,1125.0,'L7'],
        [135,19342,'f','2017-07-21','2017-07-27',7,0.0,1125.0,'L14'],
        [136,17725,'t','2017-06-21','2017-07-17',27,0.0,1125.0,'A'],
        [137,17725,'f','2017-07-18','2017-07-27',10,0.0,1125.0,'L14'],
    ]
    new = []
    to_insert = []
    to_delete = []
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Empty
Import all new lines
'''
def Mock2():
    old = []
    new = [
        [9342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [9342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],
        [19342,'f','2017-07-18','2017-07-20',3,0.0,1125.0,'L7'],
        [19342,'f','2017-07-21','2017-07-27',7,0.0,1125.0,'L14'],
        [17725,'t','2017-06-21','2017-07-17',27,0.0,1125.0,'A'],
        [17725,'f','2017-07-18','2017-07-27',10,0.0,1125.0,'L14'],
    ]
    to_insert = new
    to_delete = []
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Same as old
Old : Has Data
Di nothing since no update from data, evrything is already in the database
'''
def Mock3():
    old = [
        [125,9342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [126,9342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],
        [127,19342,'f','2017-07-18','2017-07-20',3,0.0,1125.0,'L7'],
        [128,19342,'f','2017-07-21','2017-07-27',7,0.0,1125.0,'L14'],
        [129,17725,'t','2017-06-21','2017-07-17',27,0.0,1125.0,'A'],
        [130,17725,'f','2017-07-18','2017-07-27',10,0.0,1125.0,'L14'],
    ]
    new = [
        [9342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [9342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],
        [19342,'f','2017-07-18','2017-07-20',3,0.0,1125.0,'L7'],
        [19342,'f','2017-07-21','2017-07-27',7,0.0,1125.0,'L14'],
        [17725,'t','2017-06-21','2017-07-17',27,0.0,1125.0,'A'],
        [17725,'f','2017-07-18','2017-07-27',10,0.0,1125.0,'L14'],
    ]
    to_insert = []
    to_delete = []
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Has Data
Update from closed to open so delete old and insert the new line
'''
def Mock4():
    old = [
        [125,9342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [126,9342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],

        [127,19342,'f','2017-07-18','2017-07-20',3,0.0,1125.0,'L7'],
        [128,19342,'f','2017-07-21','2017-07-27',7,0.0,1125.0,'L14'], #Modif

        [129,17725,'t','2017-06-21','2017-07-17',27,0.0,1125.0,'A'],
        [130,17725,'f','2017-07-18','2017-07-27',10,0.0,1125.0,'L14'],
    ]
    new = [
        [9342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [9342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],
        [19342,'f','2017-07-18','2017-07-20',3,0.0,1125.0,'L7'],
        [19342,'t','2017-07-21','2017-07-27',7,0.0,1125.0,'A'], #Modif
        [17725,'t','2017-06-21','2017-07-17',27,0.0,1125.0,'A'],
        [17725,'f','2017-07-18','2017-07-27',10,0.0,1125.0,'L14'],
    ]
    to_insert = [
        [19342,'t','2017-07-21','2017-07-27',7,0.0,1125.0,'A'],
    ]
    to_delete = [128] 
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Has Data
Update from open to close so delete old and insert the new line
'''
def Mock5():
    old = [
        [125,9342,'f','2017-07-17','2017-07-18',2,0.0,1125.0,'L7'],
        [126,9342,'f','2017-07-19','2017-07-20',2,0.0,1125.0,'L7'],
        [127,9342,'t','2017-07-21','2017-07-22',2,0.0,1125.0,'A'],
        [128,9342,'f','2017-07-22','2017-07-23',2,0.0,1125.0,'L7'],
    ]
    new = [
        [9342,'f','2017-07-17','2017-07-18',2,0.0,1125.0,'L7'],
        [9342,'f','2017-07-19','2017-07-20',2,0.0,1125.0,'L7'],
        [9342,'f','2017-07-21','2017-07-22',2,0.0,1125.0,'L7'],
        [9342,'f','2017-07-22','2017-07-23',2,0.0,1125.0,'L7'],
    ]
    to_insert = [
        [9342,'f','2017-07-21','2017-07-22',2,0.0,1125.0,'L7'],
    ]
    to_delete = [127]
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data starting on 2017-02-01
Old : Has Data starting on 2017-01-01
Do not break period if cut by the begining of new calendar
'''
def Mock6():
    old = [
        [125,9342,'f','2017-01-01','2017-02-08',39,0.0,1125.0,'M21'],
        [126,9342,'f','2017-02-19','2017-02-20',9,0.0,1125.0,'L7'],
    ]
    new = [
        [9342,'f','2017-02-01','2017-02-08',8,0.0,1125.0,'L14'], 
        [9342,'f','2017-02-19','2017-02-20',9,0.0,1125.0,'L7'],
    ]
    to_insert = []
    to_delete = []
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Has Data
Do not break period if cut by the end of old calendar so delete the old one and update with the new one
'''
def Mock7():
    old = [
        [125,9342,'f','2018-11-01','2018-11-18',18,0.0,1125.0,'L21'],
        [126,9342,'f','2018-11-19','2018-11-30',12,0.0,1125.0,'L14'],
    ]
    new = [
        [9342,'f','2018-11-01','2018-11-18',18,0.0,1125.0,'L21'],
        [9342,'f','2018-11-19','2018-12-19',30,0.0,1125.0,'M21'],
    ]
    to_insert = [
        [9342,'f','2018-11-19','2018-12-19',30,0.0,1125.0,'M21']
    ]
    to_delete = [126]
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Has Data
Extension by the right but no split periods since additional time < MAX_NUMBER_OF_DAYS_WHEN_EXTENDING (=2)
'''
def Mock8():
    old = [
        [125,9342,'f','2017-07-17','2017-07-18',2,0.0,1125.0,'L7'],
        [126,9342,'f','2017-07-19','2017-07-25',7,0.0,1125.0,'L14'],    #Modif
        [127,9342,'t','2017-07-26','2017-07-28',3,0.0,1125.0,'A'],      #Modif
        [128,9342,'f','2017-07-28','2017-07-29',2,0.0,1125.0,'L7'],
    ]
    new = [
        [9342,'f','2017-07-17','2017-07-18',2,0.0,1125.0,'L7'],
        [9342,'f','2017-07-19','2017-07-26',8,0.0,1125.0,'L14'],        #Modif
        [9342,'t','2017-07-27','2017-07-28',2,0.0,1125.0,'A'],          #Modif
        [9342,'f','2017-07-28','2017-07-29',2,0.0,1125.0,'L7'],
    ]
    to_insert = [
        [9342,'f','2017-07-19','2017-07-26',8,0.0,1125.0,'L14'],
        [9342,'t','2017-07-27','2017-07-28',2,0.0,1125.0,'A'],
    ]
    to_delete = [126,127]
    return CreateMock(new, old, to_insert, to_delete) 


'''
New : Has Data
Old : Has Data
Extension by the right with period split into two periods
'''
def Mock9():
    old = [
        [125,9342,'f','2017-07-17','2017-07-18',2,0.0,1125.0,'L7'],
        [126,9342,'f','2017-07-19','2017-07-20',2,0.0,1125.0,'L7'], #Modif
        [127,9342,'t','2017-07-21','2017-07-28',8,0.0,1125.0,'A'],  #Modif
        [128,9342,'f','2017-07-28','2017-07-29',2,0.0,1125.0,'L7'],
    ]
    new = [
        [9342,'f','2017-07-17','2017-07-18',2,0.0,1125.0,'L7'],
        [9342,'f','2017-07-19','2017-07-26',8,0.0,1125.0,'L14'],    #Modif
        [9342,'t','2017-07-27','2017-07-28',2,0.0,1125.0,'A'],      #Modif
        [9342,'f','2017-07-28','2017-07-29',2,0.0,1125.0,'L7'],
    ]
    to_insert = [
        [9342,'f','2017-07-19','2017-07-20',2,0.0,1125.0,'L7'],
        [9342,'f','2017-07-21','2017-07-26',6,0.0,1125.0,'L7'],
        [9342,'t','2017-07-27','2017-07-28',2,0.0,1125.0,'A'],
    ]
    to_delete = [126,127]
    return CreateMock(new, old, to_insert, to_delete)


'''
New : Has Data
Old : Has Data
Case 1: Update from closed to open so delete old and insert the new line, alongside a open period, so merge them
'''
def Mock10():
    old = [
        [125,19342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [126,19342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],
        [127,19342,'f','2017-07-18','2017-07-20',3,0.0,1125.0,'L7'], #Modif
        [128,19342,'f','2017-07-21','2017-07-27',7,0.0,1125.0,'L14'], 
    ]
    new = [
        [19342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [19342,'t','2017-07-12','2017-07-20',9,0.0,1125.0,'A'], #Modif
        [19342,'f','2017-07-21','2017-07-27',7,0.0,1125.0,'L14'],  
    ]
    to_insert = [
        [19342,'t','2017-07-12','2017-07-20',9,0.0,1125.0,'A'],
    ]
    to_delete = [126,127]
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Has Data
Case 1: Shortened reserved period and up-sizing available => should replace both periods
'''
def Mock11():
    old = [
        [125,19342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [126,19342,'t','2017-07-12','2017-07-14',3,0.0,1125.0,'A'],     #Modif
        [127,19342,'f','2017-07-15','2017-07-22',8,0.0,1125.0,'L14'],   #Modif
        [128,19342,'f','2017-07-23','2017-07-27',5,0.0,1125.0,'L7'],  
    ]
    new = [
        [19342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [19342,'t','2017-07-12','2017-07-16',5,0.0,1125.0,'A'],         #Modif
        [19342,'f','2017-07-17','2017-07-22',6,0.0,1125.0,'L7'],        #Modif
        [19342,'f','2017-07-23','2017-07-27',5,0.0,1125.0,'L7'], 
    ]
    to_insert = [
        [19342,'t','2017-07-12','2017-07-16',5,0.0,1125.0,'A'],
        [19342,'f','2017-07-17','2017-07-22',6,0.0,1125.0,'L7']
    ]
    to_delete = [126,127]
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Has Data
Case 2:
'''
def Mock12():
    old = [
        [125,19342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [126,19342,'t','2017-07-12','2017-07-22',11,0.0,1125.0,'A'],     #Modif
        [127,19342,'f','2017-07-23','2017-07-27',5,0.0,1125.0,'L14'],   #Modif
    ]
    new = [
        [19342,'f','2017-06-21','2017-07-11',21,0.0,1125.0,'M21'],
        [19342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],         #Modif
        [19342,'f','2017-07-18','2017-07-27',10,0.0,1125.0,'L14'],      #Modif
    ]
    to_insert = [
        [19342,'t','2017-07-12','2017-07-17',6,0.0,1125.0,'A'],
        [19342,'f','2017-07-18','2017-07-22',5,0.0,1125.0,'L7'],
        [19342,'f','2017-07-23','2017-07-27',5,0.0,1125.0,'L7'],
    ]
    to_delete = [126,127]
    return CreateMock(new, old, to_insert, to_delete) 

'''
New : Has Data
Old : Has Data
Case 3: Case of one period canceled and another period rented
'''
def Mock13():
    old = [
        [125,19342,'t','2017-06-21','2017-07-11',21,0.0,1125.0,'A'],
        [126,19342,'f','2017-07-12','2017-07-22',11,0.0,1125.0,'L14'],
        [127,19342,'t','2017-07-23','2017-07-27',5,0.0,1125.0,'A'],
    ]
    new = [
        [19342,'t','2017-06-21','2017-07-06',16,0.0,1125.0,'A'],
        [19342,'f','2017-07-07','2017-07-20',14,0.0,1125.0,'L21'],
        [19342,'t','2017-07-21','2017-07-27',7,0.0,1125.0,'A'],
    ]
    to_insert = [
        [19342,'t','2017-06-21','2017-07-06',16,0.0,1125.0,'A'],
        [19342,'f','2017-07-07','2017-07-20',14,0.0,1125.0,'L21'],
        [19342,'t','2017-07-21','2017-07-27',7,0.0,1125.0,'A'],
    ]
    to_delete = [125,126,127]
    return CreateMock(new, old, to_insert, to_delete) 


'''
New : Has Data
Old : Has Data
Case 4: Old period canceled and multiple new period created 
'''
def Mock14():
    old = [
        [123,19342,'f','2017-06-28','2017-06-30',3,0.0,1125.0,'L7'],
        [124,19342,'t','2017-07-01','2017-07-08',8,0.0,1125.0,'A'],
        [125,19342,'f','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [126,19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'A']
    ]
    new = [
        [19342,'f','2017-06-28','2017-06-30',3,0.0,1125.0,'L7'],
        [19342,'t','2017-07-01','2017-07-06',6,0.0,1125.0,'A'],
        [19342,'f','2017-07-07','2017-07-09',3,0.0,1125.0,'L7'],
        [19342,'t','2017-07-10','2017-07-12',3,0.0,1125.0,'A'],
        [19342,'f','2017-07-13','2017-07-25',13,0.0,1125.0,'L21'],
        [19342,'t','2017-07-26','2017-07-31',6,0.0,1125.0,'A']
    ]
    to_insert = [
        [19342,'t','2017-07-01','2017-07-06',6,0.0,1125.0,'A'],
        [19342,'f','2017-07-07','2017-07-09',3,0.0,1125.0,'L7'],
        [19342,'t','2017-07-10','2017-07-12',3,0.0,1125.0,'A'],
        [19342,'f','2017-07-13','2017-07-25',13,0.0,1125.0,'L21'],
        [19342,'t','2017-07-26','2017-07-31',6,0.0,1125.0,'A']
    ]
    to_delete = [124,125,126]
    return CreateMock(new, old, to_insert, to_delete) 



'''
New : Has Data
Old : Has Data
Case 15: Annulation of first dates, re-rent but subdivised 
'''
def Mock15():
    old = [
        [123,19342,'t','2017-06-20','2017-06-30',11,0.0,1125.0,'L7'],
        [124,19342,'t','2017-07-01','2017-07-08',8,0.0,1125.0,'A'],
        [125,19342,'f','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [126,19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'A']
    ]
    new = [
        [19342,'t','2017-06-20','2017-06-23',3,0.0,1125.0,'L7'],
        [19342,'f','2017-06-23','2017-06-26',3,0.0,1125.0,'A'],
        [19342,'t','2017-06-26','2017-06-30',3,0.0,1125.0,'L7'],
        [19342,'t','2017-07-01','2017-07-08',6,0.0,1125.0,'A'],
        [19342,'t','2017-07-09','2017-07-31',13,0.0,1125.0,'L21']
    ]
    to_insert = [
        [19342,'t','2017-06-20','2017-06-23',3,0.0,1125.0,'L7'],
        [19342,'f','2017-06-23','2017-06-26',3,0.0,1125.0,'A'],
        [19342,'t','2017-06-26','2017-06-30',3,0.0,1125.0,'L7'],
        [19342,'t','2017-07-09','2017-07-31',13,0.0,1125.0,'L21']
    ]
    to_delete = [123,125,126]
    return CreateMock(new, old, to_insert, to_delete) 



'''
New : Has Data
Old : Has Data
Case 16: Extension 
'''
def Mock16():
    old = [
        [123,19342,'t','2017-06-20','2017-06-30',11,0.0,1125.0,'L7'],
        [124,19342,'f','2017-07-01','2017-07-08',8,0.0,1125.0,'A'],
        [125,19342,'t','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [126,19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'A']
    ]
    new = [
        [19342,'t','2017-06-20','2017-07-05',11,0.0,1125.0,'L21'],
        [19342,'f','2017-07-05','2017-07-08',8,0.0,1125.0,'A'],
        [19342,'t','2017-07-09','2017-07-28',15,0.0,1125.0,'L21'],
        [19342,'t','2017-07-28','2017-07-31',8,0.0,1125.0,'A']
    ]
    to_insert = [
        [19342,'t','2017-06-20','2017-07-05',11,0.0,1125.0,'L21'],
        [19342,'f','2017-07-05','2017-07-08',8,0.0,1125.0,'A'],
        [19342,'t','2017-07-09','2017-07-28',15,0.0,1125.0,'L21'],
        [19342,'t','2017-07-28','2017-07-31',8,0.0,1125.0,'A']
    ]
    to_delete = [123,124,125,126]
    return CreateMock(new, old, to_insert, to_delete)     

'''
New : Has Data
Old : Has Data
Case 17: Annulation of first dates, re-rent but subdivised 
'''
def Mock17():
    old = [
        [123,19342,'f','2017-06-20','2017-06-30',11,0.0,1125.0,'A'],
        [124,19342,'f','2017-07-01','2017-07-08',8,0.0,1125.0,'A'],
        [125,19342,'t','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [126,19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'A']
    ]
    new = [
        [19342,'t','2017-06-20','2017-06-24',11,0.0,1125.0,'L7'],
        [19342,'t','2017-06-24','2017-06-27',11,0.0,1125.0,'L7'],
        [19342,'f','2017-06-27','2017-06-30',11,0.0,1125.0,'A'],
        [19342,'t','2017-06-30','2017-07-05',11,0.0,1125.0,'L7'],
        [19342,'f','2017-07-05','2017-07-08',8,0.0,1125.0,'A'],
        [19342,'t','2017-07-09','2017-07-28',15,0.0,1125.0,'L21'],
        [19342,'t','2017-07-28','2017-07-31',8,0.0,1125.0,'A']
    ]
    to_insert = [
        [19342,'t','2017-06-20','2017-06-24',11,0.0,1125.0,'L7'],
        [19342,'t','2017-06-24','2017-06-27',11,0.0,1125.0,'L7'],
        [19342,'f','2017-06-27','2017-06-30',11,0.0,1125.0,'A'],
        [19342,'t','2017-06-30','2017-07-05',11,0.0,1125.0,'L7'],
        [19342,'f','2017-07-05','2017-07-08',8,0.0,1125.0,'A'],
        [19342,'t','2017-07-09','2017-07-28',15,0.0,1125.0,'L21'],
        [19342,'t','2017-07-28','2017-07-31',8,0.0,1125.0,'A']
    ]
    to_delete = [123,124,125,126]
    return CreateMock(new, old, to_insert, to_delete)     


'''
New : Has Data
Old : Has Data
Case 18:
'''
def Mock18():
    old = [
        [123,19342,'f','2017-06-20','2017-06-30',11,0.0,1125.0,'A'],
        [124,19342,'f','2017-07-01','2017-07-08',8,0.0,1125.0,'A'],
        [125,19342,'t','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [126,19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'A']
    ]
    new = [
        [19342,'f','2017-06-20','2017-06-30',11,0.0,1125.0,'A'],
        [19342,'t','2017-07-01','2017-07-05',8,0.0,1125.0,'L7'],
        [19342,'t','2017-07-05','2017-07-09',8,0.0,1125.0,'L7'],
        [19342,'t','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'L7'],
        [19342,'f','2017-08-01','2017-08-14',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-14','2017-08-18',8,0.0,1125.0,'L7'],
        [19342,'f','2017-08-18','2017-08-21',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-21','2017-08-31',8,0.0,1125.0,'L21'],
    ]
    to_insert = [
        [19342,'t','2017-07-01','2017-07-05',8,0.0,1125.0,'L7'],
        [19342,'t','2017-07-05','2017-07-09',8,0.0,1125.0,'L7'],
        [19342,'t','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'L7'],
        [19342,'f','2017-08-01','2017-08-14',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-14','2017-08-18',8,0.0,1125.0,'L7'],
        [19342,'f','2017-08-18','2017-08-21',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-21','2017-08-31',8,0.0,1125.0,'L21']
    ]
    to_delete = [124,125,126]
    return CreateMock(new, old, to_insert, to_delete)     

'''
New : Has Data
Old : Has Data
Case 19: Annulation of first dates, re-rent but subdivised 
'''
def Mock19():
    old = [
        [120,19342,'f','2017-06-20','2017-06-30',11,0.0,1125.0,'A'],
        [121,19342,'t','2017-07-01','2017-07-05',8,0.0,1125.0,'L7'],
        [122,19342,'t','2017-07-05','2017-07-09',8,0.0,1125.0,'L7'],
        [123,19342,'t','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [124,19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'L7'],
        [125,19342,'f','2017-08-01','2017-08-14',8,0.0,1125.0,'A'],
        [126,19342,'t','2017-08-14','2017-08-18',8,0.0,1125.0,'L7'],
        [127,19342,'f','2017-08-18','2017-08-21',8,0.0,1125.0,'A'],
        [128,19342,'t','2017-08-21','2017-08-31',8,0.0,1125.0,'L21']
    ]
    new = [
        [19342,'t','2017-06-20','2017-06-30',11,0.0,1125.0,'L21'],
        [19342,'t','2017-07-01','2017-07-09',8,0.0,1125.0,'L21'],
        [19342,'t','2017-07-09','2017-07-28',15,0.0,1125.0,'L21'],
        [19342,'f','2017-07-28','2017-07-31',8,0.0,1125.0,'A'],
        [19342,'f','2017-08-01','2017-08-14',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-14','2017-08-18',8,0.0,1125.0,'L7'],
        [19342,'t','2017-08-18','2017-08-31',8,0.0,1125.0,'L21'],
    ]
    to_insert = [
        [19342,'t','2017-06-20','2017-06-30',11,0.0,1125.0,'L21'],
        [19342,'t','2017-07-01','2017-07-09',8,0.0,1125.0,'L21'],
        [19342,'t','2017-07-09','2017-07-28',15,0.0,1125.0,'L21'],
        [19342,'f','2017-07-28','2017-07-31',8,0.0,1125.0,'A'],
        [127,19342,'f','2017-08-18','2017-08-21',8,0.0,1125.0,'A'],
        [128,19342,'t','2017-08-21','2017-08-31',8,0.0,1125.0,'L21']
    ]
    to_delete = [120,121,122,123,124,127,128]
    return CreateMock(new, old, to_insert, to_delete)  

'''
New : Has Data
Old : Has Data
Case 20: Cross months
'''
def Mock20():
    old = [
        [123,19342,'t','2017-06-20','2017-06-30',11,0.0,1125.0,'L7'],
        [124,19342,'t','2017-07-01','2017-07-08',8,0.0,1125.0,'A'],
        [125,19342,'f','2017-07-09','2017-07-23',15,0.0,1125.0,'L21'],
        [126,19342,'t','2017-07-24','2017-07-31',8,0.0,1125.0,'A']
    ]
    new = [
        [19342,'t','2017-06-20','2017-06-30',11,0.0,1125.0,'L7'],
        [19342,'t','2017-07-01','2017-07-08',8,0.0,1125.0,'A'],
        [19342,'f','2017-07-09','2017-07-23',15,0.0,1125.0,'A'],
        [19342,'t','2017-07-23','2017-08-12',8,0.0,1125.0,'L21'],
        [19342,'f','2017-08-12','2017-08-18',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-18','2017-08-24',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-24','2017-08-31',8,0.0,1125.0,'L7']
    ]
    to_insert = [
        [19342,'t','2017-07-23','2017-08-12',8,0.0,1125.0,'L21'],
        [19342,'f','2017-08-12','2017-08-18',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-18','2017-08-24',8,0.0,1125.0,'A'],
        [19342,'t','2017-08-24','2017-08-31',8,0.0,1125.0,'L7']
    ]
    to_delete = [124]
    return CreateMock(new, old, to_insert, to_delete) 
   