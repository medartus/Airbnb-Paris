import datetime
from dateutil import relativedelta
import pandas as pd
import DatabaseConnector

DATABASE_CALENDARS_COLUMNS = [
    "listing_id",
    "available",
    "start_date",
    "end_date",
    "num_day",
    "minimum_nights",
    "maximum_nights",
    "label",
    "validation",
	"proba"
    # "ext_validation"
]

DATABASE_RESULTS_COLUMNS = [
	"extraction_date",
    "listing_id",
	"past12_m50",
	"past12_m75",
    "past12_m95",
	"past12_m100",
	"past12_m95_e75",
	"past12_m100_e75",
	"civil_m50",
	"civil_m75",
    "civil_m95",
	"civil_m100",
	"civil_m95_e75",
	"civil_m100_e75",
	"predict_m50",
    "predict_m75",
	"predict_m95"
]

'''
Rectifies the difference in the number of days to rent if the reservation dates are less or more than the deadline
'''
def RestrictNumDay(row,**kwags):
    diff = min(kwags['max_date'],row['end_date']) - max(row['start_date'],kwags['min_date'])
    return diff.days + 1


'''
Retrieve all the data from one year before and one month after a specific date
'''
def RetrieveData(queryDate):
    # Calculate min and max date to get all the data that we want to extract
    convertedQueryDate = datetime.datetime.strptime(queryDate,"%Y-%m-%d").date()
    nextMonthDate = convertedQueryDate + relativedelta.relativedelta(days=30)
    lastYearDate =  convertedQueryDate - relativedelta.relativedelta(years=1)

    # Query the database
    requestedColumns = DatabaseConnector.FormatInsert(DATABASE_CALENDARS_COLUMNS)
    res = DatabaseConnector.Execute(f"SELECT {requestedColumns} FROM calendars where end_date >= '{str(lastYearDate)}' and start_date <= '{str(nextMonthDate)}'")
    df = pd.DataFrame(res, columns=DATABASE_CALENDARS_COLUMNS)
    return df


'''
Create the estimated columns with the different probability aggregates
'''
def EstimateTimeRented(dataframe, dataframeName, isPast = True):
    tot = dataframe.groupby('listing_id').num_day.sum().rename(dataframeName+"_tot")
    m50 = dataframe[dataframe.proba >= 0.5].groupby('listing_id').num_day.sum().rename(dataframeName+"_m50")
    m75 = dataframe[dataframe.proba >= 0.75].groupby('listing_id').num_day.sum().rename(dataframeName+"_m75")
    m95 = dataframe[dataframe.proba >= 0.95].groupby('listing_id').num_day.sum().rename(dataframeName+"_m95")
    m100 = dataframe[dataframe.proba == 1.0].groupby('listing_id').num_day.sum().rename(dataframeName+"_m100")

    # m95_e75 = dataframe[(dataframe.proba >= 0.95) & (dataframe.ext_validation >= 0.75)].groupby('listing_id').num_day.sum().rename(dataframeName+"_m95_e75")
    # m100_e75 = dataframe[(dataframe.proba == 1.0) & (dataframe.ext_validation >= 0.75)].groupby('listing_id').num_day.sum().rename(dataframeName+"_m100_e75")

    res = [tot, m50, m75, m95, m100] if isPast else [tot, m50, m75, m95]

    return pd.concat(res, axis=1)


'''
Given a query date it will:
 - Extract relevant(regarding the date) related data from the db
 - Sort them by labels (only non-zero probas)
 - calculate number of closed days: total, with 95%+ proba, with 100%
   => For the past 12 months AND the actual civil year AND next 30 days
'''
def ExportResult(queryDate):

    # Date calculations
    convertedQueryDate = datetime.datetime.strptime(queryDate,"%Y-%m-%d").date()
    nextMonthDate = convertedQueryDate + relativedelta.relativedelta(days=30)
    lastYearDate =  convertedQueryDate - relativedelta.relativedelta(years=1)
    civilDate = datetime.datetime.strptime(queryDate[:4] + "-01-01","%Y-%m-%d").date()

    # Extract relevant data
    results = RetrieveData(queryDate)
    # Sort by labels
    results = results[(results.label != "A") & (results.label != "MIN") & (results.label != "MAX")]

    # Split data
    past12 = results[results.start_date <= convertedQueryDate]
    civil = results[(results.end_date >= civilDate) & (results.start_date <= convertedQueryDate)]
    predict = results[results.end_date >= convertedQueryDate]

    # Calculation of the number of days for each period
    past12['num_day'] = past12.apply(RestrictNumDay, min_date = lastYearDate, max_date = convertedQueryDate, axis=1)
    civil['num_day'] = civil.apply(RestrictNumDay, min_date = civilDate, max_date = convertedQueryDate, axis=1)
    predict['num_day'] = predict.apply(RestrictNumDay, min_date = convertedQueryDate, max_date = nextMonthDate, axis=1)

    # Calculation of the number of closed days for each period
    estimatedPast12 = EstimateTimeRented(past12,'past12')
    estimatedCivil = EstimateTimeRented(civil,'civil')
    estimatedPredict = EstimateTimeRented(predict,'predict', False)

    return pd.concat([estimatedPast12, estimatedCivil, estimatedPredict], axis=1).fillna(0).astype(int)

def SaveResult(queryDate, result, exportFormatList, filename=None):
    for exportFormat in exportFormatList:
        if exportFormat == "csv":
            result.to_csv(f"./datasets/altered/{filename}.csv")
        if exportFormat == "excel":
            result.to_excel(f"./datasets/altered/{filename}.xlsx")
        if exportFormat == "database":
            result['extraction_date'] = queryDate
            listResult = result.values.tolist()
            DatabaseConnector.Execute(f'DELETE FROM results WHERE extraction_date = {queryDate}')
            DatabaseConnector.Insert(listResult,'results',DATABASE_RESULTS_COLUMNS)

queryDate = '2018-12-01'
result = ExportResult(queryDate)
SaveResult(queryDate,result,['csv','excel'],'TestExport')
# queryDate = '2020-09-28'
# result = ExportResult(queryDate)
# # result.to_csv("./datasets/altered/plop.csv")
# result.to_excel("./datasets/altered/plop.xlsx")