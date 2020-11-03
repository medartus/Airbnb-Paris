import datetime
from dateutil import relativedelta
import pandas as pd
import DatabaseConnector

DATABASE_CALENDARS_COLUMNS = [
    "listing_id",
    "period_id",
    "available",
    "start_date",
    "end_date",
    "num_day",
    "minimum_nights",
    "maximum_nights",
    "label",
	"proba",
    "validation"
]

def RestrictNumDay(row,**kwags):
    if 'max_date' in kwags and row['end_date'] >= kwags['max_date']:
        diff = kwags['max_date'] - row['start_date']
        restrictedNumDays = diff.days + 1
        return restrictedNumDays
    if 'min_date' in kwags and row['start_date'] <= kwags['min_date']:
        diff = row['end_date'] -  kwags['min_date']
        restrictedNumDays = diff.days + 1
        return restrictedNumDays
    return row['num_day']


def RetrieveData(queryDate):
    convertedQueryDate = datetime.datetime.strptime(queryDate,"%Y-%m-%d").date()
    nextMonthDate = convertedQueryDate + relativedelta.relativedelta(months=1)
    lastYearDate =  convertedQueryDate - relativedelta.relativedelta(years=1)
    print("SELECT * FROM result where end_date >= '" + str(lastYearDate) + "' and start_date <= '" + str(nextMonthDate) + "'")

    res = DatabaseConnector.Select("SELECT * FROM result where end_date >= '" + str(lastYearDate) + "' and start_date <= '" + str(nextMonthDate) + "'")
    df = pd.DataFrame(res, columns=DATABASE_CALENDARS_COLUMNS)
    return df

def EstimateTimeRented(dataframe, dataframeName, isPast = True):
    tot = dataframe.groupby('listing_id').num_day.sum().rename(dataframeName+"_tot")
    m95 = dataframe[dataframe.proba >= 0.95].groupby('listing_id').num_day.sum().rename(dataframeName+"_m95")
    m100 = dataframe[dataframe.proba == 1.0].groupby('listing_id').num_day.sum().rename(dataframeName+"_m100")

    res = [tot, m95, m100] if isPast else [tot, m95]

    return pd.concat(res, axis=1)

def ExportResult(queryDate):
    convertedQueryDate = datetime.datetime.strptime(queryDate,"%Y-%m-%d").date()
    nextMonthDate = convertedQueryDate + relativedelta.relativedelta(months=1)
    civilDate = datetime.datetime.strptime(queryDate[:4] + "-01-01","%Y-%m-%d").date()

    results = RetrieveData(queryDate)

    results = results[(results.label != "A") & (results.label != "MIN") & (results.label != "MAX")]

    past12 = results[results.start_date <= convertedQueryDate]
    civil = results[(results.end_date >= civilDate) & (results.start_date <= convertedQueryDate)]
    predict = results[results.end_date >= convertedQueryDate]

    past12['num_day'] = past12.apply(RestrictNumDay, max_date = convertedQueryDate, axis=1)
    civil['num_day'] = civil.apply(RestrictNumDay, min_date = civilDate, max_date = convertedQueryDate, axis=1)
    predict['num_day'] = predict.apply(RestrictNumDay, min_date = convertedQueryDate, axis=1)

    estimatedPast12 = EstimateTimeRented(past12,'past12')
    estimatedCivil = EstimateTimeRented(civil,'civil')
    estimatedPredict = EstimateTimeRented(predict,'predict', False)

    return pd.concat([estimatedPast12, estimatedCivil, estimatedPredict], axis=1).fillna(0).astype(int)

# queryDate = '2020-08-28'
# result = ExportResult(queryDate)
# result.to_csv("./datasets/altered/plop.csv")
# result.to_excel("./datasets/altered/plop.xlsx")  