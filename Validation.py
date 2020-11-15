import pandas as pd
import time
import os
from datetime import timedelta

'''
This function returns the oldest 'end_date' date of the calendar dataset
'''
def get_last_day(calendar):

    calendar['end_date'] = pd.to_datetime(calendar.end_date)
    calendar = calendar.sort_values(by="end_date")
    last_day = calendar['end_date'].iloc[0]

    return last_day


'''
Thanks to the oldest day from the previous function,
this function select only the reviews that appeared after this day, cutting all the previous reviews which are not useful.
'''
def sort_reviews(reviews, last_day):

    reviews["date"] = pd.to_datetime(reviews.date)
    reviews = reviews.sort_values(by="date")
    reviews = reviews[reviews.date >= last_day]

    return reviews


'''
Given a review, this funtion will search for any period of the calendar the review is possibly a validation of.
If there is several, it validates the shortest period.
'''   
def validate_internal_period(review, calendar):
    try:
        # Select possible validation regarding several criteras
        validations = calendar.loc[[review.listing_id]]
        validations = validations[validations.validation == False]
        validations = validations[ (validations.end_date <= review.date) & (review.date <= validations.end_date + timedelta(days=14))]

        # if there is validations, mark the shortest period as validated
        if not validations.empty:
            validated_index = validations.sort_values(by="num_day").index.values
            calendar.at[validated_index, 'validation'] = True
    except:
        pass
    return calendar


'''
Given a review, this funtion will search for any period of the calendar the review is possibly a validation of.
If there is several, it validates the shortest period.
'''   
def validate_external_period(review, calendar):
    try:
        # Select possible validation regarding several criteras
        validations = calendar.loc[[review.listing_id]]
        validations = validations[validations.validation == False]
        validations = validations[validations.ext_validation < review.corresp]
        validations = validations[ (validations.end_date <= review.date) & (review.date <= validations.end_date + timedelta(days=14))]

        # if there is validations, mark the shortest period as validated
        if not validations.empty:
            validated_index = validations.sort_values(by="num_day").index.values
            calendar.at[validated_index, 'ext_validation'] = review.corresp
    except:
        pass
    return calendar

'''
Given a calendar and reviews dataframes,
this function will iterate over each interesting review (after sorting)
to search possible validation from this review and update the result in calendar.
'''
def validateInternalCalendar(calendar, reviews):

    last_day = get_last_day(calendar)
    reviews = sort_reviews(reviews, last_day)


    calendar['period_id'] = calendar.index
    calendar = calendar.set_index(['listing_id','period_id'])

    newCalendar = calendar[(calendar['end_date'] <= reviews['date'].max())]
    newCalendar = newCalendar[(newCalendar.available != 't') & (newCalendar.label != 'MIN') & (newCalendar.label != 'MAX')]
    
    newCalendar["validation"] = False
    
    for review in reviews.sort_values('date', ascending=False).itertuples():
        newCalendar = validate_internal_period(review,newCalendar)
    
    validatedCalendar = pd.concat([calendar, newCalendar[['validation']]], axis=1)
    validatedCalendar['validation'] = validatedCalendar['validation'].fillna(False)

    validatedCalendar = validatedCalendar.reset_index()
    del validatedCalendar['period_id']

    return validatedCalendar


'''
Given a calendar and reviews dataframes,
this function will iterate over each interesting review (after sorting)
to search possible validation from this review and update the result in calendar.
'''
def validateExternalCalendar(calendar, reviews):

    last_day = get_last_day(calendar)
    reviews = sort_reviews(reviews, last_day)


    calendar['period_id'] = calendar.index
    calendar = calendar.set_index(['listing_id','period_id'])

    newCalendar = calendar[(calendar['end_date'] <= reviews['date'].max())]
    newCalendar = newCalendar[(newCalendar.available != 't') & (newCalendar.label != 'MIN') & (newCalendar.label != 'MAX')]
    
    newCalendar["ext_validation"] = 0
    
    for review in reviews.sort_values('date', ascending=False).itertuples():
        newCalendar = validate_external_period(review,newCalendar)
    
    validatedCalendar = pd.concat([calendar, newCalendar[['ext_validation']]], axis=1)
    validatedCalendar['ext_validation'] = validatedCalendar['ext_validation'].fillna(0)

    validatedCalendar = validatedCalendar.reset_index()
    del validatedCalendar['period_id']

    return validatedCalendar


'''
Given the calendar, and the review file name, it will sort reviews fields and run the validation
'''
def ValidateWithReviews(calendar,filename):
    # open files
    reviews = pd.read_csv("./datasets/reviews/reviews-"+filename+".csv")

    # optimize calendar data to process
    reviews = reviews.drop(columns=['id','reviewer_id','reviewer_name','comments'])

    # Execute validation
    return validateInternalCalendar(calendar,reviews)
    
def ProcessAndSave(fileNameDate,SavedName,calendar):
    exists = os.path.isfile(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv") 
    if exists:
        print(f'--- Used ./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv ---')
        return pd.read_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv",sep=",")
    else:
        start_time = time.time()
        df = ValidateWithReviews(calendar,fileNameDate)
        print(f'--- Validation {fileNameDate} : {time.time() - start_time} ---')
        df.to_csv(f"./datasets/saved/{fileNameDate}/{SavedName}-{fileNameDate}.csv", index_col = False)
        return df

# calendar = pd.read_csv("./datasets/altered/labelized_calendar_periods.csv")
# validated_calendar = ValidateWithReviews(calendar,"reviews-2020-09")
# # Print and save result
# print(validated_calendar[validated_calendar.validation == True])
# validated_calendar.to_csv("./datasets/altered/validated_calendar_periods.csv")