import pandas as pd
import time
from datetime import timedelta

'''
This function returns the oldest 'end' date of the calendar dataset
'''
def get_last_day(calendar):

    calendar['end'] = pd.to_datetime(calendar.end)
    calendar = calendar.sort_values(by="end")
    last_day = calendar['end'].iloc[0]

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
def validate_period(review,calendar):
    try:
        # Select possible validation regarding several criteras
        validations = calendar.loc[[review.listing_id]]
        # validations = validations[ (validations.validation == False) & (validations.available != 't') & (validations.label != 'MIN') & (validations.label != 'MAX')]
        validations = validations[validations.validation == False]
        validations = validations[ (validations.end <= review.date) & (review.date <= validations.end + timedelta(days=14))]

        # if there is validations, mark the shortest period as validated
        if not validations.empty:
            validated_index = validations.sort_values(by="num_day").index.values
            calendar.at[validated_index, 'validation'] = True
    except:
        # print(review.listing_id)
        pass
    return calendar

'''
Given a calendar and reviews dataframes,
this function will iterate over each interesting review (after sorting)
to search possible validation from this review and update the result in calendar.
'''
def validateCalendar(calendar, reviews):

    last_day = get_last_day(calendar)
    reviews = sort_reviews(reviews, last_day)

    calendar["validation"] = False

    calendar = calendar.set_index(['listing_id','period_id'])
    
    for review in reviews.sort_values('date', ascending=False).itertuples():
        calendar = validate_period(review,calendar)

    return calendar

def ValidateWithReviews(calendar,filename):
    # open files
    reviews = pd.read_csv("./datasets/reviews/"+filename+".csv")

    # optimize calendar data to process
    reviews = reviews.drop(columns=['id','reviewer_id','reviewer_name','comments'])
    calendar = calendar[(calendar['end'] <= reviews['date'].max())]
    calendar = calendar[(calendar.available != 't') & (calendar.label != 'MIN') & (calendar.label != 'MAX')]

    # Execute validation
    return validateCalendar(calendar,reviews)
    
calendar = pd.read_csv("./datasets/altered/labelized_calendar_periods.csv")
validated_calendar = ValidateWithReviews(calendar,"reviews-2020-09")
# Print and save result
print(validated_calendar[validated_calendar.validation == True])
import random
validated_calendar['Proba'] = [ random.randint(0,100)/100  for k in validated_calendar.index]
validated_calendar.to_csv("./datasets/altered/validated_calendar_periods.csv")