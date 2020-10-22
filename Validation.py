import pandas as pd
import time
from datetime import timedelta


def get_last_day(calendar):

    calendar['end'] = pd.to_datetime(calendar.end)
    calendar = calendar.sort_values(by="end")
    last_day = calendar['end'].iloc[0]

    return last_day

def sort_reviews(reviews, last_day):

    reviews["date"] = pd.to_datetime(reviews.date)
    reviews = reviews.sort_values(by="date")
    reviews = reviews[reviews.date >= last_day]

    return reviews
        
        
def validate_period(review,calendar):
    try:
        validations = calendar.loc[[review.listing_id]]
        # validations = validations[ (validations.validation == False) & (validations.available != 't') & (validations.label != 'MIN') & (validations.label != 'MAX')]
        validations = validations[validations.validation == False]
        validations = validations[ (validations.end <= review.date) & (review.date <= validations.end + timedelta(days=14))]

        if not validations.empty:
            validated_index = validations.sort_values(by="num_day").index.values
            calendar.at[validated_index, 'validation'] = True
    except:
        # print(review.listing_id)
        pass
    return calendar

def validate(calendar, reviews):

    last_day = get_last_day(calendar)
    reviews = sort_reviews(reviews, last_day)

    calendar["validation"] = False

    calendar = calendar.set_index(['listing_id','period_id'])
    
    for review in reviews.sort_values('date', ascending=False).itertuples():
        calendar = validate_period(review,calendar)

    return calendar

calendar = pd.read_csv("./datasets/altered/labelized_calendar_periods.csv")
reviews = pd.read_csv("./datasets/reviews/reviews-2020-09.csv")

start_time = time.time()

reviews = reviews.drop(columns=['id','reviewer_id','reviewer_name','comments'])
calendar = calendar[(calendar['end'] <= reviews['date'].max())]
calendar = calendar[(calendar.available != 't') & (calendar.label != 'MIN') & (calendar.label != 'MAX')]

validated_calendar = validate(calendar,reviews)
print("---  %s seconds ---" % (time.time() - start_time))

print(validated_calendar[validated_calendar.validation == True])
validated_calendar[validated_calendar.validation == True].to_csv("./datasets/altered/validated_calendar_periods.csv")
