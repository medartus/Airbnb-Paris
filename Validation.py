import pandas as pd
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

    validations = calendar[ (calendar.listing_id == review.listing_id) & (calendar.validation == False) & (calendar.available != 't') & (calendar.label != 'MIN') & (calendar.label != 'MAX')]
    validations = validations[ (validations.end <= review.date) & (review.date <= validations.end + timedelta(days=14))]

    if not validations.empty:
        validated_index = validations.sort_values(by="num_day")["period_id"].iloc[0]
        validated_calendar_index = calendar[calendar["period_id"] == validated_index].index[0]
        calendar.loc[validated_calendar_index,"validation"] = True
    return calendar

def validate(calendar, reviews):

    last_day = get_last_day(calendar)
    print(last_day)
    reviews = sort_reviews(reviews, last_day)

    calendar["validation"] = False

    for review in reviews.itertuples(): 
        calendar = validate_period(review,calendar)

    return calendar

calendar = pd.read_csv("./datasets/altered/labelized_calendar_periods.csv")
reviews = pd.read_csv("./datasets/reviews.csv")

validated_calendar = validate(calendar,reviews)
print(validated_calendar[validated_calendar.validation == True])
