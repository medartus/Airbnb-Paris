import pandas as pd


def set_label(period):
    # Available
    if period['available'] == 't':
        return "A"
    # Closed
    else:
        # Closed outside min or max:
        if period['num_day'] < period['minimum_nights']:
            return "MIN"
        elif period['num_day'] > period['maximum_nights']:
            return "MAX"
        # Closed inside min or max
        else:
            # Closed inside min or max, less than 7 days
            if period['num_day'] < 7:
                return "L7"
            # Closed inside min or max, less than 14 days ( between 7 and 14)
            elif period['num_day'] < 14:
                return "L14"
            # Closed inside min or max, less than 14 days ( between 14 and 21)
            elif period['num_day'] < 21:
                return "L21"
            # Closed inside min or max, more than 21 days
            else:
                return "M21"

def labelize(calendar_per):
    calendar_per['label'] = calendar_per.apply(lambda period: set_label(period), axis=1)
    return calendar_per

calendar_per = pd.read_csv("./datasets/altered/calendar_periods.csv")
labelized = labelize(calendar_per)
labelized.to_csv("./datasets/altered/labelized_calendar_periods.csv")

print(labelized)