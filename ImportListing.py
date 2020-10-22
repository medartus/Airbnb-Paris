import pandas as pd

DATABASE_CONTENT = [
    "id",
    "listing_url",
    "scrape_id",
    "last_scraped",
    "name",
    "description",
    "neighborhood_overview",
    "host_id",
    "host_acceptance_rate",
    "host_listings_count",
    "neighbourhood",
    "neighbourhood_cleansed",
    "neighbourhood_group_cleansed",
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "minimum_nights",
    "maximum_nights",
    "calendar_updated",
    "has_availability",
    "availability_365",
    "calendar_last_scraped",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "first_review",
    "last_review",
    "license",
    "instant_bookable",
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "reviews_per_month"
]


def ImportListings(filename):
    listings = pd.read_csv(filename,sep=",")
    return listings[DATABASE_CONTENT]

def FormatInsert():
    listColumns = "("
    for index in range(len(DATABASE_CONTENT)-1):
        listColumns += DATABASE_CONTENT[index]+","
    listColumns += DATABASE_CONTENT[-1]+")"
    return listColumns

def FormatUpdate():
    listColumns = ""
    for index in range(len(DATABASE_CONTENT)-1):
        listColumns += DATABASE_CONTENT[index]+" = EXCLUDED."+ DATABASE_CONTENT[index]+", "
    listColumns += DATABASE_CONTENT[-1]+" = EXCLUDED."+ DATABASE_CONTENT[-1]
    return listColumns
