import pandas as pd
import time
import DatabaseConnector

# List of columns kept in the database for listings dataset
DATABASE_LISTINGS_COLUMNS = [
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

def RetrieveListings(filename):
    listings = pd.read_csv('./datasets/listings/'+filename,sep=",")
    return listings[DATABASE_LISTINGS_COLUMNS]

def ImportListings(filename):
    start_time = time.time()
    listings = RetrieveListings(filename)
    listings['host_listings_count'] = listings['host_listings_count'].fillna(0) # Mandatory to not have NaN to crash the importation
    listingsList = listings.values.tolist()

    # Update the databse with Listings informations
    DatabaseConnector.InsertOrUpdate('listings',DATABASE_LISTINGS_COLUMNS,listingsList) 
    print("---  %s seconds ---" % (time.time() - start_time))

# ImportListings('listings-2020-09.csv')