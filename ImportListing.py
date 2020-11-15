import pandas as pd
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
    "first_review",
    "last_review",
    "license",
    "instant_bookable",
    "calculated_host_listings_count",
    "reviews_per_month"
]

'''
Retrieve the listings file from de dataset folder
'''   
def RetrieveListings(filename):
    listings = pd.read_csv('./datasets/listings/'+filename+'.csv',sep=",")
    return listings[DATABASE_LISTINGS_COLUMNS]

'''
Import listings file in the database in the table listings
'''   
def ImportListings(filename):
    listings = RetrieveListings(filename)
    listings['host_listings_count'] = listings['host_listings_count'].fillna(0) # Mandatory to not have NaN to crash the importation
    listingsList = listings.values.tolist()

    # Update the databse with Listings informations
    DatabaseConnector.InsertOrUpdate('listings',DATABASE_LISTINGS_COLUMNS,listingsList) 

# ImportListings('listings-2020-09.csv')