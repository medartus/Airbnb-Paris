# Airbnb - Paris
## Getting Started
1. Install Python modules
```
pip install -r requirements.txt
```

2. Create a `dev.env` file in the **root folder** with the following content:
```
POSTGRESQL_HOST=
POSTGRESQL_USER=
POSTGRESQL_PASSWORD=
POSTGRESQL_DATABASE=
```

To access and visualize the database, you can use [pgAdmin](https://www.pgadmin.org/download/).

## Organization of Files and Folders

- **datasets**: Regroup all datasets files
  - **datasets/listings**: Regroup listings datasets
  - **datasets/reviews**: Regroup reviews datasets
  - **datasets/calendar**: Regroup calendars datasets
- **tests**: All the tests files
- **notebook**: All the files containing ideas to be implemented

## Getting Started

1. Create a table `calendars` on your database :
``` SQL
CREATE TABLE public.calendars
(
    cal_key serial,
    listing_id integer,
    available text COLLATE pg_catalog."default",
    start_date date,
    end_date date,
    num_day integer,
    minimum_nights double precision,
    maximum_nights double precision,
    label text COLLATE pg_catalog."default",
    validation boolean DEFAULT false,
    proba double precision,
    ext_validation double precision DEFAULT 0.0,
    CONSTRAINT calendars_pkey PRIMARY KEY (cal_key)
)
```

2. Create a table `listings` on your database :
``` SQL
CREATE TABLE public.listings
(
    id integer NOT NULL,
    listing_url text COLLATE pg_catalog."default",
    scrape_id bigint,
    last_scraped text COLLATE pg_catalog."default",
    name text COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    neighborhood_overview text COLLATE pg_catalog."default",
    host_id integer,
    host_acceptance_rate text COLLATE pg_catalog."default",
    host_listings_count integer,
    neighbourhood text COLLATE pg_catalog."default",
    neighbourhood_cleansed text COLLATE pg_catalog."default",
    neighbourhood_group_cleansed text COLLATE pg_catalog."default",
    latitude double precision,
    longitude double precision,
    property_type text COLLATE pg_catalog."default",
    room_type text COLLATE pg_catalog."default",
    minimum_nights integer,
    maximum_nights integer,
    calendar_updated text COLLATE pg_catalog."default",
    has_availability text COLLATE pg_catalog."default",
    availability_365 integer,
    calendar_last_scraped text COLLATE pg_catalog."default",
    number_of_reviews integer,
    first_review text COLLATE pg_catalog."default",
    last_review text COLLATE pg_catalog."default",
    license text COLLATE pg_catalog."default",
    instant_bookable text COLLATE pg_catalog."default",
    calculated_host_listings_count integer,
    reviews_per_month double precision,
    CONSTRAINT id PRIMARY KEY (id)
)
```

2. Create a table `results` on your database :
``` SQL
CREATE TABLE public.results
(
    extraction_date date,
    listing_id integer,
    past12_m50 integer,
    past12_m75 integer,
    past12_m95 integer,
    past12_m100 integer,
    past12_m95_e75 integer,
    past12_m100_e75 integer,
    civil_m50 integer,
    civil_m75 integer,
    civil_m95 integer,
    civil_m100 integer,
    civil_m95_e75 integer,
    civil_m100_e75 integer,
    predict_m50 integer,
    predict_m75 integer,
    predict_m95 integer
)
```

### Daily execution

This project has been created in such a way that it can be run every day. To process the data present on InsideAirbnb yesterday, just run the `Daily.py` file. To automate this execution, you can schedule it. 

> For Windows, you can for example follow [this tutorial](https://www.jcchouinard.com/python-automation-using-task-scheduler/).

On average, the script execution time to retrieve the files and do the processing is 30 minutes. Your computer will still be usable because Python uses only one core to run.