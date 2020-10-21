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
  - **datasets/listing**: Regroup listings datasets
  - **datasets/review**: Regroup reviews datasets
  - **datasets/calendar**: Regroup calendars datasets
- **tests**: All the tests files that you have

## Getting Started with Power BI

1. Download the latest version of the postgres ODBC driver [here](https://www.postgresql.org/ftp/odbc/versions/msi/) and run the msi file.
2. On Power BI select the `ODBC` data importation.
![Power-BI-to-PostgreSQL-4-1](https://user-images.githubusercontent.com/45569127/96734811-31c8fe00-13bb-11eb-91cd-ab6fccc28ac1.png)


3. Select Data source as `None` and set the connection string with:
```
Driver={PostgreSQL ANSI(x64)};Server=DATABASE_HOST;Port=5432;Database=airbnb
```
Don't forget to change `DATABASE_HOST` with the ip of your database.

![Power-BI-to-PostgreSQL-5](https://user-images.githubusercontent.com/45569127/96735985-6ee1c000-13bc-11eb-9ef5-04a651d02094.png)

4. Add your username and password to get access to the table.