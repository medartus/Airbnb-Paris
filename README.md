# Airbnb - Paris
## Getting Started
1. Install Python modules
```
pip install -r requirements.txt
```
2. Create the Mysql database and copy the database files. Run the batch file : `CreateDatabase.bat`.

To access and visualize the database, you can use [MySQL Workbench](https://dev.mysql.com/downloads/workbench/).

Database informations :
- MYSQL_DATABASE: `airbnb`
- MYSQL_USER: `user`
- MYSQL_PASSWORD: `password`
- MYSQL_ROOT_PASSWORD: `password`

## Organization of Files and Folders

- **datasets**: Regroup all datasets files
  - **datasets/listing**: Regroup listings datasets
  - **datasets/review**: Regroup reviews datasets
  - **datasets/calendar**: Regroup calendars datasets
- **mysql**: Regroup all database files
  - **mysql/aribnb**: Volume from docker airbnb's database
- **tests**: All the tests files that you have
