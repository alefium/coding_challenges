# coding_challenges
Pyhton and SQL coding challenges


## SQL Challenge
It has been done using PostgreSQL.

In the script you can find the code which creates the tables.
You can decomment it in order to run it.

Then there is the query which perform the requested operation.
The result is the expected one.


## Python challenge
It has been done using Python 3.7

There are 3 files:
- python_challenge.py which contains the funtions I wrote.
- python_challenge_create_table.py Running it you create the SQL table we need to populate with the given data.
- python_challenge_populate_table.py This is the file which needs to be run every time we have new data to ingest into the SQL table.

To run the files you need to provide the password of your database, your API key and, in case, change the database name, schema and file path in the relative spaces, according with your system.
You should run first the file called python_challenge_create_table.py.
And then run python_challenge_populate_table.py every time you have a new orders.xlsx file.

I performed all the requested operations and also the bonus points.

### Assumptions:
- The name of the file is every day the same.
- The order_id is unique. For this reason I check the presence in the table of a record with the same order_id before ingesting the data. If I find a record with the same order_id I don't ingest the new one.
But I didn't make this field unique in the SQL code because we can have restatements and corrections and hence we can use the field timerange_transaction in order to remove the validity of a previous record without losing it.
- We want to keep in the table all the information we get from the API.
- Running requests to the API is costly and hence we would like to run them as little as possible. For this reason I collected the unique set of zipcodes in order to avoid requesting 2 times the localization data to the API for the same zipcode, situation which can happen in case we have two records with the same zipcode.
I prefer this solution for the reason just explained but the API requests can be done in multiple other ways.

### Additional explanation:
We run first a multiple API request, if it fails we run a series of single ones, one for each zipcode, we log the errors and in this way we can find which one created problems.
We then ingest the data without localization information for the zipcodes that got errors.

In order to schedule the run of this script every day there are multiple solutions. 
There are python libraries for it as well as tools.
To do it I would use an automation server like Jenkins or a workflow management platform like Airflow.
