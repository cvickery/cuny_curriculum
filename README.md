# CUNY Courses database

These are scripts for managing a PostgreSQL cache of the course catalog and transfer rules for the City University of New York (CUNY). The enterprise management system for CUNY is called CUNYfirst. The Postgres database is built by running queries on CUNYfirst to extract the information needed to construct and update the Postgres database, and then running the `update_db` script to update the local database.
The CUNYfirst queries are scheduled to run periodically, with the generated .csv files delivered to Tumbleweed (CUNY's internal file
sharing system). The update_db script gets the query results from Tumbleweed, checks their integrity, and then re-creates the Postgres db.

The first application to use the database is [Transfer App](https://github.com/cvickery/transfer-app), but other apps might be built on it in the future.
