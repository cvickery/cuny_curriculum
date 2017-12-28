# CUNY Courses database
These are scripts for managing a shadow database of the course catalog and transfer rules for the
City University of New York (CUNY). The enterprise management system for CUNY is called CUNYfirst.
The shadow database is built by running queries on CUNYfirst to extract the information needed to
construct and update the shadow database, and then running the _initialize\_db.sh_ script to update
the local database.

The first application to use the database is [https://github.com/cvickery/transfer-app](Transfer
App), but other apps might be built on it in the future (or not).