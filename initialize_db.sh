#! /usr/local/bin/bash

# Run the sequence of sql and python scripts to create and initialize the cuny_courses database.

# Default is to dump and restore events. But -n or --no-events suppresses it.
do_events=1
if [ $# -gt 0 ]
then
  if [ $# -eq 1 -a $1 = --no-events -o $1 = -n ]
  then do_events=0
  else
    echo "usage: $0 [--no-events]"
    exit 1
  fi
fi

if [ $do_events -eq 1 ]
then
  # If the db has no events table (yet), re-use the existing events-dump.sql if there is one, or
  # create an empty one.
  touch events-dump.sql
  echo -n SAVE EVENTS TABLE ...
  # events-dump.sql won’t be changed if "no matching tables found"
  pg_dump --data-only --table=events -f events-dump.sql cuny_courses
  # Convert old-style group numbers (ints) to floats
  #   This can away once all old-style reviews have been converted.
  python3 fix_events_dump.py < events-dump.sql > t.sql
  mv t.sql events-dump.sql
  echo done.
fi

echo BEGIN INITIALIZATION
echo -n DROP/CREATE cuny_courses ... | tee init_psql.log
dropdb cuny_courses >> init_psql.log
createdb cuny_courses >> init_psql.log
echo done.

echo -n CREATE TABLE updates ... | tee -a init_psql.log
psql cuny_courses < updates.sql >> init_psql.log
echo done.

# The following is the organizational structure of the University:
#   Students are undergraduate or graduate (careers) at a college
#   Colleges own divisions (groups/schools)
#   Divisions own departments (organizations)
#   Departments own disciplines (subjects)
#   Disciplines map to CUNY subjects (external subject areas)
#   Disciplines have courses
#   Courses have a requirement designation
#
# The sequence of initializations, however, does not quite follow this
# structure:
#   Careers references institutions, so create institutions first
#   Divisions references departments, so create departments first
#
echo -n CREATE TABLE institutions... | tee -a init_psql.log
psql cuny_courses < cuny_institutions.sql >> init_psql.log
echo done.

# Python scripts process query results, so check that they are all present
# and report any mismatched dates.

echo -n CHECK QUERY FILES... | tee init.log
./check_query_dates.sh > init.log
if [ $? -ne 0 ]
  then echo "WARNING: mismatched dates."
  else echo OK.
fi

# Now regenerate the tables that are based on query results
#
echo -n CREATE TABLE cuny_careers... | tee -a init.log
python3 cuny_careers.py >> init.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n CREATE TABLE cuny_departments... | tee -a init.log
python3 cuny_departments.py >> init.log
if [ $? -ne 0 ]
  then echo  -e '\nFAILED!'
       exit
fi
echo done.

echo -n CREATE TABLE cuny_divisions... | tee -a init.log
python3 cuny_divisions.py --active_only >> init.log
if [ $? -ne 0 ]
  then echo  -e '\nFAILED!'
       exit
fi
echo done.

echo -n CREATE TABLE cuny_subjects... | tee -a init.log
python3 cuny_subjects.py >> init.log
if [ $? -ne 0 ]
  then echo  -e '\nFAILED!'
       exit
fi
echo done.

echo -n CREATE TABLE designations... | tee -a init.log
python3 designations.py >> init.log
if [ $? -ne 0 ]
  then echo  -e '\nFAILED!'
       exit
fi
echo done.

echo -n CREATE TABLE crse_quiv_tbl... | tee -a init.log
python3 mk_crse_equiv_tbl.py >> init.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n CREATE TABLE courses... | tee -a init_psql.log
psql cuny_courses < create_courses.sql >> init_psql.log
psql cuny_courses < view_courses.sql >> init_psql.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n POPULATE courses... | tee -a init.log
python3 populate_courses.py --progress >> init.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n CHECK component contact hours... | tee -a init.log
python3 check_total_hours.py > check_contact_hours.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

# Transfer rules
echo -n CREATE TABLE review_status_bits... | tee -a init_psql.log
psql cuny_courses < review_status_bits.sql >> init_psql.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n CREATE TABLE rule_groups, source_courses, destination_courses... | tee -a init_psql.log
psql cuny_courses < create_rule_groups.sql >> init_psql.log
psql cuny_courses < view_rules.sql >> init_psql.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n POPULATE rule_groups... | tee -a init.log
python3 populate_rule_groups.py --progress --report >> init.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n CHECK bogus rules... | tee -a init.log
python3 bogus_rules.py --progress >> init.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

# Managing the rule review process
echo -n CREATE TABLE sessions... | tee -a init_psql.log
psql cuny_courses < sessions.sql >> init_psql.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

#echo CREATE TABLE pending_reviews...
#echo CREATE TABLE event_types...
echo -n CREATE TABLE events... | tee -a init_psql.log
psql cuny_courses < reviews.sql >> init_psql.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

if [ $do_events -eq 1 ]
then
  echo -n RESTORE previous events... | tee -a init_psql.log
  psql cuny_courses < events-dump.sql >> init_psql.log
  if [ $? -ne 0 ]
    then echo -e '\nFAILED!'
         exit
  fi
  echo done.

  echo -n UPDATE statuses... | tee -a init.log
  python3 update_statuses.py >> init.log
  if [ $? -ne 0 ]
    then echo -e '\nFAILED!'
         exit
  fi
  echo done.
fi
echo INITIALIZATION COMPLETE
