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

# Separate log files depending on host
IFS='. ' read -r -a name <<< $HOSTNAME
this_host=${name[0]}

if [ $do_events -eq 1 ]
then
  echo -n Save events table ...
  pg_dump --data-only --table=events -f events-dump.sql cuny_courses
  echo done.
fi

dropdb cuny_courses > init_psql.$this_host.log
createdb cuny_courses >> init_psql.$this_host.log

echo -n CREATE TABLE updates ...
psql cuny_courses < updates.sql >> init_psql.$this_host.log
echo done.

echo -n CREATE TABLE institutions...
psql cuny_courses < institutions.sql >> init_psql.$this_host.log
echo done.

echo -n CREATE TABLE cuny_subjects...
python3 cuny_subjects.py >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo done.

echo -n CREATE TABLE cuny_careers...
python3 cuny_careers.py >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo done.

echo -n CREATE TABLE designations...
python3 designations.py >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo done.

echo -n CREATE TABLE attributes...
python3 attributes.py >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo done.

echo -n CREATE TABLE course_attributes...
psql cuny_courses < course_attributes.sql >> init_psql.$this_host.log
echo done.

echo -n CREATE TABLE cuny_departments...
python3 cuny_departments.py >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo done.

echo -n CREATE TABLE courses...
psql cuny_courses < create_courses.sql >> init_psql.$this_host.log
python3 populate_courses.py --report >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo done.

echo CREATE TABLE rule_groups...
psql cuny_courses < review_status_bits.sql >> init_psql.$this_host.log
psql cuny_courses < create_rule_groups.sql >> init_psql.$this_host.log
echo "  generate bad id list... "
python3 rule_groups.py --generate --progress >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo "  populate rule_groups... "
python3 rule_groups.py --progress --report >> init.$this_host.log
if [ $? -ne 0 ]
  then echo failed
       exit
fi
echo -e '\ndone.       '

echo -n CREATE TABLE sessions...
psql cuny_courses < sessions.sql >> init_psql.$this_host.log
echo done.

echo CREATE TABLE pending_evaluations...
echo CREATE TABLE event_types...
echo -n CREATE TABLE events...
psql cuny_courses < evaluations.sql >> init_psql.$this_host.log
echo done.

if [ $do_events -eq 1 ]
then
  echo -n Restore previous events...
  psql cuny_courses < events-dump.sql >> init_psql.$this_host.log
  python3 update_statuses.py >> init.$this_host.log
  echo done.
fi
