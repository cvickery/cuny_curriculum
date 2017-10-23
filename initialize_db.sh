#! /usr/local/bin/bash

# Run the sequence of sql and python scripts to create and initialize the cuny_courses database.

# Separate log files depending on host
IFS='. ' read -r -a name <<< $HOSTNAME
this_host=${name[0]}

dropdb cuny_courses > init_psql.$this_host.log
createdb cuny_courses >> init_psql.$this_host.log

echo -n CREATE TABLE institutions...
psql cuny_courses < institutions.sql >> init_psql.$this_host.log
echo done.

echo -n CREATE TABLE cuny_subjects...
python3 cuny_subjects.py > init.$this_host.log
echo done.

echo -n CREATE TABLE cuny_careers...
python3 cuny_careers.py >> init.$this_host.log
echo done.

echo -n CREATE TABLE designations...
python3 designations.py >> init.$this_host.log
echo done.

echo -n CREATE TABLE attributes...
python3 attributes.py >> init.$this_host.log
echo done.

echo -n CREATE TABLE course_attributes...
psql cuny_courses < course_attributes.sql >> init_psql.$this_host.log
echo done.

echo -n CREATE TABLE cuny_departments...
python3 cuny_departments.py >> init.$this_host.log
echo done.

echo -n CREATE TABLE courses...
psql cuny_courses < create_courses.sql >> init_psql.$this_host.log
python3 populate_courses.py --report >> init.$this_host.log
echo done.

echo CREATE TABLE transfer_rules...
psql cuny_courses < evaluation_states.sql >> init_psql.$this_host.log
echo "  generate bad id list... "
python3 transfer_rules.py --generate --progress >> init.$this_host.log
echo "  generate transfer rules... "
python3 transfer_rules.py --progress --report >> init.$this_host.log
echo 'done.       '

echo -n CREATE TABLE sessions...
psql cuny_courses < sessions.sql >> init_psql.$this_host.log
echo done.

echo CREATE TABLE pending_evaluations...
echo CREATE TABLE event_types...
echo -n CREATE TABLE events...
psql cuny_courses < evaluations.sql >> init_psql.$this_host.log
echo done.
