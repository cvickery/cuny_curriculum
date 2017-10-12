#! /usr/local/bin/bash

# Run the sequence of sql and python scripts to create and initialize the cuny_courses database.

dropdb cuny_courses
createdb cuny_courses

echo -n CREATE TABLE institutions...
psql cuny_courses < institutions.sql
echo done.

echo -n CREATE TABLE cuny_subjects...
python3 cuny_subjects.py > init.log
echo done.

echo -n CREATE TABLE cuny_careers...
python3 cuny_careers.py >> init.log
echo done.

echo -n CREATE TABLE designations...
python3 designations.py >> init.log
echo done.

echo -n CREATE TABLE attributes...
python3 attributes.py >> init.log
echo done.

echo -n CREATE TABLE course_attributes...
psql cuny_courses < course_attributes.sql
echo done.

echo -n CREATE TABLE cuny_departments...
python3 cuny_departments.py >> init.log
echo done.

echo -n CREATE TABLE courses...
psql cuny_courses < create_courses.sql
python3 populate_courses.py --report >> init.log
echo done.

echo -n CREATE TABLE transfer_rules...
psql cuny_courses < evaluation_states.sql
echo -n "  Generate bad id list"
python3 transfer_rules.py --generate >> init.log
echo -n "  Generate transfer rules"
python3 transfer_rules.py >> init.log
echo done.

echo -n CREATE TABLE sessions...
psql cuny_courses < sessions.sql
echo done.

echo CREATE TABLE pending_evaluations...
echo CREATE TABLE event_types...
echo -n CREATE TABLE events...
psql cuny_courses < evaluations.sql
echo done.
