#! /usr/local/bin/bash

# Run the sequence of sql and python scripts to create and initialize the cuny_courses database.

echo -n CREATE TABLE institutions...
psql cuny_courses < institutions.sql > out
echo done.

echo -n CREATE TABLE cuny_subjects...
python3 cuny_subjects.py >> out
echo done.

echo -n CREATE TABLE cuny_careers...
python3 cuny_careers.py >> out
echo done.

echo -n CREATE TABLE designations...
python3 designations.py >> out
echo done.

echo -n CREATE TABLE attributes...
python3 attributes.py >> out
echo done.

echo -n CREATE TABLE course_attributes...
psql cuny_courses < course_attributes.sql >> out
echo done.

echo -n CREATE TABLE cuny_departments...
python3 cuny_departments.py >> out
echo done.

echo -n CREATE TABLE courses...
psql cuny_courses < create_courses.sql >> out
python3 populate_courses.py --report >> out
echo done.

echo -n CREATE TABLE transfer_rules...
psql cuny_courses < evaluation_states.sql
python3 transfer_rules.py >> out
echo done.

echo -n CREATE TABLE sessions...
psql cuny_courses < sessions.sql
echo done.

echo CREATE TABLE pending_evaluations...
echo CREATE TABLE event_types...
echo -n CREATE TABLE events...
psql cuny_courses < evaluations.sql
echo done.