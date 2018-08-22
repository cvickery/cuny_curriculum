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
  # python3 fix_events_dump.py < events-dump.sql > t.sql
  # mv t.sql events-dump.sql
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

echo -n CREATE TABLE transfer_rules, source_courses, destination_courses... | tee -a init_psql.log
psql cuny_courses < create_transfer_rules.sql >> init_psql.log
psql cuny_courses < view_transfer_rules.sql >> init_psql.log
if [ $? -ne 0 ]
  then echo -e '\nFAILED!'
       exit
fi
echo done.

echo -n POPULATE transfer_rules... | tee -a init.log
python3 populate_transfer_rules.py --progress --report >> init.log
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

# Clear all existing status bits: only status changes from the events table
# will be reflected in the rules table.
cursor.execute('select count(*) as num_rules from transfer_rules where review_status != 0')
print('  Reset status for {:,} rules ...'.format(cursor.fetchone().num_rules))
cursor.execute('update transfer_rules set review_status = 0 where review_status != 0')

# Initialize the bitmasks dict for this script to work from
cursor.execute('select * from review_status_bits')
Event_Type = namedtuple('Event_Type', [d[0] for d in cursor.description])
event_types = map(Event_Type._make, cursor.fetchall())
bitmasks = dict()
for event_type in event_types:
  bitmasks[event_type.abbr] = event_type.bitmask

# Process the events table
cursor.execute('select * from events')
print('  Process {} events ...'.format(cursor.rowcount))
events = cursor.fetchall()
for event in events:
  cursor.execute("""select review_status
                      from transfer_rules
                     where id = %s
                 """, (event.rule_id,))
  review_status = cursor.fetchone().review_status
  # print('status is {}\n  event_type is {}\n  bitmask is {}'.format(status, event.event_type, bitmasks[event.event_type]))
  review_status = review_status | bitmasks[event.event_type]
  # print('new status:', status)
  cursor.execute("""
                  update transfer_rules set review_status = %s
                   where id = %s
               """, (review_status, event.rule_id))
  db.commit()
print('  Done')
db.close()
