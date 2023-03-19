#! /usr/local/bin/python3
"""Create sessions table for 2010 to now.

Renaming CUNYfirst fields:
  Institution             institution
  Term                    term
  Session                 session
  Session Beginning Date  start_classes
  Session End Date        end_classes
  First Date to Enroll    early_enrollment
  Open Enrollment Date    open_enrollment
  Last Date to Enroll     last_enrollment
  Census Date             census_date

"""
import csv
import psycopg

from collections import namedtuple
from psycopg.rows import namedtuple_row

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor() as cursor:

    cursor.execute("""
    drop table if exists cuny_sessions;
    create table cuny_sessions (
    institution           text,
    term                  integer,
    session               text,
    early_enrollment      date default null,
    open_enrollment       date default null,
    last_enrollment       date default null,
    classes_start         date default null,
    census_date           date default null,
    classes_end           date default null,
    primary key (institution, term, session)
    )
    """)
    csv_to_db = {'first_date_to_enroll': 'early_enrollment',
                 'open_enrollment_date': 'open_enrollment',
                 'last_date_to_enroll': 'last_enrollment',
                 'session_beginning_date': 'classes_start',
                 'census_date': 'census_date',
                 'session_end_date': 'classes_end'
                 }
    with open('./latest_queries/QNS_CV_SESSION_TABLE.csv') as sess:
      reader = csv.reader(sess)
      for line in reader:
        if reader.line_num == 1:
          cols = [c.lower().replace(' ', '_') for c in line]
        else:
          row = dict()
          for index, value in enumerate(line):
            row[cols[index]] = value
          if row['career'].startswith('U'):
            column_names = ['institution', 'term', 'session']
            placeholders = '%s, %s, %s'
            values = [row['institution'], row['term'], row['session']]
            # Handle missing dates
            for field in ['first_date_to_enroll', 'open_enrollment_date', 'last_date_to_enroll',
                          'session_beginning_date', 'census_date', 'session_end_date']:
                if row[field]:
                  column_names.append(csv_to_db[field])
                  placeholders += ', %s'
                  values.append(row[field])
            column_names = ', '.join(column_names)
            cursor.execute(f"""
            insert into cuny_sessions ({column_names}) values ({placeholders})
            """, values)
