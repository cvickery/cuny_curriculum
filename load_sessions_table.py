#! /usr/local/bin/python3
""" (Re-)create table of undergraduate sessions.
"""

import csv
import psycopg
import sys

from collections import namedtuple
from psycopg.rows import namedtuple_row

missing_date = '1901-01-01'
with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    with open('./latest_queries/QNS_CV_SESSION_TABLE.csv') as csv_file:
      reader = csv.reader(csv_file)
      for line in reader:
        if reader.line_num == 1:
          col_names = [col_name.lower().replace(' ', '_') for col_name in line]
          Row = namedtuple('Row', col_names)
          schema_rows = []
          for col_name in col_names:
            if col_name == 'term':
              schema_rows.append('term integer')
            elif col_name in ['session_beginning_date',
                              'session_end_date',
                              'open_enrollment_date',
                              'first_date_to_enroll',
                              'last_date_to_enroll',
                              'census_date',
                              'sysdate']:
              schema_rows.append(f'{col_name} DATE')
            else:
              schema_rows.append(f'{col_name} TEXT')
          placeholders = ('%s, ' * len(schema_rows)).strip(' ,')
          schema = ','.join(schema_rows)
          cursor.execute(f"""
          drop table if exists cuny_sessions;
          create table cuny_sessions ({schema}, PRIMARY KEY (institution, career, term, session))
          """)
        else:
          row = Row._make(line)
          values = [missing_date if value == '' else value for value in row]
          cursor.execute(f"""
          insert into cuny_sessions values ({placeholders})
          """, values)
