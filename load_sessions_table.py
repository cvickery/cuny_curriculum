#! /usr/local/bin/python3
""" (Re-)create table of undergraduate sessions.
"""

import csv
import psycopg
import sys

from psycopg.rows import namedtuple_row

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    with open('./latest_queries/SESSION_TABLE.csv') as csv_file:
      reader = csv.reader(csv_file)
      for line in reader:
        if reader.line_num == 1:
          col_names = [col_name.lower().replace(' ', '_') for col_name in line]
          schema_rows = [f'{col_name} TEXT' for col_name in col_names]
          placeholders = ('%s, ' * len(schema_rows)).strip(' ,')
          schema = ','.join(schema_rows)
          cursor.execute(f"""
          drop table if exists cuny_sessions;
          create table cuny_sessions ({schema}, PRIMARY KEY (institution, career, term, session))
          """)
        else:
          cursor.execute(f"""
          insert into cuny_sessions values ({placeholders})
          """, line)
