#! /usr/local/bin/python3
""" Build three tables:
      cuny_subplans             ACAD_SUBPLANS       institution x plan x subplan
      cuny_plan_enrollments     ACAD_PLAN_ENRL      institution x plan x enrollment
      cuny_subplan_enrollments  ACAD_SUBPLAN_ENRL   institution x plan x subplan x enrollment
"""
import csv
import psycopg
import sys

from collections import namedtuple
from pathlib import Path
from psycopg.rows import namedtuple_row

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    query_files = {'cuny_subplans': 'ACAD_SUBPLANS',
                   'cuny_plan_enrollments': 'ACAD_PLAN_ENRL',
                   'cuny_subplan_enrollments': 'ACAD_SUBPLAN_ENRL'}
    for table_name, query_name in query_files.items():
      print(table_name, query_name, end='')

      latest = None
      csv_files = Path('.').glob(f'{query_name}*')
      for csv_file in csv_files:
        if latest is None or latest.stat().st_mtime < csv_file.stat().st_mtime:
          latest = csv_file
      print(f' {latest.name}')
      with open(latest) as csv_file:
        reader = csv.reader(csv_file)
        for line in reader:
          if reader.line_num == 1:
            cols = [col.lower().replace(' ', '_').replace('-', '').replace('academic_', '') for col in line]
            Row = namedtuple('Row', cols)
            col_defs = ''
            for col in cols:
              col_defs += 'enrollment int,\n' if col.startswith('count') else f'{col} text,\n'
              pkey = ['institution', 'plan']
              if 'subplan' in cols:
                pkey.append('subplan')
              pkey = ', '.join(pkey)
            print(col_defs, pkey)
            cursor.execute(f"""
            drop table if exists {table_name};
            create table {table_name} (
              {col_defs}
              primary key ({pkey}))
            """)
            values_clause = ', '.join(['%s'] * len(cols))
            insert_query = f'insert into {table_name} values({values_clause})'
          else:
            row = Row._make(line)
            values = [int(v) if v.isdigit() else v.replace('\'', '’') for v in row]
            cursor.execute(f"""
            {insert_query}
            """, values)
