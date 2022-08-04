#! /usr/local/bin/python3
""" Build three tables:
      Local Table               CUNYfirst Query     Description
      cuny_subplans             ACAD_SUBPLANS       institution x plan x subplan
      cuny_plan_enrollments     ACAD_PLAN_ENRL      institution x plan x enrollment
      cuny_subplan_enrollments  ACAD_SUBPLAN_ENRL   institution x plan x subplan x enrollment

    These tables are designing to identify what subplans belong to which plans when a Scribe block
    calls for a nested block from a Major or Minor. In addition, they provided enrollment data for
    inclusion in the metadata for requirement blocks. The table names were chosen to avoid conflict
    with tables containing overlapping information already in use in the cuny_curriculum database.
"""
import csv
import psycopg
import sys

from collections import namedtuple
from pathlib import Path
from psycopg.rows import namedtuple_row

# Since the cuny_curriculum tables are just copies of the CUNYfirst queries, the query_files dict
# allows us to build all the local tables in a uniform way.
query_files = {'cuny_subplans': 'ACAD_SUBPLANS',
               'cuny_plan_enrollments': 'ACAD_PLAN_ENRL',
               'cuny_subplan_enrollments': 'ACAD_SUBPLAN_ENRL'}

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:

    for table_name, query_name in query_files.items():
      latest = None
      csv_files = Path('.').glob(f'{query_name}*')

      for csv_file in csv_files:
        if latest is None or latest.stat().st_mtime < csv_file.stat().st_mtime:
          latest = csv_file

      with open(latest) as csv_file:
        reader = csv.reader(csv_file)
        for line in reader:
          if reader.line_num == 1:
            cols = [col.lower().replace(' ', '_').replace('-', '').replace('academic_', '')
                    for col in line]
            Row = namedtuple('Row', cols)
            col_defs = ''
            for col in cols:
              col_defs += 'enrollment int,\n' if col.startswith('count') else f'{col} text,\n'
              pkey = ['institution', 'plan']
              if 'subplan' in cols:
                pkey.append('subplan')
              pkey = ', '.join(pkey)
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
