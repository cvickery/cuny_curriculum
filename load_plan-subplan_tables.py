#! /usr/local/bin/python3
""" Build local copies of CUNYfirst plan/subplan/enrollment tables

    These tables are designing to identify what subplans belong to which plans when a Scribe block
    calls for a nested block from a Major or Minor. In addition, they provided enrollment data for
    inclusion in the metadata for requirement blocks. The local table names were chosen to avoid
    conflict with tables containing overlapping information already in use in the cuny_curriculum
    database.

    All table fields are text unless the column name starts with “count” or ends with “date.”
"""
import csv
import psycopg
import sys

from collections import namedtuple
from datetime import date
from pathlib import Path
from psycopg.rows import namedtuple_row

# Since the cuny_curriculum tables are just copies of the CUNYfirst queries, this query_files dict
# allows us to build all the local tables in a uniform way.
#                                                                 Institution x plan x ...
query_files = {'cuny_acad_plan_tbl': 'ACAD_PLAN_TBL',
               'cuny_plan_enrollments': 'ACAD_PLAN_ENRL',         # ... enrollment
               'cuny_acad_subplan_tbl': 'ACAD_SUBPLAN_TBL',
               'cuny_subplan_enrollments': 'ACAD_SUBPLAN_ENRL'}   # ... subplan x enrollment

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:

    for table_name, query_name in query_files.items():
      latest = None
      csv_files = Path('./latest_queries').glob(f'{query_name}*')

      for csv_file in csv_files:
        if latest is None or latest.stat().st_mtime < csv_file.stat().st_mtime:
          latest = csv_file
      print(query_name, latest)

      with open(latest) as csv_file:
        num_rows = len(csv_file.readlines())
      date_str = date.fromtimestamp(latest.stat().st_mtime)

      with open(latest) as csv_file:
        print(f'Loading {table_name} from {latest.name} {date_str} ({num_rows:,} rows)')

        reader = csv.reader(csv_file)
        for line in reader:
          if reader.line_num == 1:
            cols = [col.lower().replace(' ', '_').replace('-', '').replace('academic_', '')
                    for col in line]
            Row = namedtuple('Row', cols)
            col_defs = ''
            for col in cols:
              col_defs += 'enrollment int,\n' if col.startswith('count') \
                  else f'{col} date,\n' if col.endswith('date') \
                  else f'{col} text,\n'
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
