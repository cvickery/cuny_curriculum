#! /usr/local/bin/python3
""" Build local copies of CUNYfirst cip_code and plan/subplan/enrollment tables

    All table fields are text unless the column name starts with “count” or ends with “date.”
"""
import csv
import psycopg
import sys

from collections import namedtuple
from datetime import date
from pathlib import Path
from psycopg.rows import namedtuple_row

# For cuny_curriculum tables that are just copies of the CUNYfirst queries, this query_files dict
# allows us to build all the local tables in a uniform way. Unfortunately, adding CIP codes to the
# set of "base tables" made this messy.
#                                                                 Institution x plan x ...
query_files = {'cuny_cip_code_tbl': 'CIP_CODE_TBL',
               'cuny_acad_plan_tbl': 'ACAD_PLAN_TBL',
               'cuny_plan_enrollments': 'ACAD_PLAN_ENRL',
               'cuny_acad_subplan_tbl': 'ACAD_SUBPLAN_TBL',
               'cuny_subplan_enrollments': 'ACAD_SUBPLAN_ENRL'}

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
              col_defs += 'enrollment int,\n' if col.startswith('count_') \
                  else f'{col} date,\n' if col.endswith('date') \
                  else f'{col} text,\n'
            if cols[0] == 'institution':
              pkey = ['institution', 'plan']
              if 'subplan' in cols:
                pkey.append('subplan')
              pkey = 'primary key(' + ', '.join(pkey) + ')'
            else:
              pkey = f'primary key ({cols[0]})'
              print(f'WARNING: Using first column ({cols[0]}) as primary key for {table_name}',
                    file=sys.stderr)
            cursor.execute(f"""
            drop table if exists {table_name};
            create table {table_name} (
              {col_defs}
              {pkey})
            """)

            values_clause = ', '.join(['%s'] * len(cols))
            insert_query = f'insert into {table_name} values({values_clause})'
          else:
            row = Row._make(line)
            values = [int(v) if v.isdigit() else v.replace('\'', '’') for v in row]
            cursor.execute(f"""
            {insert_query}
            """, values)
