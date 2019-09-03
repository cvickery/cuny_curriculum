#! /usr/local/bin/python3
""" Make a copy of the CUNYfirst Academic Groups table.
"""

import os
import re
import csv
from collections import namedtuple
from datetime import date, datetime

import psycopg2
from psycopg2.extras import NamedTupleCursor

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Institutions that don’t fit our model of undergraduate colleges for within-CUNY transfers.
ignore_institutions = ['CUNY', 'UAPC1', 'MHC01']

# Get list of known departments
# departments = dict()
# cursor.execute("""
#                 select department, institution
#                 from cuny_departments
#                 group by department
#                """)
# for row in cursor.fetchall():
#   if row.institution not in departments.keys():
#     departments[row.institution] = []
#   departments[row.institution].append(row.department)

# Get names, etc. of known CUNY divisions (“academic groups”) and re-create the divisions table
cols = None
cursor.execute('drop table if exists cuny_divisions cascade')
cursor.execute("""create table cuny_divisions (
                    institution text references institutions,
                    division text not null,
                    name text not null,
                    status text not null,
                    effective_date date default('1901-01-01'),
                    primary key (institution, division)
                    )
               """)

with open('./latest_queries/ACADEMIC_GROUPS.csv') as csv_file:
  csv_reader = csv.reader(csv_file)
  for line in csv_reader:
    if cols is None:
      cols = [c.lower().replace(' ', '_') for c in line]
      Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.institution in ignore_institutions:
        continue
      cursor.execute(f"""insert into cuny_divisions values(
                           '{row.institution}',
                           '{row.academic_group}',
                           '{row.description}',
                           '{row.status}',
                           '{row.effective_date}'
                          )
                      """)
db.commit()
db.close()
