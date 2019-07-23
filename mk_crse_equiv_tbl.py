""" Create copy of CUNYfirst crse_equiv_tbl.
      Originally so I could lookup courses where the equivalent_course_group is bogus in one way or
      another. Working on answering the question, “What is a cross-listed course?”

      Now it’s just to provide a resource for elaborating on course catalog descriptions in the app.
      Cross-listing is handled by a single course_id with multiple offer_nbrs.

      It would be interesting, for example, to see what Pathways courses are designated by virtue of
      being part of an equivalence group rather than having been reviewed by the CCCRC.
"""
import os
import csv
import sys
import argparse

from collections import namedtuple

import psycopg2
from psycopg2.extras import NamedTupleCursor

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')
args = parser.parse_args()

terminal = open(os.ttyname(0), 'wt')

num_rows = 0
conn = psycopg2.connect('dbname=cuny_courses')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)

cursor.execute("""
  drop table if exists crse_equiv_tbl cascade;
  create table crse_equiv_tbl (
    equivalent_course_group integer primary key,
    description text)
""")

total_rows = sum(1 for line in open('./latest_queries/QNS_CV_CRSE_EQUIV_TBL.csv'))
with open('./latest_queries/QNS_CV_CRSE_EQUIV_TBL.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  raw = next(csv_reader, False)   # header row
  raw[0] = raw[0].replace('\ufeff', '')
  Equiv_Table_Row = namedtuple('Equiv_Table_Row',
                               [val.lower().replace(' ', '_').replace('/', '_') for val in raw])
  raw = next(csv_reader, False)   # first data row
  while raw:
    num_rows += 1
    if args.progress and 0 == num_rows % 1000:
      print(f'{num_rows:,} / {total_rows:,}\r', end='', file=terminal)
    row = Equiv_Table_Row._make(raw)
    try:
      int(row.equivalent_course_group)
      cursor.execute('insert into crse_equiv_tbl values (%s, %s)', (row.equivalent_course_group,
                                                                    row.description))
    except ValueError:
      print('Invalid Index:', row)
    raw = next(csv_reader, False)   # next data row
  if args.progress:
    print(file=terminal)
conn.commit()
conn.close()
