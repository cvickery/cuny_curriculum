""" Create my own copy of crse_equiv_tbl.
    Then I can lookup courses where the equivalent_course_group is bogus one way or another.
    Working on answering the question, "What is a cross-listed course?""
"""
import csv
import sys

from collections import namedtuple

import psycopg2
from psycopg2.extras import NamedTupleCursor
num_rows = 0;
conn = psycopg2.connect('dbname=vickery')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)
cursor.execute("""
  drop table if exists crse_equiv_tbl;
  create table crse_equiv_tbl (
    equivalent_course_group integer primary key,
    description text)
""")
with open('./QNS_CCV_CRSE_EQUIV_TBL_7102.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  raw = next(csv_reader)  # skip pre-header row
  raw = next(csv_reader, False) # header row
  raw[0] = raw[0].replace('\ufeff', '')
  Equiv_Table_Row = namedtuple('Equiv_Table_Row', [val.lower().replace(' ', '_').replace('/', '_') for val in raw])
  raw = next(csv_reader, False) # first data row
  while raw:
    num_rows += 1
    print(f'{num_rows}\r', file=sys.stderr, end='')
    row = Equiv_Table_Row._make(raw)
    try:
      int(row.equivalent_course_group)
      cursor.execute('insert into crse_equiv_tbl values (%s, %s)', (row.equivalent_course_group, row.description))
    except:
      print('Invalid Index:', row)
    raw = next(csv_reader, False) # next data row
conn.commit()
conn.close()
