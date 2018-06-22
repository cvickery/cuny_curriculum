""" Compare the equivalence info in the catalog to the base table of all equivalencies.
    This one goes through the CRSE_EQUIV_TBL and see what courses, if any, reference the
    equivalence_course_group field. Lists cases where the text doesn't match. Creates a frequency
    distribution of the nunber of courses that reference a group.

    Working on answering the question, "What is a cross-listed course?""
"""
import csv
import sys

from collections import namedtuple

import psycopg2
from psycopg2.extras import NamedTupleCursor

conn = psycopg2.connect('dbname=vickery')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)

# For each row of the table, see if the equivalent_course_group matches any equiv_course_group in
# course_info. If so, does the description match the equiv_courses string?
#   Display mismatches
#   Count matches
#   Count not-found
num_records = 0
num_found = dict()
num_ok = 0
num_active_not_ok = 0
num_inactive_not_ok = 0
with open('./QNS_CCV_CRSE_EQUIV_TBL_7102.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  raw = next(csv_reader)  # skip pre-header row
  raw = next(csv_reader, False) # header row
  raw[0] = raw[0].replace('\ufeff', '')
  Equiv_Table_Row = namedtuple('Equiv_Table_Row', [val.lower().replace(' ', '_').replace('/', '_') for val in raw])
  raw = next(csv_reader, False) # first data row
  while raw:
    num_records += 1
    if num_records % 50 == 0:
      print(f'{num_records}\r', end='', file=sys.stderr)
      for key in sorted(num_found.keys()):
        print(f'{key:6}:\t{num_found[key]}', file=sys.stderr)

    row = Equiv_Table_Row._make(raw)
    cursor.execute("""select  course_id,
                              offer_nbr,
                              institution,
                              subject,
                              catalog_number,
                              crse_catalog_status,
                              equiv_course_group,
                              equiv_courses
                        from  course_info
                        where equiv_course_group = %s""", (row.equivalent_course_group,))
    if cursor.rowcount in num_found.keys():
      num_found[cursor.rowcount] += 1
    else:
      num_found[cursor.rowcount] = 1
      print([key for key in sorted(num_found.keys())], file=sys.stderr)
    for result in cursor.fetchall():
      if result.equiv_courses == row.description:
        num_ok += 1
      else:
        if result.crse_catalog_status == 'A':
          num_active_not_ok += 1
        else:
          num_inactive_not_ok += 1
        error_info = f'{result.course_id:>6} {result.offer_nbr} {result.institution} {result.subject:<6} '
        error_info += f'{result.crse_catalog_status}'
        print(f'{row.description:<32} {result.equiv_courses:<32} {error_info}')
    raw = next(csv_reader, False) # next data row
print('{} Records\n{} OK\n{} Active Not OK\n{} Inactive Not OK\nFrequency of finds:'.format(
        num_records, num_ok, num_active_not_ok, num_inactive_not_ok))
for key in sorted(num_found.keys()):
  print(f'{key:6}:\t{num_found[key]}')
conn.close()
print(file=sys.stderr)
