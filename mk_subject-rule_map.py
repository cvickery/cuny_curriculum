""" Speed up transfer rule lookups.
    1. Create the eponymous subject-rule map table.
    2. Index the rule_id field of source_courses and destination_courses
"""
import os
import sys
import argparse
import csv

from collections import namedtuple
from datetime import date
from time import perf_counter

import psycopg2
from psycopg2.extras import NamedTupleCursor

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')  # to stderr
args = parser.parse_args()

app_start = perf_counter()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Using the subject_rule_map table (instead of putting source subjects in a colon-delimited string
# in each rule) gives a 1.97 speedup of rule lookups in do_form_2()
if args.progress:
  print('\n  Create subject-rule map', file=sys.stderr)
cursor.execute("""
    drop table if exists subject_rule_map;
    create table subject_rule_map (
    subject text references cuny_subjects,
    rule_id integer references transfer_rules,
    primary key (subject, rule_id))""")
cursor.execute('select id, source_subjects from transfer_rules')
num_rules = cursor.rowcount
count = 0
for rule in cursor.fetchall():
  count += 1
  if args.progress and (0 == count % 10000):
    print(f'    {count:,}/{num_rules:,}', end='\r', file=sys.stderr)
  subjects = rule.source_subjects.strip(':').split(':')
  for subject in subjects:
    cursor.execute('insert into subject_rule_map values(%s, %s)', (subject, rule.id))
# Creating indexes on the rule_id fields of source_courses and destination_courses gives an
# (unmeasured but really big) speedup in looking up source and destination courses in do_form_2().
if args.progress:
  end_map = perf_counter() - app_start
  print(f'\n    That took {end_map:0.1f} seconds.', file=sys.stderr)
  print('  Index source_courses', file=sys.stderr)
cursor.execute('create index on source_courses (rule_id)')
if args.progress:
  end_index_src = perf_counter() - end_map
  print(f'    That took {end_index_src:0.1f} seconds.', file=sys.stderr)
  print('  Index destination_courses', file=sys.stderr)
cursor.execute('create index on destination_courses (rule_id)')
if args.progress:
  end_index_dst = perf_counter() - end_index_src
  print(f'    That took {end_index_dst:0.1f} seconds.', file=sys.stderr)
  app_end = perf_counter() - app_start
  print(f'\n  Completed in {app_end:0.1f} seconds.', file=sys.stderr)
db.commit()
db.close()
