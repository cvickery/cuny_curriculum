""" Speed up transfer rule lookups
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
parser.add_argument('--report', '-r', action='store_true')    # to stdout
args = parser.parse_args()

app_start = perf_counter()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Using the subject_rule_map table (instead of putting source subjects in a colon-delimited string
# in each rule) gives a 1.97 speedup of rule lookups in do_form_2()
cursor.execute("""
    drop table if exists subject_rule_map;
    create table subject_rule_map (
    subject text references cuny_subjects,
    rule_id integer references transfer_rules,
    primary key (subject, rule_id))""")
cursor.execute('select id, source_subjects from transfer_rules')
count = 0
for rule in cursor.fetchall():
  count += 1
  if 0 == count % 10000:
    print(f'{count:,}')
  subjects = rule.source_subjects.strip(':').split(':')
  for subject in subjects:
    cursor.execute('insert into subject_rule_map values(%s, %s)', (subject, rule.id))

# Creating indexes on the rule_id fields of source_courses and destination_courses gives an
# (unmeasured but really big) speedup in looking up source and destination courses in do_form_2().
cursor.execute('create index on source_courses (rule_id)')
cursor.execute('create index on destination_courses (rule_id)')

db.commit()
db.close()