#! /usr/local/bin/python3

""" Experiments in the structure of transfer rules’ credit information.
    Current version builds a copy of a CUNYfirst CSV query result as a table in the vickery db;
    used it to see whether there are institution codes that match the numeric Source ID field
    in QCCV_EXT_TRANSFER_EQUIV_COMP-xxx.csv. (Answer is no.) So we don't know what to make, if
    anything of that field, which sometimes has an institution code (QNS01, etc.) instead of a
    mysterious number.
"""

import os
import sys
import argparse
import csv

from collections import namedtuple
from datetime import date

import psycopg2
from psycopg2.extras import NamedTupleCursor

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')  # to stderr
args = parser.parse_args()

if args.progress:
  print('\nInitializing.', file=terminal)

db = psycopg2.connect('dbname=vickery')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Get the table
cf_rules_file = './QCCV_EXT_TRANSFER_EQUIV_COMP-29143052.csv'
with open(cf_rules_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  line_num = 0
  for line in csv_reader:
    if cols is None:
      line[0] = line[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
      Row = namedtuple('Row', cols)
      table_def = ''
      fields = (len(cols) * '%s, ').strip(', ')
      for col in cols:
        if col.endswith('date'):
          col_type = 'date'
        elif col.isnumeric():
          col_type = 'integer'
        else:
          col_type = 'text'
        table_def += f'{col} {col_type},'
      table_def = table_def.strip(',')
      cursor.execute(f"""
                        drop table if exists ext_transfer_equiv_comp;
                        create table ext_transfer_equiv_comp ({table_def});
                      """)
    else:
      cursor.execute(f'insert into ext_transfer_equiv_comp values ({fields})', line)

db.commit()
db.close()