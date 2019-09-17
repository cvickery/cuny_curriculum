#! /usr/local/bin/python3

""" Transfer any ”properly-structured” csv file into a Postgres table. The table will be replaced if
    it already exists.

    The CSV file has to have a heading row in which each column name turns into a valid db column
    name when lowercased and with spaces replaced by underscores. And no column names may be
    repeated.

    There are type checks for column names that end with '_date' or '_num', which will be typed as
    date and integer respectively.

    The default database name is the user's name.

    The default table name is based on the input file name (see code), or 'test' if reading from
    stdin.

    Use a positive value for the modulus option to get an arguments summary and progress report.
"""

import sys
import argparse
import csv

from getpass import getuser
from collections import namedtuple
from datetime import date

import psycopg2
from psycopg2.extras import NamedTupleCursor

# Command line processing
parser = argparse.ArgumentParser()
parser.add_argument('--file_name', '-f', default=None)  # csv file
parser.add_argument('--db_name', '-db', default=getuser())
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--columns', '-c', action='store_true')
parser.add_argument('--id_is_int', '-i', action='store_false')
parser.add_argument('--modulus', '-m', type=int, default=0)   # for progress reporting
parser.add_argument('--primary_key', '-p', nargs='+')
parser.add_argument('--table_name', '-t', default=None)
args = parser.parse_args()

db_name = args.db_name
table_name = args.table_name
if args.file_name is None:
  csv_file = sys.stdin
  file_name = 'stdin'
  num_lines = ':stream input'
else:
  file_name = args.file_name
  num_lines = f'/{len(open(file_name).readlines()):,}'
  csv_file = open(file_name)
  if table_name is None:
    table_name = file_name.lower().replace(' ', '_').replace('.csv', '').strip('-0123456789')
if table_name is None:
  table_name = 'test'

if args.modulus > 0:
  print(f'{file_name} => {db_name}.{table_name}')

db = psycopg2.connect(f'dbname={db_name}')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Process the table
cols = None
line_num = 0
csv_reader = csv.reader(csv_file)
for line in csv_reader:
  line_num += 1
  if args.modulus > 0 and (0 == line_num % args.modulus):
    print(f'{line_num:,}{num_lines}\r', end='')
  if cols is None:
    # Determine the table structure from the header row.
    line[0] = line[0].replace('\ufeff', '')
    cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
    Row = namedtuple('Row', cols)
    table_def = ''
    fields = (len(cols) * '%s, ').strip(', ')
    for col in cols:
      if col.endswith('_date'):
        col_type = 'date'
      elif (col.endswith('_num')
            or col.endswith('_nbr')
            or (args.id_is_int and col.endswith('_id'))):
        col_type = 'integer'
      else:
        col_type = 'text'
      table_def += f'{col} {col_type}, '
    if args.primary_key:
      table_def += f'primary key ({", ".join(args.primary_key)})'
    table_def = table_def.strip(', ')
    if args.columns:
      print(table_def.replace(', ', ',\n'))
      exit()
    cursor.execute(f"""
                      drop table if exists {table_name};
                      create table {table_name} ({table_def});
                    """)
  else:
    # Insert rows into the table
    cursor.execute(f'insert into {table_name} values ({fields})', line)

# Clear the progress line
if args.modulus > 0:
  print('\r' + len(f'{line_num:,}/{num_lines}') * ' ' + '\r', end='')

db.commit()
db.close()
