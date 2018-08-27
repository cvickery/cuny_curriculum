# Identify rows from the internal rules query where the discipline/catalog differ
# between the rule and the actual catalog info. Create and populate the bogus_rules
# table; generate a log file with same info. (The db table is not used in the app, but
# is useful for reporting to CUNY.)

import psycopg2
from psycopg2.extras import NamedTupleCursor

import csv
import re
import os
import sys
import argparse
from collections import namedtuple
from datetime import date
from time import perf_counter

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')
args = parser.parse_args()

start_time = perf_counter()
if args.progress:
  print('', file=sys.stderr)

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# There be some garbage institution "names" in the transfer_rules
cursor.execute("""select code as institution
                  from institutions
                  group by institution
                  order by institution""")
known_institutions = [inst[0] for inst in cursor.fetchall()]
if args.debug:
  print(known_institutions)

# Get most recent transfer_rules query file
csvfile_name = './latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES.csv'
file_date = date.fromtimestamp(os.lstat(csvfile_name).st_birthtime)\
    .strftime('%Y-%m-%d')
logfile_name = './bogus_rules_report_{}.log'.format(file_date)

if args.debug:
  print('rules file: {}'.format(csvfile_name))

cursor.execute('drop table if exists bogus_rules')
cursor.execute("""
               create table bogus_rules (

                 id serial primary key,

                 source_institution text references institutions,
                 destination_institution text references institutions,
                 subject_area text,
                 group_number integer,

                 source_course_id integer,
                 real_source_discipline text,
                 real_source_catalog_number text,
                 bogus_source_discipline text,
                 bogus_source_catalog_number text,

                 destination_course_id integer,
                 real_destination_discipline text,
                 real_destination_catalog_number text,
                 bogus_destination_discipline text,
                 bogus_destination_catalog_number text)
               """)

num_records = sum(1 for line in open(csvfile_name))
count_records = 0
num_bogus = 0
with open(logfile_name, 'w') as logfile:
  with open(csvfile_name) as csvfile:
    csv_reader = csv.reader(csvfile)
    cols = None
    row_num = 0
    for row in csv_reader:
      if cols is None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
        Record = namedtuple('Record', cols)
        if args.debug:
          for col in cols:
            print('{} = {}; '.format(col, cols.index(col), end=''))
          print()
      else:
        count_records += 1
        if args.progress:
          if (count_records % 1000) == 0:
            elapsed_time = perf_counter() - start_time
            total_time = num_records * elapsed_time / count_records
            secs_remaining = total_time - elapsed_time
            mins_remaining = int((secs_remaining) / 60)
            secs_remaining = int(secs_remaining - (mins_remaining * 60))
            print('line {:,}/{:,} ({:.1f}%) {}:{:02} remaining.\r'
                  .format(count_records,
                          num_records,
                          100 * count_records / num_records,
                          mins_remaining,
                          secs_remaining),
                  file=sys.stderr,
                  end='')
        if len(row) != len(cols):
          print('\nrow {} len(cols) = {} but len(rows) = {}'.format(row_num, len(cols), len(row)))
          continue
        record = Record._make(row)
        if args.debug:
          print()
          print(record)
        # Ignore records that reference nonexistent institutions
        if record.source_institution not in known_institutions or \
           record.destination_institution not in known_institutions:
          continue

        is_bogus = False

        # Check source course
        real_source_discipline = 'NOT FOUND'
        real_source_catalog_number = 'NOT FOUND'
        bogus_source_discipline = record.component_subject_area
        bogus_source_catalog_number = record.source_catalog_num

        source_course_id = int(record.source_course_id)
        cursor.execute("""
                       select discipline, catalog_number
                       from courses
                       where course_id = %s
                       """, (source_course_id, ))
        cross_listed_source_count = cursor.rowcount
        if cursor.rowcount < 1:
          is_bogus = True
        else:
          real_source_discipline, real_source_catalog_number = cursor.fetchone()
          source_num = re.search(r'\d+', real_source_catalog_number)
          if source_num:
            source_num = source_num.group(0)
          bogus_num = re.search(r'\d+', record.source_catalog_num)
          if bogus_num:
            bogus_num = bogus_num.group(0)
          if (real_source_discipline != bogus_source_discipline) or \
             (source_num != bogus_num):
            is_bogus = True

        # Check destination course
        real_destination_discipline = 'NOT FOUND'
        real_destination_catalog_number = 'NOT FOUND'
        bogus_destination_discipline = record.destination_discipline
        bogus_destination_catalog_number = record.destination_catalog_num

        destination_course_id = int(record.destination_course_id)
        cursor.execute("""
                       select discipline, catalog_number
                       from courses
                       where course_id = %s
                       """, (destination_course_id, ))
        cross_listed_destination_count = cursor.rowcount
        if cursor.rowcount < 1:
          is_bogus = True
        else:
          real_destination_discipline, real_destination_catalog_number = cursor.fetchone()
          destination_num = re.search(r'\d+', real_destination_catalog_number)
          if destination_num:
            destination_num = destination_num.group(0)
          bogus_num = re.search(r'\d+', bogus_destination_catalog_number)
          if bogus_num:
            bogus_num = bogus_num.group(0)
          if (real_destination_discipline != bogus_destination_discipline) or \
             (destination_num != bogus_num):
            is_bogus = True

        if is_bogus:
          num_bogus += 1

          cursor.execute("""
                          insert into bogus_rules
                          values (default,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                         """, (record.source_institution,
                               record.destination_institution,
                               record.component_subject_area,
                               record.src_equivalency_component,

                               source_course_id,
                               real_source_discipline,
                               real_source_catalog_number,
                               bogus_source_discipline,
                               bogus_source_catalog_number,

                               destination_course_id,
                               real_destination_discipline,
                               real_destination_catalog_number,
                               bogus_destination_discipline,
                               bogus_destination_catalog_number))
          logfile.write('{}-{}-{}-{}: {:06} {} {} ? {} {} :: {:06} {} {} ? {} {}\n'
                        .format(record.source_institution,
                                record.destination_institution,
                                record.component_subject_area,
                                record.src_equivalency_component,

                                source_course_id,
                                real_source_discipline,
                                real_source_catalog_number,
                                bogus_source_discipline,
                                bogus_source_catalog_number,

                                destination_course_id,
                                real_destination_discipline,
                                real_destination_catalog_number,
                                bogus_destination_discipline,
                                bogus_destination_catalog_number))
        if (cross_listed_source_count > 1) or (cross_listed_destination_count > 1):
          logfile.write('{}-{}-{}-{}: cross-listed source = {}; destinaton = {}\n'
                        .format(record.source_institution,
                                record.destination_institution,
                                record.component_subject_area,
                                record.src_equivalency_component,
                                cross_listed_source_count,
                                cross_listed_destination_count))
  logfile.write('\nFound {:,} bogus records ({:.2f}%) out of {:,}.\n'
                .format(num_bogus, 100 * num_bogus / num_records, num_records))

db.commit()
db.close()
if args.progress:
  print('', file=sys.stderr)
print('\rFound {:,} bogus records ({:.2f}%) out of {:,}.'
      .format(num_bogus, 100 * num_bogus / num_records, num_records))
