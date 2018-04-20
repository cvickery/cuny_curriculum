# Identify rows from the internal rules query where the discipline/catalog# differ between the rule
# and the actual catalog info. Create and populate the bogus_rules table.
import psycopg2
import csv
import re
import os
import sys
import argparse
from collections import namedtuple
from datetime import date

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()

# There be some garbage institution "names" in the transfer_rules
cursor.execute("""select code as institution
                  from institutions
                  group by institution
                  order by institution""")
known_institutions = [inst[0] for inst in cursor.fetchall()]

# Get most recent transfer_rules query file
csvfile_name = './latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES.csv'
file_date = date.fromtimestamp(os.lstat(csvfile_name).st_birthtime).strftime('%Y-%m-%d')
logfile_name = './QNS_CV_SR_TRNS_INTERNAL_RULES_{}.csv'.format(file_date)
if args.debug: print('rules file: {}'.format(csvfile_name))

cursor.execute('drop table if exists bogus_rules')
cursor.execute("""
               create table bogus_rules (

                 id serial primary key,
                 source_institution text references institutions,
                 source_discipline text,
                 rule_group integer,
                 destination_institution text references institutions,

                 source_course_id integer,
                 source_course_id_is_bogus boolean,
                 bogus_source_discipline text,
                 bogus_source_catalog_number text,

                 destination_course_id integer,
                 destination_course_id_is_bogus boolean,
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
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
        Record = namedtuple('Record', cols)
        if args.debug:
          for col in cols:
            print('{} = {}; '.format(col, cols.index(col), end = ''))
          print()
      else:
        count_records += 1
        if args.progress:
          if (count_records % 1000) == 0:
            print('\r{:,}/{:,} {:,} bogus'.format(count_records, num_records, num_bogus), end='')
        if len(row) != len(cols):
          print('\nrow {} len(cols) = {} but len(rows) = {}'.format(row_num, len(cols), len(row)))
          continue
        record = Record._make(row)
        if args.debug:
          print()
          print(record)
        if record.source_institution not in known_institutions or \
           record.destination_institution not in known_institutions:
          continue

        source_course_id_is_bogus = False
        bogus_source_discipline = ''
        bogus_source_catalog_number = ''
        destination_course_id_is_bogus = False
        bogus_destination_discipline = ''
        bogus_destination_catalog_number = ''

        # Check source course
        source_course_id = int(record.source_course_id)
        cursor.execute("""
                       select discipline, catalog_number
                       from courses
                       where course_id = %s
                       """, (source_course_id, ))
        src_info = [row for row in cursor.fetchall()]
        if len(src_info) == 0:
          source_course_id_is_bogus = True
          bogus_source_discipline = record.source_discipline
          bogus_source_catalog_number = record.source_catalog_num
        else:
          if args.debug: print('src_info', src_info)
          if src_info[0][0] != record.source_discipline:
            bogus_source_discipline = record.source_discipline
          # Check numeric part of catalog number only
          src_num = re.search('\d+', src_info[0][1])
          if src_num: src_num = src_num.group(0)
          cat_num = re.search('\d+', record.source_catalog_num)
          if cat_num: cat_num = cat_num.group(0)
          if src_num != cat_num:
            # ... but record the actual number from the CF query result
            bogus_source_catalog_number = record.source_catalog_num.strip()

        # Check destination course
        destination_course_id = int(record.destination_course_id)
        cursor.execute("""
                       select discipline, catalog_number
                       from courses
                       where course_id = %s
                       """, (destination_course_id, ))
        dst_info = [row for row in cursor.fetchall()]
        if len(dst_info) == 0:
          destination_course_id_is_bogus = True
          bogus_destination_discipline = record.destination_discipline
          bogus_destination_catalog_number = record.destination_catalog_num
        else:
          if args.debug: print('dst_info', dst_info)
          if dst_info[0][0] != record.destination_discipline:
            bogus_destination_discipline = record.destination_discipline
          dst_num = re.search('\d+', dst_info[0][1])
          if dst_num: dst_num = dst_num.group(0)
          cat_num = re.search('\d+', record.destination_catalog_num)
          if cat_num: cat_num = cat_num.group(0)
          if dst_num != cat_num:
            bogus_destination_catalog_number = record.destination_catalog_num.strip()

        if  source_course_id_is_bogus or \
            bogus_source_discipline or \
            bogus_source_catalog_number or \
            destination_course_id_is_bogus or bogus_destination_discipline or \
            bogus_destination_catalog_number:
          num_bogus += 1
          rule_group_key = '{}-{}-{}-{}'.format(record.source_institution,
                                                record.source_discipline,
                                                record.src_equivalency_component,
                                                record.destination_discipline)

          # id serial primary key,
          # source_institution text references institutions,
          # source_discipline text,
          # rule_group integer,
          # destination_institution text references institutions,

          # source_course_id integer,
          # source_course_id_is_bogus boolean,
          # bogus_source_discipline text,
          # bogus_source_catalog_number text,

          # destination_course_id integer,
          # destination_course_id_is_bogus boolean,
          # bogus_destination_discipline text,
          # bogus_destination_catalog_number text

          cursor.execute("""
                          insert into bogus_rules
                          values (default, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                         """, (record.source_institution,
                               record.source_discipline,
                               record.src_equivalency_component,
                               record.destination_institution,

                               source_course_id,
                               source_course_id_is_bogus,
                               bogus_source_discipline,
                               bogus_source_catalog_number,

                               destination_course_id,
                               destination_course_id_is_bogus,
                               bogus_destination_discipline,
                               bogus_destination_catalog_number))
          logfile.write('\n{}-{}-{}-{}: {} {} {} {} {} {} {} {}'.format(
                                                              record.source_institution,
                                                              record.source_discipline,
                                                              record.src_equivalency_component,
                                                              record.destination_institution,

                                                              source_course_id,
                                                              source_course_id_is_bogus,
                                                              bogus_source_discipline,
                                                              bogus_source_catalog_number,

                                                              destination_course_id,
                                                              destination_course_id_is_bogus,
                                                              bogus_destination_discipline,
                                                              bogus_destination_catalog_number))

  logfile.write('\nFound {:,} bogus records ({:.2f}%) out of {:,}.\n'.format(num_bogus,
                                                              100 *num_bogus / num_records,
                                                              num_records))

db.commit()
db.close()
print('  Found {:,} bogus records ({:.2f}%) out of {:,}.'.format(num_bogus,
                                                              100 *num_bogus / num_records,
                                                              num_records))
