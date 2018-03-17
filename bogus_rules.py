# Remove rows from the internal rules query where the discipline/catalog# differ between the rule
# and the actual catalog info.
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
all_files = [x for x in os.listdir('./queries/') if x.startswith('QNS_CV_SR_TRNS_INTERNAL_RULES')]
csvfile_name = sorted(all_files, reverse=True)[0]
logfile_name = csvfile_name.replace('.csv', '.log')
if args.debug: print('rules file: {}'.format(csvfile_name))

num_records = sum(1 for line in open('queries/' + csvfile_name))
count_records = 0
num_bogus = 0
with open('./' + logfile_name, 'w') as logfile:
  with open('./queries/' + csvfile_name) as csvfile:
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

        error = ''

        # Check source course
        source_course_id = int(record.source_course_id)
        cursor.execute("""
                       select discipline, catalog_number
                       from courses
                       where course_id = %s
                       """, (source_course_id, ))
        src_info = [row for row in cursor.fetchall()]
        if len(src_info) == 0:
          error = 'Source course not in cuny catalog.\n'
        else:
          if args.debug: print('src_info', src_info)
          if src_info[0][0] != record.source_discipline:
            error += '  Source discipline ({}) does not match cuny catalog ({}).\n'.format(
                      src_info[0][0], record.source_discipline)
          src_num = re.search('\d+', src_info[0][1])
          if src_num: src_num = src_num.group(0)
          cat_num = re.search('\d+', record.source_catalog_num)
          if cat_num: cat_num = cat_num.group(0)
          if src_num != cat_num:
            error += '  Source catalog number ({}) does not match cuny catalog: ({}).'.format(
                      src_num, cat_num)

        # Check destination course
        destination_course_id = int(record.destination_course_id)
        cursor.execute("""
                       select discipline, catalog_number
                       from courses
                       where course_id = %s
                       """, (destination_course_id, ))
        dst_info = [row for row in cursor.fetchall()]
        if len(dst_info) == 0:
          error = 'Destination course not in cuny catalog.\n'
        else:
          if args.debug: print('dst_info', dst_info)
          if dst_info[0][0] != record.destination_discipline:
            error += '  Destination discipline ({}) does not match cuny catalog ({}).\n'.format(
                      dst_info[0][0], record.destination_discipline)
          dst_num = re.search('\d+', dst_info[0][1])
          if dst_num: dst_num = dst_num.group(0)
          cat_num = re.search('\d+', record.destination_catalog_num)
          if cat_num: cat_num = cat_num.group(0)
          if dst_num != cat_num:
            error += '  Destination catalog number ({}) does not match cuny catalog: ({}).'.format(
                      dst_num, cat_num)
        if error != '':
          num_bogus += 1
          logfile.write('{}\n{}'.format(row, error))

  logfile.write('Found {} bogus records ({:.1}%) out of {}.\n'.format(num_bogus,
                                                              100 *num_bogus / num_records,
                                                              num_records))

print()