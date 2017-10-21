# Two modes of operation:
#   1. Create a list of all course_ids that are part of transfer rules, but do not appear in the catalog.
#   2. Clear and re-populate the transfer_rules table.

import psycopg2
import csv
import os
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--generate', '-g', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')
parser.add_argument('--report', '-r', action='store_true')
args = parser.parse_args()

# Get most recent transfer_rules file
all_files = [x for x in os.listdir('./queries/') if x.startswith('QNS_CV_SR_TRNS_INTERNAL_RULES')]
the_file = sorted(all_files, reverse=True)[0]
if args.report:
  print('Transfer rules file:', the_file)

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()
if args.generate:
  baddies = open('known_bad_ids.txt', 'w')
  bad_set = set()
  with open('./queries/' + the_file) as csvfile:
    csv_reader = csv.reader(csvfile)
    cols = None
    row_num = 0
    for row in csv_reader:
      row_num += 1
      if args.progress and row_num % 10000 == 0: print('row {}\r'.format(row_num),
                                                       end='',
                                                       file=sys.stderr)
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
      else:
        src_id = int(row[cols.index('source_course_id')])
        dst_id = int(row[cols.index('destination_course_id')])
        if src_id not in bad_set:
          cursor.execute("select course_id from courses where course_id = {}".format(src_id))
          if cursor.rowcount == 0:
            bad_set.add(src_id)
            baddies.write('{} src\n'.format(src_id))
          else:
            if src_id == 105967: print('found src {}'.format(src_id))
        if dst_id not in bad_set:
          cursor.execute("select course_id from courses where course_id = {}".format(dst_id))
          if cursor.rowcount == 0:
            bad_set.add(dst_id)
            baddies.write('{} dst\n'.format(dst_id))
          else:
            if dst_id == 105967: print('found dst {}'.format(dst_id))
  baddies.close()
else:
  cursor.execute('drop table if exists transfer_rules cascade')
  cursor.execute("""
      create table transfer_rules (
        source_course_id integer references courses,
        destination_course_id integer references courses,
        status integer default 0 references transfer_rule_status,
        primary key (source_course_id, destination_course_id))
      """)

  known_bad_ids = [int(id.split(' ')[0]) for id in open('known_bad_ids.txt')]
  num_rules = 0
  num_conflicts = 0
  with open('./queries/' + the_file) as csvfile:
    csv_reader = csv.reader(csvfile)
    cols = None
    row_num = 0;
    for row in csv_reader:
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
      else:
        row_num += 1
        if args.progress and row_num % 10000 == 0: print('row {}\r'.format(row_num),
                                                         end='',
                                                         file=sys.stderr)
        src_id = int(row[cols.index('source_course_id')])
        dst_id = int(row[cols.index('destination_course_id')])
        if src_id in known_bad_ids or dst_id in known_bad_ids:
          continue

        q = """
            insert into transfer_rules values({}, {})
            on conflict(source_course_id, destination_course_id) do nothing
            """.format(
            src_id,
            dst_id)
        cursor.execute(q)
        num_rules += 1
        num_conflicts += (1 - cursor.rowcount)
    if args.report:
      cursor.execute('select count(*) from transfer_rules')
      num_inserted = cursor.fetchone()[0]
      num_ignored = num_rules - num_inserted
      print('Given {} transfer rules: kept {}; rejected {} ({} conflicts).'.format(num_rules,
                                                                                   num_inserted,
                                                                                   num_ignored,
                                                                                   num_conflicts))
    db.commit()
    db.close()
