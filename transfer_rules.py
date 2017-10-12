# Two modes of operation:
#   1. Create a list of all course_ids that are part of transfer rules, but do not appear in the catalog.
#   2. Clear and re-populate the transfer_rules table.

import psycopg2
import csv
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--generate', '-g', action='store_true')
args = parser.parse_args()

# Get most recent transfer_rules file
all_files = [x for x in os.listdir('./queries/') if x.startswith('QNS_CV_SR_TRNS_INTERNAL_RULES')]
the_file = sorted(all_files, reverse=True)[0]
if args.debug: print(the_file)

db = psycopg2.connect('dbname=cuny_courses')
cur = db.cursor()
if args.generate:
  baddies = open('known_bad_ids.txt', 'w')
  bad_set = set()
  with open('./queries/' + the_file) as csvfile:
    csv_reader = csv.reader(csvfile)
    cols = None
    row_num = 0
    for row in csv_reader:
      row_num += 1
#      if row_num % 10000 == 0: print('row {}\r'.format(row_num), end='')
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
      else:
        src_id = int(row[cols.index('source_course_id')])
        dst_id = int(row[cols.index('destination_course_id')])
        if src_id not in bad_set:
          cur.execute("select course_id, institution, department, discipline, number from courses where course_id = {}".format(src_id))
          result = cur.fetchall()
          if len(result) < 1:
            bad_set.add(src_id)
            baddies.write('{} src\n'.format(src_id))
        if dst_id not in bad_set:
          cur.execute("select course_id, institution, department, discipline, number from courses where course_id = {}".format(dst_id))
          result = cur.fetchall()
          if len(result) < 1:
            bad_set.add(dst_id)
            baddies.write('{} dst\n'.format(src_id))
  baddies.close()
else:
  cur.execute('drop table if exists transfer_rules cascade')
  cur.execute("""
      create table transfer_rules (
        source_course_id integer references courses,
        destination_course_id integer references courses,
        status integer default 0 references transfer_rule_status,
        primary key (source_course_id, destination_course_id))
      """)

  known_bad_ids = [int(id.split(' ')[0]) for id in open('known_bad_ids.txt')]
  num_rules = 0
  with open('./queries/' + the_file) as csvfile:
    csv_reader = csv.reader(csvfile)
    cols = None
    for row in csv_reader:
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
      else:
        src_id = int(row[cols.index('source_course_id')])
        dst_id = int(row[cols.index('destination_course_id')])
        if src_id in known_bad_ids or dst_id in known_bad_ids: continue
        q = """
            insert into transfer_rules values({}, {})
            on conflict(source_course_id, destination_course_id) do nothing
            """.format(
            src_id,
            dst_id)
        cur.execute(q)
        num_rules += 1
    cur.execute('select count(*) from transfer_rules')
    num_inserted = cur.fetchone()[0]
    num_ignored = num_rules - num_inserted
    print('Given {} transfer rules: kept {}; rejected {}.'.format(num_rules, num_inserted, num_ignored))
    db.commit()
    db.close()
