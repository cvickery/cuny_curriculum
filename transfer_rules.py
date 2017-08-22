# Clear and re-populate the transfer_rules table.

import psycopg2
import csv
import os

# Get most recent transfer_rules file
all_files = [x for x in os.listdir('.') if x.startswith('QNS_CV_TRNS_INTERNAL_RULS_SHRT')]
the_file = sorted(all_files, reverse=True)[0]
print(the_file)
db = psycopg2.connect('dbname=cuny_courses')
cur = db.cursor()
cur.execute('drop table if exists transfer_rules')
cur.execute("""
    create table transfer_rules (
      source_course_id integer references courses,
      destination_course_id integer references courses,
      primary key (source_course_id, destination_course_id))
    """)
known_bad_ids = [int(id) for id in open('known_bad_ids')]
num_rules = 0
with open(the_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      src_id = int(row[cols.index('source_course_id')])
      dst_id = int(row[cols.index('destination_course_id')])
      # cur.execute("select course_id from courses where course_id = '{}'".format(src_id))
      # result = cur.fetchall()
      # if len(result) < 1: print(src_id)
      # cur.execute("select course_id from courses where course_id = '{}'".format(dst_id))
      # result = cur.fetchall()
      # if len(result) < 1: print(dst_id)
      # continue
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
