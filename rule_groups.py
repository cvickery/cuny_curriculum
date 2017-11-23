# Two modes of operation:
#   1. Create a list of all course_ids that are part of transfer rules, but which do not appear in
#   the catalog.
#   2. Clear and re-populate the rule_groups, source_courses, and destination_courses tables.

import psycopg2
import csv
import os
import sys
import argparse
from collections import namedtuple

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--generate', '-g', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')
parser.add_argument('--report', '-r', action='store_true')
args = parser.parse_args()

# Get most recent transfer_rules query file
all_files = [x for x in os.listdir('./queries/') if x.startswith('QNS_CV_SR_TRNS_INTERNAL_RULES')]
the_file = sorted(all_files, reverse=True)[0]
if args.report:
  print('Transfer rules query file:', the_file)

num_lines = sum(1 for line in open('queries/' + the_file))

known_bad_filename = 'known_bad_ids.{}.log'.format(os.getenv('HOSTNAME').split('.')[0])
db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()

# There be some garbage institution "names" in the transfer_rules
cursor.execute("""select code as institution
                  from institutions
                  group by institution
                  order by institution""")
known_institutions = [inst[0] for inst in cursor.fetchall()]

if args.generate:
  baddies = open(known_bad_filename, 'w')
  bad_set = set()
  with open('./queries/' + the_file) as csvfile:
    csv_reader = csv.reader(csvfile)
    cols = None
    row_num = 0
    for row in csv_reader:
      row_num += 1
      if args.progress and row_num % 10000 == 0:
        print('row {:,}/{:,}\r'.format(row_num, num_lines), end='', file=sys.stderr)
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
        Record = namedtuple('Record', cols)
        if args.debug:
          print(cols)
          for col in cols:
            print('{} = {}; '.format(col, cols.index(col), end = ''))
          print()
      else:
        if len(row) != len(cols):
          print('\nrow {} len(cols) = {} but len(rows) = {}'.format(row_num, len(cols), len(row)))
          continue
        record = Record._make(row)
        if record.source_institution not in known_institutions or \
           record.destination_institution not in known_institutions:
          continue
        source_course_id = int(record.source_course_id)
        destination_course_id = int(record.destination_course_id)
        if source_course_id not in bad_set:
          cursor.execute("""select course_id
                            from courses
                            where course_id = {}""".format(source_course_id))
          if cursor.rowcount == 0:
            bad_set.add(source_course_id)
            baddies.write('{} src\n'.format(source_course_id))
        if destination_course_id not in bad_set:
          cursor.execute("""select course_id
                            from courses
                            where course_id = {}""".format(destination_course_id))
          if cursor.rowcount == 0:
            bad_set.add(destination_course_id)
            baddies.write('{} dst\n'.format(destination_course_id))
  baddies.close()
else:
  conflicts = open('conflicts.{}.log'.format(os.getenv('HOSTNAME').split('.')[0]), 'w')

  known_bad_ids = [int(id.split(' ')[0]) for id in open(known_bad_filename)]
  # Clear the three tables
  cursor.execute('truncate source_courses, destination_courses, rule_groups')

  num_groups = 0
  num_source_courses = 0
  num_destination_courses = 0

  with open('./queries/' + the_file) as csvfile:
    csv_reader = csv.reader(csvfile)
    cols = None
    row_num = 0;
    for row in csv_reader:
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
        Record = namedtuple('Record', cols);
        if args.debug:
          print(cols)
          for col in cols:
            print('{} = {}; '.format(col, cols.index(col), end = ''))
            print()
      else:
        row_num += 1
        if args.progress and row_num % 10000 == 0: print('row {:,}/{:,}\r'.format(row_num,
                                                                                  num_lines),
                                                         end='',
                                                         file=sys.stderr)
        record = Record._make(row)

        source_institution = record.source_institution
        destination_institution = record.destination_institution
        if source_institution not in known_institutions:
          conflicts.write('Unknown institution: {}\n'.format(source_institution))
          continue
        if destination_institution not in known_institutions:
          conflicts.write('Unknown institution: {}\n'.format(destination_institution))
          continue

        # Assemble the components of the rule group
        source_course_id = int(record.source_course_id)
        destination_course_id = int(record.destination_course_id)
        if source_course_id in known_bad_ids or destination_course_id in known_bad_ids:
          continue
        cursor.execute("""select institution, discipline
                          from courses
                          where course_id = {}""".format(source_course_id))
        source_institution, source_discipline = cursor.fetchone()
        rule_group_number = int(record.src_equivalency_component)
        min_gpa = float(record.min_grade_pts)
        max_gpa = float(record.max_grade_pts)
        transfer_credits = float(record.units_taken)

        # Create or look up the rule group
        cursor.execute("""
                       insert into rule_groups values(
                       default, '{}', '{}', {}) on conflict do nothing returning id
                       """.format(source_institution, source_discipline, rule_group_number))
        num_groups += cursor.rowcount
        if cursor.rowcount == 0:
          cursor.execute("""
                         select id
                         from rule_groups
                         where institution = '{}'
                         and discipline = '{}'
                         and group_number = {}""".format(source_institution,
                                                         source_discipline,
                                                         rule_group_number))
          assert cursor.rowcount == 1, """select rule_group id returned {} values
                                       """.format(cursor.rowcount)
        rule_group_id = cursor.fetchone()[0]

        # Add the source course
        cursor.execute("""
                        insert into source_courses values(default, {}, {}, {})
                        on conflict do nothing
                       """.format(rule_group_id, source_course_id, min_gpa, max_gpa))
        num_source_courses += cursor.rowcount
        # Add the destination course
        cursor.execute("""
                        insert into destination_courses values(default, {}, {}, {})
                        on conflict do nothing
                       """.format(rule_group_id, destination_course_id, transfer_credits))
        num_destination_courses += cursor.rowcount

    if args.report:
      print("""\n{:,} Groups\n{:,} Source courses\n{:,} Destination courses
            """.format(num_groups, num_source_courses, num_destination_courses))
    db.commit()
    db.close()
    conflicts.close()
