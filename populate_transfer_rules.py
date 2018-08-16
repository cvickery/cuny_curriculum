#   Clear and re-populate the transfer_rules, source_courses, and destination_courses tables using
#   the result of the CUNYfirst query, QNS_CV_SR_TRNS_INTERNAL_RULES.
#
#   Note and ignore query records that have invalid course_id fields (lookup fails).
#
#   Note, but keep, query records where the textual description of the course does not match the
#   catalog description. (Mismatched institution and/or discipline.) Use the course_id and catalog
#   info.
#
#   Deal with cross-listed courses by allowing a source or destination course_id to represent all
#   offer_nbr values for “the course“.
#
#   Design:
#     1. Extract information from the CF query: data structures for transfer rule keys and lists of
#     source and destination course_ids.
#       Note and reject records that reference non-existent institutions
#     2. Lookup course_ids
#           Note and eliminate rules that specifiy non-existent courses
#           Note rules that specify inactive destination courses
#           Build lists of source disciplines for all rules
#     3. Clone rules with cross-listed courses so there is a rule for each one.
#     4. Insert rules and course lists into database tables

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

if args.progress:
  print('', file=sys.stderr)

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Get most recent transfer_rules query file
cf_rules_file = './latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES.csv'
file_date = date\
    .fromtimestamp(os.lstat(cf_rules_file).st_mtime).strftime('%Y-%m-%d')
num_lines = sum(1 for line in open(cf_rules_file))

if args.report:
  print('Transfer rules query file: {} {}'.format(file_date, cf_rules_file))

cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'transfer_rules'""".format(file_date, cf_rules_file))

# There be some garbage institution "names" in the transfer_rules, but the app’s institutions
# table is “definitive”.
cursor.execute("""select code
                  from institutions
                  order by code""")
known_institutions = [record.code for record in cursor.fetchall()]

# Use the disciplines table for reporting cases where the component_subject_area isn't there.
cursor.execute("""select institution, discipline
                  from disciplines""")
valid_disciplines = [(record.institution, record.discipline) for record in cursor.fetchall()]

conflicts = open('rule_group_conflicts.log', 'w')

# Clear the three tables
cursor.execute('truncate source_courses, destination_courses, transfer_rules')


# All three dicts use the same key.
Primary_Key = namedtuple('Primary_Key',
                         'source_institution destination_institution subject_area group_number')
Source_Course = namedtuple('Source_Course',
                           'course_id min_gpa max_gpa')
Destination_Course = namedtuple('Destination_Course',
                                'course_id, transfer_credits')
transfer_rules = dict()
source_courses = dict()
destination_courses = dict()

start_time = perf_counter()
with open(cf_rules_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  line_num = 0
  for line in csv_reader:
    if cols is None:
      line[0] = line[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
      Record = namedtuple('Record', cols)
      if args.debug:
        print(cols)
        for col in cols:
          print('{} = {}; '.format(col, cols.index(col), end=''))
          print()
    else:
      line_num += 1
      if args.progress and line_num % 1000 == 0:
        elapsed_time = perf_counter() - start_time
        total_time = num_lines * elapsed_time / line_num
        secs_remaining = total_time - elapsed_time
        mins_remaining = int((secs_remaining) / 60)
        secs_remaining = int(secs_remaining - (mins_remaining * 60))
        print('line {:,}/{:,} ({:.1f}%) {}:{:02} remaining.\r'
              .format(line_num,
                      num_lines,
                      100 * line_num / num_lines,
                      mins_remaining,
                      secs_remaining),
              end='', file=sys.stderr)

      record = Record._make(line)

      # 2018-07-19: The following two tests never fail
      if record.source_institution not in known_institutions:
        conflicts.write('Unknown institution: {}. Record skipped.\n'
                        .format(record.source_institution))
        continue
      if record.destination_institution not in known_institutions:
        conflicts.write('Unknown institution: {}. Record skipped.\n'
                        .format(record.destination_institution))
        continue

      # The source_discipline for the rule group may differ from the disciplines of the source
      # courses. What we call the source_discipline, CF calls the Component Subject Area.
      if (record.source_institution, record.component_subject_area) not in valid_disciplines:
        # Report the problem, but accept the record.
        conflicts.write('({}, {}) not in cuny_subject_table. Record kept.\n'
                        .format(record.source_institution, record.component_subject_area))

      primary_key = Primary_Key(record.source_institution,
                                record.destination_institution,
                                record.component_subject_area,
                                record.src_equivalency_component)

      if primary_key not in source_courses.keys():
        source_courses[primary_key] = set()
        destination_courses[primary_key] = set()
      source_courses[primary_key].add(Source_Course(int(record.source_course_id),
                                                    record.min_grade_pts,
                                                    record.max_grade_pts))
      destination_courses[primary_key].add(Destination_Course(int(record.destination_course_id),
                                                              record.units_taken))
      if record.source_institution == 'QCC01' and\
         record.destination_institution == 'QNS01' and\
         record.component_subject_area == 'PH' and\
         record.src_equivalency_component == '0036':
        print(record)
        print(source_courses[primary_key])
        print(destination_courses[primary_key])
        print()
      # # The source course_id might reference multiple (cross-listed) courses with different
      # # offer_nbrs. In that case, create multiple identical rules, appending the offer_nbr as a
      # # trailing decimal digit.
      # cursor.execute("""select institution, offer_nbr, discipline
      #                   from courses
      #                   where course_id = %s""", (source_course_id,))
      # if cursor.rowcount == 0:
      #   conflicts.write(f'Source Course ID {source_course_id} not found for {record}\n')
      #   continue
      # for course in cursor.fetchall():
      #   if args.debug:
      #     print('{}: {} {} {}'.format(source_course_id,
      #                                 course.institution,
      #                                 course.offer_nbr,
      #                                 course.discipline))
      #   source_institution = course.institution
      #   offer_nbr = course.offer_nbr
      #   if offer_nbr > 4:
      #     conflicts.write('Bogus offer_nbr ({}) for source_course_id {}.\n  {}\n'
      #                     .format(offer_nbr, source_course_id))
      #     offer_nbr = 1
      #   group_number = float() + (offer_nbr / 10.0)
      #   rules.append(dict(source_institution=source_institution,
      #                           source_discipline=source_discipline,
      #                           group_number=group_number,
      #                           offer_nbr=offer_nbr))

      # cursor.execute("""select institution
      #                   from courses
      #                   where course_id = %s
      #                     and course_status = 'A'
      #                   group by institution""", (destination_course_id,))
      # if cursor.rowcount > 0:
      #   if cursor.rowcount > 1:
      #     conflicts.write('Multiple destination institutions ({}) for {}.\n  {}\n'
      #                     .format(cursor.rowcount, destination_course_id, record))
      #     continue
      #   destination_institution = cursor.fetchone().institution
      # else:
      #   conflicts.write('No active courses with destination_course_id {} for {}\n'
      #                   .format(destination_course_id, record))
      #   continue
      # if destination_institution != dest_institution:
      #   conflicts.write('Destination institution ({}) != course institution ({})\n  {}\n'
      #                   .format(dest_institution, destination_institution, record))
      #   continue

      # # Create or look up the rule group(s)
      # try:
      #   for rule_group in rules:
      #     cursor.execute("""insert into rules values (%s, %s, %s, %s)
      #                    on conflict do nothing""",
      #                    (rule_group['source_institution'],
      #                     rule_group['source_discipline'],
      #                     rule_group['group_number'],
      #                     destination_institution))
      #     num_groups += cursor.rowcount
      #     if args.debug:
      #       print(f'{cursor.query}\n  Rows inserted {cursor.rowcount}')
      # except psycopg2.Error as e:
      #   print('ERROR creating/updating rule group for {}, {}'
      #         .format(source_course_id, destination_course_id),
      #         file=sys.stderr)
      #   print(cursor.query)
      #   print(e.pgerror, file=sys.stderr)
      #   exit(1)

      # # Add the source course(s)
      # for rule_group in rules:
      #   cursor.execute("""
      #                  insert into source_courses values(default,
      #                    '{}', '{}', {}, '{}', {}, {}, {})
      #                  on conflict do nothing"""
      #                  .format(rule_group['source_institution'],
      #                          rule_group['source_discipline'],
      #                          rule_group['group_number'],
      #                          destination_institution,
      #                          source_course_id,
      #                          min_gpa,
      #                          max_gpa))
      #   num_source_courses += cursor.rowcount
      # # Add the destination course (to each rule_group)
      # for rule_group in rules:
      #   cursor.execute("""
      #                  insert into destination_courses values(default,
      #                    '{}', '{}', {}, '{}', {}, {})
      #                  on conflict do nothing """
      #                  .format(rule_group['source_institution'],
      #                          rule_group['source_discipline'],
      #                          rule_group['group_number'],
      #                          destination_institution,
      #                          destination_course_id,
      #                          transfer_credits))
      #   num_destination_courses += cursor.rowcount
if args.progress:
  print('', file=sys.stderr)
if args.report:
  num_rules = len(source_courses.keys())
  num_source_courses = sum([len(course) for course in source_courses])
  num_destination_courses = sum([len(course) for course in destination_courses])
  print("""\n{:,} Rules\n{:,} Source courses\n{:,} Destination courses
        """.format(num_rules, num_source_courses, num_destination_courses))
  secs = perf_counter() - start_time
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'Completed in {mins}:{secs:02} minutes')
db.commit()
db.close()
conflicts.close()
