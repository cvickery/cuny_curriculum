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
#     3. Insert rules and course lists into database tables

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

app_start = perf_counter()


# Bogus_Course_Error
# =================================================================================================
class Failed_Course_Error(Exception):
  """Exception raised for course lookup failures.

  Attributes:
      message -- explanation of the error
  """

  def __init__(self, message):
      self.message = message


# mk_rule_key()
# -------------------------------------------------------------------------------------------------
def mk_rule_key(rule):
  """ Convert a rule tuple to a hyphen-separated string.
  """
  return '{}-{}-{}-{}'.format(rule.source_institution,
                              rule.destination_institution,
                              rule.subject_area,
                              rule.group_number)


if args.progress:
  print('\nInitializing.', file=sys.stderr)

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

# There be some garbage institution "names" in the transfer_rules, but the app’s
# institutions table is “definitive”.
cursor.execute("""select code
                  from institutions
                  order by code""")
known_institutions = [record.code for record in cursor.fetchall()]

# Use the disciplines table for reporting cases where the component_subject_area isn't
# there.
cursor.execute("""select institution, discipline
                  from disciplines""")
valid_disciplines = [(record.institution, record.discipline)
                     for record in cursor.fetchall()]

conflicts = open('transfer_rule_conflicts.log', 'w')

# Clear the three tables
cursor.execute('truncate source_courses, destination_courses, transfer_rules')


# All three dicts use the same rule key.
Primary_Key = namedtuple('Primary_Key',
                         'source_institution destination_institution subject_area group_number')
Source_Course = namedtuple('Source_Course',
                           'course_id min_credits max_credits min_gpa max_gpa')
Destination_Course = namedtuple('Destination_Course',
                                'course_id transfer_credits')
transfer_rules = dict()
source_courses = dict()
destination_courses = dict()

if args.progress:
  print('\nProcessing csv file.', file=sys.stderr)
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

      # The source_discipline for the rule group may differ from the disciplines of the
      # source courses. What we call the source_discipline, CF calls the Component
      # Subject Area, which in practice is an arbitrary string. The rules contain lists
      # of all disciplines and cuny_subjects covered by each rule.
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
                                                    record.src_min_units,
                                                    record.src_max_units,
                                                    record.min_grade_pts,
                                                    record.max_grade_pts))
      destination_courses[primary_key].add(Destination_Course(int(record.destination_course_id),
                                                              record.units_taken))

if args.progress:
  secs = perf_counter() - start_time
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'\n  That took {mins} min {secs} sec.\nLooking up courses:', file=sys.stderr)
  start_time = perf_counter()

# Create list of source disciplines and source cuny_subjects for each rule. Report and
# drop any course lookups that fail. Likewise for destination courses; report inactives
course_id_cache = dict()
bogus_course_ids = set()
source_disciplines = dict()
source_subjects = dict()
bogus_keys = set()
total_keys = len(source_courses.keys())
keys_so_far = 0
for key in source_courses.keys():
  keys_so_far += 1
  if args.progress and 0 == keys_so_far % 1000:
    print(f'\r{keys_so_far:,}/{total_keys:,} keys. {100 * keys_so_far / total_keys:.1f}%',
          file=sys.stderr, end='')
  try:
    for course in source_courses[key]:
      print(f'212 {course}')
      if course.course_id in bogus_course_ids:
        raise Failed_Course_Error(course.course_id)
      assert 94933 not in course_id_cache.keys(), \
      f'now what? {course}'
      if course.course_id not in course_id_cache.keys():
        print(f'Adding {course.course_id} to course_id cache')
        cursor.execute("""select course_id,
                                 offer_nbr,
                                 institution,
                                 discipline,
                                 cuny_subject,
                                 min_credits,
                                 max_credits,
                                 course_status
                          from courses
                          where course_id = %s""", (course.course_id,))
        if cursor.rowcount == 0:
          conflicts.write('Source course lookup failed for {:06} in rule {}. Rule deleted.\n'
                          .format(course.course_id, mk_rule_key(key)))
          bogus_course_ids.add(course.course_id)
          raise Failed_Course_Error(course.course_id)
        else:
          course_id_cache[course.course_id] = cursor.fetchall()
          print('233', course.course_id, course_id_cache[course.course_id])
      else:
        print(f'{course.course_id} was already cached')
      source_disciplines_set = set()
      source_subjects_set = set()
      for course_info in course_id_cache[course.course_id]:
        print('239', course.course_id, course_info)
        source_disciplines_set.add(course_info.discipline)
        source_subjects_set.add(course_info.cuny_subject)
      source_disciplines[key] = ':' + ':'.join(sorted(source_disciplines_set)) + ':'
      source_subjects[key] = ':' + ':'.join(sorted(source_subjects_set)) + ':'
    for course in destination_courses[key]:
      if course.course_id not in course_id_cache.keys():
        cursor.execute("""select course_id, offer_nbr, institution, discipline, course_status
                          from courses
                          where course_id = %s""", (course.course_id,))
        if cursor.rowcount == 0:
          conflicts.write('Destination course lookup failed for {:06} in rule {}. Rule deleted.\n'
                          .format(course.course_id, mk_rule_key(key)))
          raise Failed_Course_Error('course.course_id')
        else:
          course_id_cache[course.course_id] = cursor.fetchall()
        for course_info in course_id_cache[course.course_id]:
          if course_info.course_status != 'A':
            conflicts.write('Inactive destination course_id ({:06}) in rule {}. Rule retained.\n'.
                            format(course.course_id, mk_rule_key(key)))
  except Failed_Course_Error as fce:
    bogus_keys.add(key)
num_bogus_keys = len(bogus_keys)
if args.progress:
  secs = perf_counter() - start_time
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'\n  That took {mins} min {secs} sec.', file=sys.stderr)
  print(f'\nRemoving {num_bogus_keys:,} rules that reference nonexistent courses.', file=sys.stderr)
  start_time = perf_counter()

# Prune rules that reference non-existent course_ids
if num_bogus_keys > 0:
  for key in bogus_keys:
    del source_courses[key]
    del destination_courses[key]
    if key in source_disciplines.keys():
      del source_disciplines[key]
if args.progress:
  secs = perf_counter() - start_time
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'  That took {mins} min {secs} sec.', file=sys.stderr)
  print('\nPopulating the tables.', file=sys.stderr)
  start_time = perf_counter()

if args.report:
  print('  {:,} Source courses\n  {:,} Source disciplines\n  {:,} Destination courses'
        .format(len(source_courses), len(source_disciplines), len(destination_courses)))

# Populate the db tables
total_keys = len(source_courses.keys())
keys_so_far = 0
for key in source_courses.keys():
  keys_so_far += 1
  if args.progress and 0 == keys_so_far % 1000:
    print(f'\r{keys_so_far:,}/{total_keys:,} keys. {100 * keys_so_far / total_keys:.1f}%',
          file=sys.stderr, end='')
  key_asdict = key._asdict()
  primary_key = [key_asdict[k] for k in key_asdict.keys()]
  rule_values = primary_key + [source_disciplines[key] + source_subjects[key]]
  cursor.execute("""insert into transfer_rules (
                                  source_institution,
                                  destination_institution,
                                  subject_area,
                                  group_number,
                                  source_disciplines,
                                  source_subjects)
                                values (%s, %s, %s, %s, %s, %s) returning id""",
                 rule_values)
  rule_id = cursor.fetchone()[0]
  for source_course in source_courses[key]:
    source_values = [rule_id] + primary_key + [source_course.course_id,
                                               source_course.min_credits,
                                               source_course.max_credits,
                                               source_course.min_gpa,
                                               source_course.max_gpa]
    cursor.execute("""insert into source_courses (
                                    rule_id,
                                    source_institution,
                                    destination_institution,
                                    subject_area,
                                    group_number,
                                    course_id,
                                    min_credits,
                                    max_credits,
                                    min_gpa,
                                    max_gpa)
                                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   """, source_values)
  for destination_course in destination_courses[key]:
    destination_values = [rule_id] + primary_key + [destination_course.course_id,
                                                    destination_course.transfer_credits]
    cursor.execute("""insert into destination_courses (
                                    rule_id,
                                    source_institution,
                                    destination_institution,
                                    subject_area,
                                    group_number,
                                    course_id,
                                    transfer_credits)
                                  values (%s, %s, %s, %s, %s, %s, %s)
                   """, destination_values)

if args.progress:
  secs = perf_counter() - start_time
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'\n  That took {mins} min {secs} sec.', file=sys.stderr)
  cursor.execute('select count(*) from transfer_rules')
  num_rules = cursor.fetchone()[0]
  print(f'\nThere are {num_rules:,} rules', file=sys.stderr)

conflicts.close()
db.commit()
db.close()

if args.report:
  secs = perf_counter() - app_start
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  num_rules = len(source_courses.keys())
  print(f'\nGenerated {num_rules:,} rules in {mins} min {secs} sec.')
