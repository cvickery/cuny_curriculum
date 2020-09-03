#! /usr/local/bin/python3
"""
  Clear and re-populate the transfer_rules, source_courses, and destination_courses tables using
  the result of the CUNYfirst query, QNS_CV_SR_TRNS_INTERNAL_RULES.

  Note and ignore query records that have invalid course_id fields (lookup fails).

  Note, but keep, query records where the textual description of the course does not match the
  catalog description. (Mismatched institution and/or discipline.) Use the course_id and catalog
  info.

  Note rules where the actual source course min/max credits are not in the range specified in the
  rule. To avoid extra course lookups later, the source_courses get the actual course min/max.

  The query includes a “Subject Credit source” field which can have one of the following
  values and descriptions:
    C   Use Catalog Units (Catalog)
    E   Specify Maximum Units (External)
    R   Specify Fixed Units (Rule)
  This field is included in the source_courses table.

  CF uses a pair of values (a string and an integer) to identify sets of related rule
  components. They call the string part the Component Subject Area, but it turns out to be
  an arbitrary string, which is sometimes an external subject area, maybe is a discipline, or
  maybe is just a string that somebody thought was a good idea, perhaps because it identifies
  a program ... or something. Anyway, we call it subject_area . They call the number the
  Src Equivalency Component, and we call it the group_number.

  There is a flag called Transfer Rule in the query. If this flag is false, the rule is ignored.

  Deal with cross-listed courses by allowing a source or destination course_id to represent all
  offer_nbr values for “the course“.

  Design:
    1. Extract information from the CF query: data structures for transfer rule keys and lists of
    source and destination course_ids.
      Note and reject records that reference non-existent institutions
    2. Lookup course_ids
          Note and eliminate rules that specifiy non-existent courses
          Note rules that specify inactive destination courses
          Build lists of source disciplines for all rules
    3. Insert rules and course lists into database tables
"""

import os
import sys
import argparse
import csv

from collections import namedtuple, defaultdict
from datetime import date
from time import perf_counter

from pgconnection import PgConnection

from cuny_divisions import ignore_institutions

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')  # to stderr
parser.add_argument('--report', '-r', action='store_true')    # to stdout
args = parser.parse_args()

app_start = perf_counter()

try:
  terminal = open(os.ttyname(0), 'wt')
except OSError as e:
  # No progress reporting unless run from command line
  terminal = open('/dev/null', 'wt')


# mk_rule_key()
# -------------------------------------------------------------------------------------------------
def mk_rule_key(rule):
  """ Convert a rule tuple to a hyphen-separated string.
  """
  return '{}-{}-{}-{}'.format(rule.source_institution,
                              rule.destination_institution,
                              rule.subject_area,
                              rule.group_number)


# Start Execution
# =================================================================================================
if args.progress:
  print('\nInitializing.', file=terminal)

conn = PgConnection()
cursor = conn.cursor()

# Get most recent transfer_rules query file
cf_rules_file = './latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES.csv'
file_date = date\
    .fromtimestamp(os.lstat(cf_rules_file).st_mtime).strftime('%Y-%m-%d')
num_lines = sum(1 for line in open(cf_rules_file))

if args.report:
  print('\n  Transfer rules query file: {} {}'.format(file_date, cf_rules_file))

# There be some garbage institution "names" in the transfer_rules, but the app’s
# cuny_institutions table is “definitive”.
cursor.execute("""select code
                  from cuny_institutions
                  order by code""")
known_institutions = [record.code for record in cursor.fetchall()]

# Use the disciplines table for reporting cases where the component_subject_area isn't
# there.
cursor.execute("""select institution, discipline
                  from cuny_disciplines""")
valid_disciplines = [(record.institution, record.discipline)
                     for record in cursor.fetchall()]

# Cache the information that might be used for all courses in the cuny_courses table.
# Index by course_id; list info for each offer_nbr.
cursor.execute("""
               select course_id,
                      offer_nbr,
                      institution,
                      discipline,
                      catalog_number,
                      numeric_part(catalog_number) as cat_num,
                      cuny_subject,
                      min_credits,
                      max_credits,
                      course_status from cuny_courses""")
course_cache = defaultdict(list)
for course in cursor.fetchall():
  course_cache[course.course_id].append(course)

# Logging file
conflicts = open('transfer_rule_conflicts.log', 'w')

# Templates for building the three tables
Rule_Key = namedtuple('Rule_Key',
                      'source_institution destination_institution subject_area group_number')

Source_Course = namedtuple('Source_Course', """
                           course_id
                           offer_nbr
                           offer_count
                           discipline
                           catalog_number
                           cat_num
                           cuny_subject
                           min_credits
                           max_credits
                           credits_source
                           min_gpa
                           max_gpa""")
Destination_Course = namedtuple('Destination_Course', """
                                course_id
                                offer_nbr
                                offer_count
                                discipline
                                catalog_number
                                cat_num
                                cuny_subject
                                transfer_credits""")
# Rules dict is keyed by Rule_Key. Values are sets of courses and sets of disciplines
# and subjects.
Rule_Tuple = namedtuple('Rule_Tuple', """
                        source_courses
                        source_disciplines
                        source_subjects
                        destination_courses
                        effective_date""")


def rule_key_to_str(self):
  """ Augment Rule_Key namedtuple with str dunder method.
      Makes error-log file easier to read.
  """
  return (f'{self.source_institution}-{self.destination_institution}'
          f'-{self.subject_area}-{self.group_number}')


setattr(Rule_Key, '__str__', rule_key_to_str)

rules_dict = dict()
num_missing_courses = 0

# Step 1: Go through the CF query file; extract a dict of rules and associated courses.
# -----------------------------------------------------------------
if args.progress:
  print('\nStep 1/2: Process the csv file.', file=terminal)
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
      if args.progress and line_num % 10000 == 0:
        elapsed_time = perf_counter() - start_time
        total_time = num_lines * elapsed_time / line_num
        secs_remaining = total_time - elapsed_time
        mins_remaining = int((secs_remaining) / 60)
        secs_remaining = int(secs_remaining - (mins_remaining * 60))
        print('line {:,}/{:,} ({:.1f}%) Estimated time remaining: {}:{:02}\r'
              .format(line_num,
                      num_lines,
                      100 * line_num / num_lines,
                      mins_remaining,
                      secs_remaining),
              end='', file=terminal)

      try:
        record = Record._make(line)
      except TypeError as te:
        print(f'{te}\nline {line_num}:, {line}', file=sys.stderr)
        continue

      # 2020-0902: Check "Transfer Course" flag
      if record.transfer_course != 'Y':
        continue

      if record.source_institution in ignore_institutions or \
         record.destination_institution in ignore_institutions:
         conflicts.write(f'Ignoring rule from {record.source_institution} to '
                         f'{record.destination_institution}\n')
         continue
      try:
        rule_key = Rule_Key(record.source_institution,
                            record.destination_institution,
                            record.component_subject_area,
                            int(record.src_equivalency_component))
      except ValueError as e:
        conflicts.write(f'Unable to construct Rule Key for {record}.\n{e}')
        continue

      # Determine the effective date of the row (the latest effective date of any of the
      # tables that make up the CF query).
      date_vals = [[int(f) for f in field.split('/')]for field in
                   [record.transfer_subject_eff_date,
                    record.transfer_component_eff_date,
                    record.source_inst_eff_date,
                    record.transfer_to_eff_date,
                    record.crse_offer_eff_date,
                    record.crse_offer_view_eff_date]]
      effective_date = max([date(month=v[0], day=v[1], year=v[2]) for v in date_vals])
      if rule_key not in rules_dict.keys():
        # source_courses, source_disciplines, source_subjects, destination_courses,
        # Effective Date
        rules_dict[rule_key] = Rule_Tuple(set(), set(), set(), set(), effective_date)
      elif effective_date > rules_dict[rule_key].effective_date:
        rules_dict[rule_key].effective_date.replace(year=effective_date.year,
                                                    month=effective_date.month,
                                                    day=effective_date.day)

      # 2018-07-19: The following two tests never fail
      if record.source_institution not in known_institutions:
        conflicts.write('Unknown institution: {} for rule {}. Rule ignored.\n'
                        .format(record.source_institution, rule_key))
        del(rules_dict[rule_key])
        continue
      if record.destination_institution not in known_institutions:
        conflicts.write('Unknown institution: {} for rule {}. Rule ignored.\n'
                        .format(record.destination_institution, rule_key))
        del(rules_dict[rule_key])
        continue

      if (record.source_institution, record.component_subject_area) \
         not in valid_disciplines:
        # Report the anomaly, but accept the record.
        conflicts.write(
            'Notice: Component Subject Area {} not a CUNY Subject Area for rule {}. '
            'Record kept.\n'.format(record.component_subject_area, rule_key))

      # Process source_course_id
      # ------------------------
      course_id = int(record.source_course_id)
      offer_nbr = int(record.source_offer_nbr)
      if course_id not in course_cache.keys():
        conflicts.write('Source course {:06}.{} not in course catalog for rule {}. '
                        'Rule ignored.\n'.format(course_id, offer_nbr, rule_key))
        del(rules_dict[rule_key])
        num_missing_courses += 1
        continue
      # Only one course gets added to the rule, but all (cross-listed) disciplines and
      # subjects
      courses = course_cache[course_id]
      course = courses[0]

      # Eliminate rules with zero-credit source courses.
      if float(course.max_credits) < 0.1:
        conflicts.write(f'Source_course {course_id} in rule {rule_key} is a zero-credit course. '
                        f'Rule ignored.\n')
        del(rules_dict[rule_key])
        continue

      if float(course.min_credits) < float(record.src_min_units):
        conflicts.write('Source course {:06} has {} min credits, '
                        'but rule {} speifies {} min units\n'
                        .format(course.course_id,
                                course.min_credits,
                                rule_key,
                                record.src_min_units))
      if float(course.max_credits) > float(record.src_max_units):
        conflicts.write('Source course {:06} has {} max credits, '
                        'but rule {} speifies {} max units\n'
                        .format(course.course_id,
                                course.max_credits,
                                rule_key,
                                record.src_max_units))
      source_course = Source_Course(course_id,
                                    offer_nbr,
                                    len(courses),
                                    course.discipline,
                                    course.catalog_number,
                                    float(course.cat_num),
                                    course.cuny_subject,
                                    course.min_credits,
                                    course.max_credits,
                                    record.subject_credit_source,
                                    record.min_grade_pts,
                                    record.max_grade_pts)
      rules_dict[rule_key].source_courses.add(source_course)
      fail = False
      for course in courses:
        if course.cat_num < 0:
          conflicts.write(
              'Source course {:06} with non-numeric catalog number {} for rule {}. '
              'Rule ignored.\n'.format(course_id, course.catalog_number, rule_key))
          fail = True
          break
        rules_dict[rule_key].source_disciplines.add(course.discipline)
        rules_dict[rule_key].source_subjects.add(course.cuny_subject)
      if fail:
        rules_dict.pop(rule_key)
        continue

      # Process destination_course_id
      # -----------------------------
      course_id = int(record.destination_course_id)
      offer_nbr = int(record.destination_offer_nbr)
      if course_id not in course_cache.keys():
        conflicts.write('Destination course {:06}.{} not in catalog for rule {}. '
                        'Rule ignored.\n'.format(course_id, offer_nbr, rule_key))
        rules_dict.pop(rule_key)
        continue
      courses = course_cache[course_id]
      destination_course = Destination_Course(course_id,
                                              offer_nbr,
                                              len(courses),
                                              courses[0].discipline,
                                              courses[0].catalog_number,
                                              float(courses[0].cat_num),
                                              courses[0].cuny_subject,
                                              record.units_taken)
      rules_dict[rule_key].destination_courses.add(destination_course)
      if len(courses) > 1:
        conflicts.write(
            'Destination course_id {:06} for rule {} is cross-listed {} times. '
            'Rule retained.\n'.format(destination_course.course_id, rule_key,
                                      len(course_cache[destination_course.course_id])))
      fail = False
      for course in courses:
        if course.cat_num < 0:
          conflicts.write('Destination course {:06} with non-numeric catalog number {} '
                          'for rule {}. Rule ignored.\n'
                          .format(course_id, course.catalog_number, rule_key))
          fail = True
          break
        if course.course_status != 'A':
          conflicts.write('Inactive destination course_id ({:06}) in rule {}. Rule retained.\n'.
                          format(course_id, rule_key))
      if fail:
        rules_dict.pop(rule_key)
        continue

if args.progress:
  print(f'\n  Found {len(rules_dict.keys()):,} rules', file=terminal)
  secs = perf_counter() - start_time
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'\n  That took {mins} min {secs} sec.', file=terminal)
  print('\nStep 2/2: Populate the three tables', file=terminal)
  start_time = perf_counter()

# Step 2
# -------------------------------------------------------------------------------------------------
# Clear the three db tables and re-populate them.

cursor.execute('truncate source_courses, destination_courses, transfer_rules cascade')
# update the update date
cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'transfer_rules'""".format(file_date, cf_rules_file))

total_keys = len(rules_dict.keys())
keys_so_far = 0
for rule_key in rules_dict.keys():
  keys_so_far += 1
  if args.progress and 0 == keys_so_far % 1000:
    print(f'\r{keys_so_far:,}/{total_keys:,} keys. {100 * keys_so_far / total_keys:.1f}%',
          end='', file=terminal)

  # Build the colon-delimited discipline and subject strings
  source_disciplines_str = ':' + ':'.join(sorted(rules_dict[rule_key].source_disciplines)) + ':'
  source_subjects_str = ':' + ':'.join(sorted(rules_dict[rule_key].source_subjects)) + ':'

  # Insert the rule, getting back it's id
  cursor.execute("""insert into transfer_rules (
                                  source_institution,
                                  destination_institution,
                                  subject_area,
                                  group_number,
                                  source_disciplines,
                                  source_subjects,
                                  effective_date)
                                values (%s, %s, %s, %s, %s, %s, %s) returning id""",
                 rule_key + (source_disciplines_str,
                             source_subjects_str,
                             rules_dict[rule_key].effective_date.isoformat()))
  rule_id = cursor.fetchone()[0]

  # Sort and insert the source_courses
  for course in sorted(rules_dict[rule_key].source_courses,
                       key=lambda c: (c.discipline, c.cat_num)):
    cursor.execute("""insert into source_courses
                                  (
                                    rule_id,
                                    course_id,
                                    offer_nbr,
                                    offer_count,
                                    discipline,
                                    catalog_number,
                                    cat_num,
                                    cuny_subject,
                                    min_credits,
                                    max_credits,
                                    credits_source,
                                    min_gpa,
                                    max_gpa
                                  )
                                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   """, (rule_id, ) + course)

  # Sort and insert the destination_courses
  for course in sorted(rules_dict[rule_key].destination_courses,
                       key=lambda c: (c.discipline, c.cat_num)):
    cursor.execute("""insert into destination_courses
                                  (
                                    rule_id,
                                    course_id,
                                    offer_nbr,
                                    offer_count,
                                    discipline,
                                    catalog_number,
                                    cat_num,
                                    cuny_subject,
                                    transfer_credits
                                  )
                                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   """, (rule_id, ) + course)

cursor.execute('select count(*) from transfer_rules')
num_rules = cursor.fetchone()[0]
if args.progress:
  secs = perf_counter() - start_time
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'\n  That took {mins} min {secs} sec.', file=terminal)
  print(f'\nThere are {num_rules:,} rules', file=terminal)

conflicts.close()
conn.commit()
conn.close()

if args.report:
  secs = perf_counter() - app_start
  mins = int(secs / 60)
  secs = int(secs - 60 * mins)
  print(f'\n  Generated {num_rules:,} rules in {mins} min {secs} sec.')
