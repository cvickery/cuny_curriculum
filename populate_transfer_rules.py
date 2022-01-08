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

  The query includes a “Subject Credit Source” field which can have one of the following
  values and descriptions:
    C   Use Catalog Units (Catalog)
    E   Specify Maximum Units (External)
    R   Specify Fixed Units (Rule)

  I _think_ this is how to determine the actual number of credits transferred. I now (April 2021)
  add this to each source and destination course, and a collection of all values found for a rule in
  the rule itself. I'm also adding a boolean to entries in the destination_courses table to mark
  blanket credit courses to see how this flag correlates with the value(s) of the Subject Credit
  Source field..

  CF uses a pair of values (a string and an integer) to identify sets of related rule
  components. They call the string part the Component Subject Area, but it turns out to be
  an arbitrary string, which is sometimes an external subject area, maybe is a discipline, or
  maybe is just a string that somebody thought was a good idea, perhaps because it identifies
  a program ... or something. Anyway, we call it subject_area. They call the number the
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
          Note and eliminate rules where the sending institution does not match the sending course.
          Note rules that specify inactive destination courses
          Build lists of source disciplines for all rules
    3. Insert rules and course lists into database tables
"""

import argparse
import csv
import json
import os
import resource
import sys

from collections import namedtuple, defaultdict
from datetime import date
from time import perf_counter

from pgconnection import PgConnection

from cuny_divisions import ignore_institutions

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, [0x400, hard])

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

# There are some garbage institution "names" in the transfer_rules, but the app’s
# cuny_institutions table is “definitive”.
cursor.execute("""select code
                  from cuny_institutions
                  order by code""")
known_institutions = [record.code for record in cursor.fetchall()]

# # Use the disciplines table for reporting cases where the component_subject_area isn't
# # there.
# cursor.execute("""select institution, discipline
#                   from cuny_disciplines""")
# valid_disciplines = [(record.institution, record.discipline)
#                      for record in cursor.fetchall()]

# Cache the information that might be used for all courses in the course_cache dict.
# Index by course_id, but include info for each offer_nbr.
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
                      course_status,
                      designation in ('MLA', 'MNL') as is_mesg,
                      attributes ~* 'BKCR' as is_bkcr
                      from cuny_courses""")
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
                           credit_source
                           min_gpa
                           max_gpa
                           source_aliases
                           """)
Destination_Course = namedtuple('Destination_Course', """
                                course_id
                                offer_nbr
                                offer_count
                                discipline
                                catalog_number
                                cat_num
                                cuny_subject
                                transfer_credits
                                credit_source
                                is_mesg
                                is_bkcr
                                """)
# Rules dict is keyed by Rule_Key. Values are sets of courses and sets of disciplines
# and subjects.
# 2021-01-10: add rule priority to handle tied gpa requirements in source courses.
# 2021-04-17: add credit sources
Rule_Tuple = namedtuple('Rule_Tuple', """
                        source_courses
                        source_disciplines
                        source_subjects
                        destination_courses
                        destination_disciplines
                        src_credit_sources
                        dst_credit_sources
                        priority
                        effective_date""")


def rule_key_to_str(self):
  """ Augment Rule_Key namedtuple with str dunder method.
      Makes error-log file easier to read.
  """
  return (f'{self.source_institution}:{self.destination_institution}'
          f':{self.subject_area}:{self.group_number}')


setattr(Rule_Key, '__str__', rule_key_to_str)

rules_dict = dict()

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
                            record.component_subject_area.replace(' ', '_'),
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
        # source_courses, source_disciplines, source_subjects,
        # destination_courses, destination_disciplines,
        # source_credit_sources, destination_credit_sources,
        # Rule Priority, Effective Date
        rules_dict[rule_key] = Rule_Tuple(set(), set(), set(), set(), set(), set(), set(),
                                          record.transfer_priority, effective_date)
      elif effective_date > rules_dict[rule_key].effective_date:
        rules_dict[rule_key].effective_date.replace(year=effective_date.year,
                                                    month=effective_date.month,
                                                    day=effective_date.day)
        if rules_dict[rule_key].priority != record.transfer_priority:
          conflicts.write(f'\nConflicting priorities for {rule_key}: '
                          f'{rules_dict[rule_key].priority} != {record.transfer_priority} '
                          f'Record kept.\n')

      # Filter out rules where the source or destination institution is bogus.
      if record.source_institution not in known_institutions:
        conflicts.write('Unknown source institution: {} for rule {}. Rule ignored.\n'
                        .format(record.source_institution, rule_key))
        del(rules_dict[rule_key])
        continue
      if record.destination_institution not in known_institutions:
        conflicts.write('Unknown destination institution: {} for rule {}. Rule ignored.\n'
                        .format(record.destination_institution, rule_key))
        del(rules_dict[rule_key])
        continue

      # if (record.source_institution, record.component_subject_area) \
      #    not in valid_disciplines:
      #   # Report the anomaly, but accept the record.
      #   conflicts.write(
      #       'Notice: Component Subject Area {} not a CUNY Subject Area for rule {}. '
      #       'Record kept.\n'.format(record.component_subject_area, rule_key))

      # Process source_course_id
      # ------------------------
      course_id = int(record.source_course_id)
      offer_nbr = int(record.source_offer_nbr)
      try:
        courses = course_cache[course_id]
      except KeyError as ke:
        conflicts.write(f'{rule_key} Source course {course_id:06}.{offer_nbr} not in course '
                        f'catalog. Rule ignored.\n')
        del(rules_dict[rule_key])
        continue

      # Iterate over the matching courses. The one with the same offer_nbr is the source course;
      # others are aliases (cross-listed). Ignore any where the institution is wrong.
      the_source_course = None
      source_aliases = []
      for course in courses:
        # Ignore courses where the rule institution and the course institution don't match
        if course.institution != rule_key.source_institution:
          conflicts.write(f'{rule_key} Source course {course_id:06}.{offer_nbr} institution '
                          f'{courses[0].institution} does not match rule source institution '
                          f'{rule_key.source_institution}. Course ignored.\n')
          continue
        # Ignore courses that don't make sense on the source side: no number in the catalog number,
        # msg, bkcr, and zero-credit courses
        if (course.cat_num < 0
                or course.is_mesg
                or course.is_bkcr
                or float(course.max_credits) < 0.1):
          # conflicts.write(
          #     f'{rule_key} Source course {course_id:06}: looks bogus {course.catalog_number=} '
          #     f'{course.is_mesg=} {course.is_bkcr=}. Rule Kept.\n')

          # Report zero-credit source courses.
          # elif float(course.max_credits) < 0.1:
          #   conflicts.write(f'{rule_key} Source course {course_id:06} is zero credits. '
          #                   f'Rule Kept.\n')
          continue

        if course.offer_nbr == offer_nbr:
          the_source_course = course
        else:
          source_aliases.append(course)

      # Make sure the source course was found
      if the_source_course is None:
        conflicts.write(f'{rule_key} Source course {course_id:06}.{offer_nbr} has no matching '
                        f'offer number in course_catalog. Rule ignored.\n')
        del(rules_dict[rule_key])
        continue

      # Only one course gets added to the rule, but all (cross-listed) disciplines and
      # subjects

      source_course = Source_Course(the_source_course.course_id,
                                    the_source_course.offer_nbr,
                                    len(courses),
                                    the_source_course.discipline,
                                    the_source_course.catalog_number,
                                    float(the_source_course.cat_num),
                                    the_source_course.cuny_subject,
                                    the_source_course.min_credits,
                                    the_source_course.max_credits,
                                    record.subject_credit_source,
                                    record.min_grade_pts,
                                    record.max_grade_pts,
                                    json.dumps(source_aliases))
      rules_dict[rule_key].source_courses.add(source_course)
      rules_dict[rule_key].src_credit_sources.add(record.subject_credit_source)

      # Add all source disciplines and cuny_subjects to the rule
      for course in [the_source_course] + source_aliases:
        rules_dict[rule_key].source_disciplines.add(course.discipline)
        rules_dict[rule_key].source_subjects.add(course.cuny_subject)

        # The following check fails 3M times; it's the norm (at some schools) to specify 0-99
        # credits at the receiving side. Retained as comments for documentation purposes
        # Report rules with inconsistent min/max source credits
        # if float(course.min_credits) != float(record.src_min_units):
        #   conflicts.write(f'{rule_key} Source course {course.course_id:06}:{course.offer_nbr} '
        #                   f'has {course.min_credits} min credits, but rule says '
        #                   f'{record.src_min_units=}. Rule Kept.\n')
        # if float(course.max_credits) != float(record.src_max_units):
        #   conflicts.write(f'{rule_key} Source course {course.course_id:06}:{course.offer_nbr} '
        #                   f'has {course.max_credits} max credits, but rule says '
        #                   f'{record.src_max_units=} Rule Kept.\n')

      # Process destination_course_id
      # -----------------------------
      course_id = int(record.destination_course_id)
      offer_nbr = int(record.destination_offer_nbr)

      try:
        courses = course_cache[course_id]
      except KeyError as ke:
        conflicts.write(f'{rule_key} Destination course {course_id:06} not in catalog. '
                        f'Rule Ignored.\n')
        rules_dict.pop(rule_key)
        continue
      # Ignore rules where the destination is not in our catalog of undergraduate courses
      if len(courses) == 0:
        conflicts.write('{rule_key}: Destination course {course_id}:{offer_nbr} not in '
                        f'undergraduate catalog. Rule Ignored\n')
        rules_dict.pop(rule_key)
        continue

      destination_course = Destination_Course(course_id,
                                              offer_nbr,
                                              len(courses),
                                              courses[0].discipline,
                                              courses[0].catalog_number,
                                              float(courses[0].cat_num),
                                              courses[0].cuny_subject,
                                              record.units_taken,
                                              record.subject_credit_source,
                                              courses[0].is_mesg,
                                              courses[0].is_bkcr)
      rules_dict[rule_key].destination_courses.add(destination_course)
      rules_dict[rule_key].destination_disciplines.add(destination_course.discipline)
      rules_dict[rule_key].dst_credit_sources.add(record.subject_credit_source)

      if len(courses) > 1:
        conflicts.write(
            f'{rule_key} Destination course {destination_course.course_id:06} is cross-listed '
            f'{len(courses)} times. Rule Kept.\n')

      # Report weirdnesses
      for course in courses:
        if not (course.is_mesg or course.is_bkcr) and course.cat_num < 0:
          conflicts.write(f'{rule_key} Destination course {course.course_id:06} with non-numeric '
                          f'catalog number ‘{course.catalog_number}’. Rule Kept.\n')
        if course.course_status != 'A':
          conflicts.write(f'{rule_key} Destination course {course_id:06} is inactive. '
                          f'Rule Kept.\n')

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
  assert ' ' not in rule_key, f'{rule_key} has a space in it'

  keys_so_far += 1
  if args.progress and 0 == keys_so_far % 1000:
    print(f'\r{keys_so_far:,}/{total_keys:,} keys. {100 * keys_so_far / total_keys:.1f}%',
          end='', file=terminal)

  # Build the colon-delimited discipline and subject strings
  source_disciplines_str = ':' + ':'.join(sorted(rules_dict[rule_key].source_disciplines)) + ':'
  destination_disciplines_str = ':'.join(sorted(rules_dict[rule_key].destination_disciplines))
  source_subjects_str = ':' + ':'.join(sorted(rules_dict[rule_key].source_subjects)) + ':'
  sending_courses = ':'.join(sorted([f'{c.course_id:06}.{c.offer_nbr}'
                                    for c in rules_dict[rule_key].source_courses]))
  receiving_courses = ':'.join(sorted([f'{c.course_id:06}.{c.offer_nbr}'
                                      for c in rules_dict[rule_key].destination_courses]))

  # Insert the rule, getting back it's id
  credit_sources = (f'{"".join(sorted(rules_dict[rule_key].src_credit_sources))}:'
                    f'{"".join(sorted(rules_dict[rule_key].dst_credit_sources))}')
  cursor.execute("""insert into transfer_rules (
                                  source_institution,
                                  destination_institution,
                                  subject_area,
                                  group_number,
                                  rule_key,
                                  source_disciplines,
                                  source_subjects,
                                  sending_courses,
                                  destination_disciplines,
                                  receiving_courses,
                                  credit_sources,
                                  priority,
                                  effective_date)
                                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                returning id""",
                 rule_key + (':'.join([str(part) for part in rule_key]),
                             source_disciplines_str,
                             source_subjects_str,
                             sending_courses,
                             destination_disciplines_str,
                             receiving_courses,
                             credit_sources,
                             rules_dict[rule_key].priority,
                             rules_dict[rule_key].effective_date.isoformat()))
  rule_id = cursor.fetchone()[0]

  # Sort and insert the source_courses
  for course in sorted(rules_dict[rule_key].source_courses,
                       key=lambda c: (c.discipline, c.cat_num, c.offer_nbr)):
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
                                    credit_source,
                                    min_gpa,
                                    max_gpa,
                                    aliases
                                  )
                                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   """, (rule_id, ) + course)

  # Sort and insert the destination_courses
  for course in sorted(rules_dict[rule_key].destination_courses,
                       key=lambda c: (c.discipline, c.cat_num, c.offer_nbr)):
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
                                    transfer_credits,
                                    credit_source,
                                    is_mesg,
                                    is_bkcr
                                  )
                                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
