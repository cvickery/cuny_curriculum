# Two modes of operation:
#   Clear and re-populate the rule_groups, source_courses, and destination_courses tables.
#
#   Note and ignore rules that have invalid course_id fields (lookup fails).
#
#   Note, but keep, rules where the textual description of the course does not match the catalog
#   description. (Mismatched institution and/or discipline.) Use the course_id and catalog info.
#
#   Deal with cross-listed courses by allowing a source or destination course_id to represent all
#   offer_nbr values for “the course“.

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
the_file = './latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES.csv'
file_date = date\
      .fromtimestamp(os.lstat(the_file).st_mtime).strftime('%Y-%m-%d')

cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'rules'""".format(file_date, the_file))

if args.report:
  print('Transfer rules query file: {} {}'.format(file_date, the_file))

num_lines = sum(1 for line in open(the_file))

# There be some garbage institution "names" in the transfer_rules, but the app’s institutions
# table is “definitive”.
cursor.execute("""select *
                  from institutions
                  order by code""")
known_institutions = [inst.code for inst in cursor.fetchall()]

start_time = perf_counter()

""" Populate the three rule information tables
"""
conflicts = open('rule_group_conflicts.log', 'w')

# known_bad_ids = [int(id.split(' ')[0]) for id in open(known_bad_filename)]
# Clear the three tables
cursor.execute('truncate source_courses, destination_courses, rule_groups')

num_groups = 0
num_source_courses = 0
num_destination_courses = 0

with open(the_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  line_num = 0;
  for line in csv_reader:
    if cols == None:
      line[0] = line[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
      Record = namedtuple('Record', cols);
      if args.debug:
        print(cols)
        for col in cols:
          print('{} = {}; '.format(col, cols.index(col), end = ''))
          print()
    else:
      line_num += 1
      if args.progress and line_num % 1000 == 0:
        elapsed_time = perf_counter() - start_time
        total_time = num_lines * elapsed_time / line_num
        secs_remaining = total_time - elapsed_time
        mins_remaining = int((secs_remaining) / 60)
        secs_remaining = int(secs_remaining - (mins_remaining * 60))
        print('line {:,}/{:,} ({:.1f}%) {}:{:02} remaining.\r'.format(line_num,
                                                num_lines,
                                                100 * line_num / num_lines,
                                                mins_remaining,
                                                secs_remaining),
              end='', file=sys.stderr)
      record = Record._make(line)

      # 2018-07-19: The following two tests never fail
      src_institution = record.source_institution
      if src_institution not in known_institutions:
        conflicts.write('Unknown institution: {}\n'.format(src_institution))
        continue
      dest_institution = record.destination_institution
      if dest_institution not in known_institutions:
        conflicts.write('Unknown institution: {}\n'.format(dest_institution))
        continue

      # Assemble the components of the rule group(s)
      #   One row generates multiple rule groups if the source_course_id references multiple (cross-
      #   listed) courses. In this case, create pseudo group_numbers by adding the offer_nbr ÷ 10 to
      #   the group_number given in the record.
      rule_groups = []
      source_course_id = int(record.source_course_id)
      destination_course_id = int(record.destination_course_id)
      min_gpa = float(record.min_grade_pts)
      max_gpa = float(record.max_grade_pts)
      transfer_credits = float(record.units_taken)

      # The source course might reference multiple courses
      cursor.execute("""select institution, offer_nbr, discipline
                        from courses
                        where course_id = %s""", (source_course_id,))
      if cursor.rowcount > 0:
        if cursor.rowcount > 1:
          for course in cursor.fetchall():
            if args.debug:
              print(f'{source_course_id}: {course.institution} {course.offer_nbr} {course.discipline}')
            source_institution = course.institution
            offer_nbr = course.offer_nbr
            if offer_nbr > 4:
              conflicts.write('Bogus offer_nbr ({}) for source_course_id {}.\n{}'.format(offer_nbr,
                                                                                  source_course_id))
              offer_nbr = 1
            if offer_nbr > 4:
              conflicts.write('Bogus offer_nbr ({}) for source_course_id {}.\n{}'.format(offer_nbr,
                                                                                  source_course_id,
                                                                                  record))
              offer_nbr = 1
            source_discipline = course.discipline
            group_number = float(record.src_equivalency_component) + (offer_nbr / 10.0)
            rule_groups.append(dict(source_institution=source_institution,
                                    source_discipline=source_discipline,
                                    group_number=group_number))
        else:
          source_institution, offer_nbr, source_discipline = cursor.fetchone()
          if offer_nbr > 4:
            conflicts.write('Bogus offer_nbr ({}) for source_course_id {}.\n{}'.format(offer_nbr,
                                                                                source_course_id,
                                                                                record))
            offer_nbr = 1
          group_number = float(record.src_equivalency_component) + (offer_nbr / 10.0)
          rule_groups.append(dict(source_institution=source_institution,
                                  source_discipline=source_discipline,
                                  group_number=group_number))
      else:
        conflicts.write(f'Source Course ID {source_course_id} not found for {record}\n')
        continue
      if source_institution != src_institution:
        conflicts.write("""Source institution ({}) != course institution ({})\n{}\n"""\
                        .format(src_institution, source_institution, record))
        continue

      cursor.execute("""select institution
                        from courses
                        where course_id = %s
                          and course_status = 'A'
                        group by institution""", (destination_course_id,))
      if cursor.rowcount > 0:
        if cursor.rowcount > 1:
          conflicts.write('Multiple destination institutions ({}) for {}. {}'.format(cursor.rowcount,
                                                                             destination_course_id,
                                                                             record))
          continue
        destination_institution = cursor.fetchone().institution
      else:
        conflicts.write(f'No active courses with destination_course_id {destination_course_id} for {record}')
        continue
      if destination_institution != dest_institution:
        conflicts.write("""Destination institution ({}) != course institution ({})\n{}\n"""\
                        .format(dest_institution, destination_institution, record))
        continue

      # Create or look up the rule group(s)
      try:
        for rule_group in rule_groups:
          cursor.execute('insert into rule_groups values (%s, %s, %s, %s) on conflict do nothing',
                       (rule_group['source_institution'],
                        rule_group['source_discipline'],
                        rule_group['group_number'],
                        destination_institution))
          num_groups += cursor.rowcount
          if args.debug: print(f'{cursor.query}\n  Rows inserted {cursor.rowcount}')
      except psycopg2.Error as e:
        print(f'ERROR creating/updating rule group for {source_course_id}, {destination_course_id}',
              file=sys.stderr)
        print(cursor.query)
        print(e.pgerror, file=sys.stderr)
        exit(1)

      # Add the source course(s)
      for rule_group in rule_groups:
        cursor.execute("""
                     insert into source_courses values(default, '{}', '{}', {}, '{}', {}, {}, {})
                     on conflict do nothing
                     """.format(rule_group['source_institution'],
                                rule_group['source_discipline'],
                                rule_group['group_number'],
                                destination_institution,
                                source_course_id,
                                min_gpa,
                                max_gpa))
        num_source_courses += cursor.rowcount
      # Add the destination course (to each rule_group)
      for rule_group in rule_groups:
        cursor.execute("""
                     insert into destination_courses values(default, '{}', '{}', {}, '{}', {}, {})
                     on conflict do nothing
                     """.format(rule_group['source_institution'],
                                rule_group['source_discipline'],
                                rule_group['group_number'],
                                destination_institution,
                                destination_course_id,
                                transfer_credits))
        num_destination_courses += cursor.rowcount
  if args.progress:
    print('', file=sys.stderr)
  if args.report:
    print("""\n{:,} Groups\n{:,} Source courses\n{:,} Destination courses
          """.format(num_groups, num_source_courses, num_destination_courses))
    secs = perf_counter() - start_time
    mins = int(secs / 60)
    secs = int(secs - 60 * mins)
    print(f'Completed in {mins}:{secs:02} minutes')
  db.commit()
  db.close()
  conflicts.close()
