#! /usr/local/bin/python3

import csv
import json
import os
import re
import resource
import sys

from argparse import ArgumentParser
from collections import namedtuple
from datetime import date
from math import isclose
from time import perf_counter

import psycopg
from psycopg.rows import namedtuple_row

from cuny_divisions import ignore_institutions
from cuny_departments import ignore_departments

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, [0x200, hard])

start_time = perf_counter()

parser = ArgumentParser(description='Populate the cuny_courses table.')
parser.add_argument('-p', '--progress', action='store_true')
parser.add_argument('-d', '--debug', action='store_true')
args = parser.parse_args()

try:
  terminal = open(os.ttyname(0), 'wt')
except OSError as e:
  # No progress reporting unless run from command line
  terminal = open('/dev/null', 'wt')

if args.progress:
  print('', file=terminal)

logs = open('populate_cuny_courses.log', 'w')
# Get the three query files needed, and be sure they are in sync
cat_file = './latest_queries/QNS_QCCV_CU_CATALOG_NP.csv'
req_file = './latest_queries/QNS_QCCV_CU_REQUISITES_NP.csv'
att_file = './latest_queries/QNS_QCCV_COURSE_ATTRIBUTES_NP.csv'
cat_date = date.fromtimestamp(os.lstat(cat_file).st_mtime).strftime('%Y-%m-%d')
req_date = date.fromtimestamp(os.lstat(req_file).st_mtime).strftime('%Y-%m-%d')
att_date = date.fromtimestamp(os.lstat(att_file).st_mtime).strftime('%Y-%m-%d')
if not ((cat_date == req_date) and (req_date == att_date)):
  logs.write('*** FILE DATES DO NOT MATCH ***\n')
  print('*** FILE DATES DO NOT MATCH ***', file=sys.stderr)
  for d, file in [[att_date, att_file], [cat_date, cat_file], [req_date, req_file]]:
    print(f'  {d} {file}', file=sys.stderr)

with psycopg.connect('dbname=cuny_curriculum', row_factory=namedtuple_row, autocommit=True) as conn:

  conn.execute("""
                 update updates
                 set update_date = '{}', file_name = '{}'
                 where table_name = 'cuny_courses'""".format(cat_date, cat_file))

  if args.debug:
    print("""Catalog file\t{} ({})\nRequisites file\t{} ({})\nAttributes file\t{} ({})
          """.format(cat_file, cat_date, req_file, req_date, att_file, att_date))

  # Cache cuny_institutions
  cursor = conn.execute('select * from cuny_institutions')
  all_colleges = [inst.code for inst in cursor.fetchall()]

  # Cache primary keys from the disciplines table
  cursor = conn.execute('select institution, discipline from cuny_disciplines')
  discipline_keys = [(row.institution, row.discipline) for row in cursor.fetchall()]

  # Cache a dictionary of course requisites; key is (institution, discipline, catalog_nbr)
  with open(req_file, newline='', errors='replace') as csvfile:
    req_reader = csv.reader(csvfile)
    requisites = {}
    cols = None
    for row in req_reader:
      if cols is None:
        row[0] = row[0].replace('\ufeff', '')
        if 'Institution' == row[0]:
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
      else:
        # discipline and catalog course number are called subject and catalog
        value = row[cols.index('descr_of_pre_co-requisites')].strip().replace("'", "’")
        if value != '':
          key = (row[cols.index('institution')],
                 row[cols.index('subject')],
                 row[cols.index('catalog')].strip())
          requisites[key] = value
  if args.debug:
    print('{:,} requisites'.format(len(requisites)))

  # Populate the course_attributes table; cache the (name, value) pairs
  attribute_keys = []
  with open('latest_queries/SR742A___CRSE_ATTRIBUTE_VALUE.csv', newline='',
            errors='replace') as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
      if reader.line_num == 1:
        Row = namedtuple('Row', [val.lower().replace(' ', '_').replace('/', '_') for val in line])
        cursor.execute('delete from course_attributes')
      else:
        row = Row._make(line)
        key = (row.crse_attr, row.crsatr_val)
        if key in attribute_keys:
          logs.write(f'ERROR: duplicate value for course_attributes key {key}. Ignored.\n')
        else:
          attribute_keys.append(key)
          conn.execute('insert into course_attributes values(%s, %s, %s)',
                       (row.crse_attr, row.crsatr_val, row.formal_description))
  if args.progress:
    print(f'Inserted {len(attribute_keys)} rows into table course_attributes.', file=terminal)

  # Each (name, value) pair must appear must appear no more than once per (course_id, offer_nbr).
  # The attribute_pairs dict keys are (course_id, offer_nbr); the values are arrays of (name, value)
  # pairs.
  # Report anomalies.
  attribute_pairs = dict()
  with open(att_file, newline='', errors='replace') as csvfile:
    att_reader = csv.reader(csvfile)
    cols = None
    for line in att_reader:
      if cols is None:
        line[0] = line[0].replace('\ufeff', '')
        if 'Institution' == line[0]:
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
          Row = namedtuple('Row', cols)
      else:
        row = Row._make(line)
        key = (int(row.course_id), int(row.course_offering_nbr))
        name_value = (row.course_attribute, row.course_attribute_value)
        # There are bogus (name, value) attributes in the attributes file that don’t appear in the
        # SR742A___CRSE_ATTRIBUTE_VALUE query. Report, create bogus row in the course_attributes
        # table, and then process the (course_id, offer_nbr) that referenced the bogus attribute
        if name_value not in attribute_keys:
          logs.write(
              '{:6}: Reference to {}, which is not a known course_attribute. Adding “Bogus” row.\n'
              .format(row.course_id, name_value))
          conn.execute('insert into course_attributes values (%s, %s, %s)', (name_value[0],
                                                                             name_value[1],
                                                                             'Bogus'))
          attribute_keys.append(name_value)
        if key not in attribute_pairs.keys():
          attribute_pairs[key] = []
        if name_value in attribute_pairs[key]:
          logs.write(f'ERROR: Attempt to re-add {name_value} to attribute_pairs[{key}]\n')
        else:
          attribute_pairs[key].append(name_value)

  # Now process the rows from the course catalog query.
  # -----------------------------------------------------------------------------------------------
  """ Course components appear in separate rows of the CUNYfirst query, so they have to be built up
      incrementally as new rows are encountered.
  """
  Component = namedtuple('Component', 'component component_contact_hours')

  lookup_details_query = """
    select contact_hours, primary_component, min_credits, max_credits, components
      from cuny_courses
     where course_id = %s
       and offer_nbr = %s"""
  update_components_query = """
    update cuny_courses set components = %s
     where course_id = %s
       and offer_nbr = %s
       and discipline = %s
       and catalog_number = %s"""
  course_insertion_query = """insert into cuny_courses values
                              (%s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s,
                               %s,
                               %s, %s, %s, %s, %s,
                               %s, %s, %s, %s)
                            """
  total_lines = sum(1 for line in open(cat_file, errors='replace'))
  num_lines = 0
  num_courses = 0
  with open(cat_file, newline='', errors='replace') as csvfile:
    cat_reader = csv.reader(csvfile)
    for line in cat_reader:
      num_lines += 1
      if args.progress and 0 == num_lines % 1000:
        elapsed_seconds = perf_counter() - start_time
        total_seconds = total_lines * (elapsed_seconds / num_lines)
        remaining_seconds = total_seconds - elapsed_seconds
        remaining_minutes = int(remaining_seconds / 60)
        remaining_seconds = int(remaining_seconds - remaining_minutes * 60)
        print('\r' + 80 * ' '
              '\rRow {:,} / {:,}; {:,} courses; {}:{:02} remaining.'.format(num_lines,
                                                                            total_lines,
                                                                            num_courses,
                                                                            remaining_minutes,
                                                                            remaining_seconds),
              end='', file=terminal)

      if cat_reader.line_num == 1:
        line[0] = line[0].replace('\ufeff', '')
        if 'Institution' == line[0]:
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
          Row = namedtuple('Row', cols)
      else:
        row = Row._make(line)
        # Skip inactive and administrative courses; insert others
        #   2017-07-12: Retain inactive courses
        #   2017-07-26: Retain all courses!
        # if row[cols.index('approved')] == 'A' and \
        #    row[cols.index('schedule_course')] == 'Y':

        department = row.acad_org
        discipline = row.subject
        institution = row.institution
        if institution in ignore_institutions or \
           department in ignore_departments:
          continue
        course_id = int(row.course_id)
        offer_nbr = int(row.offer_nbr)
        key = (course_id, offer_nbr)

        # Lookup attribute_pairs and their descriptions for this (course_id, offer_nbr)
        if key not in attribute_pairs.keys():
          course_attributes = 'None'
        else:
          course_attributes = '; '.join(f'{name}:{value}' for name, value in attribute_pairs[key])

        try:
          equivalence_group = int(row.equiv_course_group)
        except ValueError:
          equivalence_group = None

        catalog_number = row.catalog_number.strip()
        component = Component._make([row.component_course_component,
                                    float(row.instructor_contact_hours)])
        primary_component = row.primary_component
        contact_hours = float(row.course_contact_hours)
        min_credits = float(row.min_units)
        max_credits = float(row.max_units)

        with conn.cursor() as lookup_cursor:
          lookup_details_args = [course_id, offer_nbr]
          lookup_cursor.execute(lookup_details_query, lookup_details_args)
          if lookup_cursor.rowcount > 0:
            if lookup_cursor.rowcount > 1:
              logs.write(f'{lookup_details_query} returned multiple rows\n')
              print(f'{lookup_details_query} returned multiple rows', file=sys.stderr)
              exit(1)
            else:
              lookup = lookup_cursor.fetchone()
              # Make sure contact_hours, primary_component, and credits haven’t changed
              if contact_hours != lookup.contact_hours or \
                 primary_component != lookup.primary_component or \
                 min_credits != lookup.min_credits or \
                 max_credits != lookup.max_credits:
                logs.write('Inconsistent hours/credits/component for {}-{} {} {}\n'
                           .format(course_id, offer_nbr, discipline, catalog_number))
                print('Inconsistent hours/credits/component for {}-{} {} {}'
                      .format(course_id, offer_nbr, discipline, catalog_number), file=sys.stderr)
                exit(1)

              components = [Component._make(c) for c in lookup.components]
              if component not in components:
                components.append(component)
                # Do the following at display time, putting the primary_component first.
                # Order components alphabetically, but LEC is always first if present.
                # components.sort()
                # if 'LEC' in components and components[0] != 'LEC':
                #   components.remove('LEC')
                #   components = ['LEC'] + components
                with conn.cursor() as update_cursor:
                  update_components_args = [json.dumps(components), course_id, offer_nbr,
                                            discipline, catalog_number]
                  update_cursor.execute(update_components_query, update_components_args)
              else:
                logs.write('Repeated component: {} {} {} {} {} :: {}\n'.format(course_id,
                                                                               offer_nbr,
                                                                               institution,
                                                                               discipline,
                                                                               catalog_number,
                                                                               component))
          else:
            components = [component]
            cuny_subject = row.subject_external_area
            if cuny_subject == '':
              cuny_subject = 'missing'
            title = row.long_course_title.replace("'", "’")\
                                         .replace('\r', '')\
                                         .replace('\n', ' ')\
                                         .replace('( ', '(')
            short_title = row.short_course_title.replace("'", "’")\
                                                .replace('\r', '')\
                                                .replace('\n', ' ')\
                                                .replace('( ', '(')

            designation = row.designation

            requisite_str = 'None'
            if (institution, discipline, catalog_number) in requisites.keys():
              requisite_str = requisites[(institution, discipline, catalog_number)]
            description = row.descr.replace("'", "’")
            career = row.career
            repeatable = row.repeat_for_credit == 'Y'
            course_status = row.crse_catalog_status
            discipline_status = row.subject_eff_status
            can_schedule = row.schedule_course
            effective_date = row.crse_catalog_effective_date

            # Report and ignore cases where the institution-discipline pair doesn’t exist in the
            # cuny_disciplines table.
            if (institution, discipline) not in discipline_keys:
              logs.write(f'{discipline} is not a known discipline at {institution}\n'
                         f'  Ignoring {discipline} {catalog_number}.\n')
              continue

            try:
              course_insertion_values = (course_id, offer_nbr, equivalence_group, institution,
                                         cuny_subject, department, discipline, catalog_number, title,
                                         short_title, json.dumps(components), contact_hours,
                                         min_credits, max_credits, repeatable, primary_component,
                                         requisite_str, designation, description, career,
                                         course_status, discipline_status, can_schedule,
                                         effective_date, course_attributes)
              cursor.execute(course_insertion_query, course_insertion_values)
              num_courses += 1
              if args.debug:
                print(course_insertion_values)
            except Exception as err:
              logs.write(f'{err}\n{cursor._query.query}')
              sys.exit(str(err))

  run_time = perf_counter() - start_time
  minutes = int(run_time / 60.)
  min_suffix = 's'
  if minutes == 1:
    min_suffix = ''
  seconds = run_time - (minutes * 60)
  logs.write('Inserted {:,} courses in {} minute{} and {:0.1f} seconds.\n'.format(num_courses,
                                                                                  minutes,
                                                                                  min_suffix,
                                                                                  seconds))

  if args.progress:
    print('', file=terminal)
