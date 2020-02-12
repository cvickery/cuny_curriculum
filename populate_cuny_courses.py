#! /usr/local/bin/python3
#
import psycopg2
from psycopg2.extras import NamedTupleCursor, Json

import csv
import argparse

from datetime import date
from time import perf_counter
import os
import sys
import re

from math import isclose
from collections import namedtuple

from cuny_divisions import ignore_institutions
from cuny_departments import ignore_departments

start_time = perf_counter()
parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')
args = parser.parse_args()

try:
  terminal = open(os.ttyname(0), 'wt')
except OSError as e:
  # No progress reporting unless run from command line
  terminal = open('/dev/null', 'wt')

if args.progress:
  print('', file=terminal)

db = psycopg2.connect('dbname=cuny_curriculum')
cursor = db.cursor(cursor_factory=NamedTupleCursor)
lookup_cursor = db.cursor(cursor_factory=NamedTupleCursor)

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
  exit(1)
cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'cuny_courses'""".format(cat_date, cat_file))

if args.debug:
  print("""Catalog file\t{} ({})\nRequisites file\t{} ({})\nAttributes file\t{} ({})
        """.format(cat_file, cat_date, req_file, req_date, att_file, att_date))

# Cache cuny_institutions
cursor.execute('select * from cuny_institutions')
all_colleges = [inst.code for inst in cursor.fetchall()]

# Cache primary keys from the disciplines table
cursor.execute('select institution, discipline from cuny_disciplines')
discipline_keys = [(r.institution, r.discipline) for r in cursor.fetchall()]

# Cache a dictionary of course requisites; key is (institution, discipline, catalog_nbr)
with open(req_file, newline='') as csvfile:
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
with open('latest_queries/SR742A___CRSE_ATTRIBUTE_VALUE.csv') as csvfile:
  reader = csv.reader(csvfile)
  cols = None
  for line in reader:
    if cols is None:
      if 'Crse Attr' == line[0]:
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      key = (row.crse_attr, row.crsatr_val)
      if key in attribute_keys:
        logs.write(f'ERROR: duplicate value for course_attributes key {key}. Ignored.\n')
      else:
        attribute_keys.append(key)
        cursor.execute('insert into course_attributes values(%s, %s, %s)', (row.crse_attr,
                                                                            row.crsatr_val,
                                                                            row.formal_description))
if args.progress:
  print(f'Inserted {len(attribute_keys)} rows into table course_attributes.', file=terminal)

# Each (name, value) pair must appear must appear no more than once per (course_id, offer_nbr).
# The attribute_pairs dict keys are (course_id, offer_nbr); the values are arrays of (name, value)
# pairs.
# Report anomalies.
attribute_pairs = dict()
with open(att_file, newline='') as csvfile:
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
        cursor.execute('insert into course_attributes values (%s, %s, %s)', (name_value[0],
                                                                             name_value[1],
                                                                             'Bogus'))
        attribute_keys.append(name_value)
      if key not in attribute_pairs.keys():
        attribute_pairs[key] = []
      if name_value in attribute_pairs[key]:
        logs.write(f'ERROR: Attempt to re-add {name_value} to attribute_pairs[{key}]\n')
      else:
        attribute_pairs[key].append(name_value)

# Now process the rows from the courses query.
Component = namedtuple('Component', 'component component_contact_hours')
total_rows = 0
with open(cat_file, newline='') as csvfile:
  cat_reader = csv.reader(csvfile)
  for row in cat_reader:
    total_rows += 1
num_rows = 0
num_courses = 0
with open(cat_file, newline='') as csvfile:
  cat_reader = csv.reader(csvfile)
  cols = None
  for row in cat_reader:
    num_rows += 1
    if args.progress and 0 == num_rows % 1000:
      elapsed_seconds = perf_counter() - start_time
      total_seconds = total_rows * (elapsed_seconds / num_rows)
      remaining_seconds = total_seconds - elapsed_seconds
      remaining_minutes = int(remaining_seconds / 60)
      remaining_seconds = int(remaining_seconds - remaining_minutes * 60)
      print('\r' + 80 * ' '
            '\rRow {:,} / {:,}; {:,} courses; {}:{:02} remaining.'.format(num_rows,
                                                                          total_rows,
                                                                          num_courses,
                                                                          remaining_minutes,
                                                                          remaining_seconds),
            end='', file=terminal)

    if cols is None:
      row[0] = row[0].replace('\ufeff', '')
      if 'Institution' == row[0]:
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
        Cols = namedtuple('Cols', cols)
    else:
      r = Cols._make(row)
      # Skip inactive and administrative courses; insert others
      #   2017-07-12: Retain inactive courses
      #   2017-07-26: Retain all courses!
      # if row[cols.index('approved')] == 'A' and \
      #    row[cols.index('schedule_course')] == 'Y':

      department = r.acad_org
      discipline = r.subject
      institution = r.institution
      if institution in ignore_institutions or \
         department in ignore_departments:
        continue
      course_id = int(r.course_id)
      offer_nbr = int(r.offer_nbr)
      key = (course_id, offer_nbr)

      # Lookup attribute_pairs and their descriptions for this (course_id, offer_nbr)
      if key not in attribute_pairs.keys():
        course_attributes = 'None'
      else:
        course_attributes = '; '.join(f'{name}:{value}' for name, value in attribute_pairs[key])

      try:
        equivalence_group = int(r.equiv_course_group)
      except ValueError:
        equivalence_group = None
      catalog_number = r.catalog_number.strip()
      component = Component._make([r.component_course_component, float(r.instructor_contact_hours)])
      primary_component = r.primary_component
      contact_hours = float(r.course_contact_hours)
      min_credits = float(r.min_units)
      max_credits = float(r.max_units)
      lookup_query = """
          select contact_hours, primary_component, min_credits, max_credits, components
            from cuny_courses
           where course_id = %s
             and offer_nbr = %s
             and discipline = %s
             and catalog_number = %s"""
      lookup_cursor.execute(lookup_query, (course_id, offer_nbr, discipline, catalog_number))
      if lookup_cursor.rowcount > 0:
        if lookup_cursor.rowcount > 1:
          logs.write(f'{lookup_query} returned multiple rows\n')
          print(f'{lookup_query} returned multiple rows', file=sys.stderr)
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
            update_query = """update cuny_courses set components = %s
                            where course_id = %s
                              and offer_nbr = %s
                              and discipline = %s
                              and catalog_number = %s"""
            lookup_cursor.execute(update_query,
                                  (Json(components),
                                   course_id,
                                   offer_nbr,
                                   discipline,
                                   catalog_number))
          else:
            logs.write('Repeated component: {} {} {} {} {} :: {}\n'.format(course_id,
                                                                           offer_nbr,
                                                                           institution,
                                                                           discipline,
                                                                           catalog_number,
                                                                           component))
      else:
        components = [component]
        cuny_subject = r.subject_external_area
        if cuny_subject == '':
          cuny_subject = 'missing'
        title = r.long_course_title.replace("'", "’")\
                                   .replace('\r', '')\
                                   .replace('\n', ' ')\
                                   .replace('( ', '(')
        short_title = r.short_course_title.replace("'", "’")\
                                          .replace('\r', '')\
                                          .replace('\n', ' ')\
                                          .replace('( ', '(')

        designation = r.designation

        requisite_str = 'None'
        if (institution, discipline, catalog_number) in requisites.keys():
          requisite_str = requisites[(institution, discipline, catalog_number)]
        description = r.descr.replace("'", "’")
        career = r.career
        course_status = r.crse_catalog_status
        discipline_status = r.subject_eff_status
        can_schedule = r.schedule_course
        effective_date = r.crse_catalog_effective_date
        # Report and ignore cases where the institution-discipline pair doesn’t exist in the
        # cuny_disciplines table.
        if (institution, discipline) not in discipline_keys:
          logs.write(f'{discipline} is not a known discipline at {institution}\n'
                     f'  Ignoring {discipline} {catalog_number}.\n')
          continue
        try:
          cursor.execute("""insert into cuny_courses values
                            (%s, %s, %s, %s, %s,
                             %s, %s, %s, %s, %s,
                             %s, %s, %s, %s,
                             %s,
                             %s, %s, %s, %s, %s,
                             %s, %s, %s, %s)
                         """,
                         (course_id, offer_nbr, equivalence_group, institution, cuny_subject,
                          department, discipline, catalog_number, title, short_title,
                          Json(components), contact_hours, min_credits, max_credits,
                          primary_component,
                          requisite_str, designation, description, career, course_status,
                          discipline_status, can_schedule, effective_date, course_attributes))
          num_courses += 1
          if args.debug:
            print(cursor.query)
        except psycopg2.Error as e:
          logs.write(e.pgerror)
          sys.exit(e.pgerror)

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

db.commit()
db.close()
