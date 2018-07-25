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

start_time = perf_counter()
parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--progress', '-p', action='store_true')
parser.add_argument('--report', '-r', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)
lookup_cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Get the three query files needed, and be sure they are in sync
cat_file = './latest_queries/QNS_QCCV_CU_CATALOG_NP.csv'
req_file = './latest_queries/QNS_QCCV_CU_REQUISITES_NP.csv'
att_file = './latest_queries/QNS_QCCV_COURSE_ATTRIBUTES_NP.csv'
cat_date = date.fromtimestamp(os.lstat(cat_file).st_birthtime).strftime('%Y-%m-%d')
req_date = date.fromtimestamp(os.lstat(req_file).st_birthtime).strftime('%Y-%m-%d')
att_date = date.fromtimestamp(os.lstat(att_file).st_birthtime).strftime('%Y-%m-%d')
if not ((cat_date == req_date) and (req_date == att_date)):
  print('*** FILE DATES DO NOT MATCH ***')
  for d, file in [[att_date, att_file], [cat_date, cat_file], [req_date, req_file]]:
    print('  {} {}'.format(d, file))
    exit()
cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'courses'""".format(cat_date, cat_file))

if args.report:
  print("""Catalog file\t{} ({})\nRequisites file\t{} ({})\nAttributes file\t{} ({})
        """.format(cat_file, cat_date, req_file, req_date, att_file, att_date  ))

# Update the attributes table for all colleges
cursor.execute("delete from course_attributes")
with open(att_file, newline='') as csvfile:
  att_reader = csv.reader(csvfile)
  cols = None
  for row in att_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      if 'Institution' == row[0]:
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = ("insert into course_attributes values ('{}', '{}', '{}', '{}')".format(
          row[cols.index('course_id')],
          row[cols.index('institution')],
          row[cols.index('course_attribute')],
          row[cols.index('course_attribute_value')]))
      cursor.execute(q)
db.commit()

# Cache institutions
cursor.execute('select * from institutions')
all_colleges = [inst.code for inst in cursor.fetchall()]

# Cache a dictionary of course requisites; key is (institution, discipline, catalog)
with open(req_file, newline='') as csvfile:
  req_reader = csv.reader(csvfile)
  requisites = {}
  cols = None
  for row in req_reader:
    if cols == None:
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
if args.debug: print('{:,} requisites'.format(len(requisites)))

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
      print('\r' + 80 * ' ' +
            '\rRow {:,} / {:,}; {:,}  courses. {}:{:02}'.format(num_rows,
                                                                total_rows,
                                                                num_courses,
                                                                remaining_minutes,
                                                                remaining_seconds) ,
            end='',
            file=sys.stderr)

    if cols == None:
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

      # There be departments that are bogus and/or in which we're not interested
      department = r.acad_org
      discipline = r.subject
      if department == 'PEES-BKL' or \
          department == 'SOC-YRK' or \
          department == 'JOUR-GRD' or \
          discipline == 'JOUR':
        continue
      course_id = int(r.course_id)

      # Checking course attributes
      lookup_cursor.execute("""select string_agg(description, '; ') as attributes
                                 from course_attributes, attributes
                                where course_id = %s
                                  and name = attribute_name
                                  and value = attribute_value""",
                            (course_id,))
      attributes = lookup_cursor.fetchone().attributes
      if attributes == None: attributes = 'None'

      offer_nbr = int(r.offer_nbr)
      try:
        equivalence_group = int(r.equiv_course_group)
      except:
        equivalence_group = None
      institution = r.institution
      catalog_number = r.catalog_number.strip()
      component = Component._make([r.component_course_component, float(r.instructor_contact_hours)])
      primary_component = r.primary_component
      contact_hours = float(r.course_contact_hours)
      min_credits = float(r.min_units)
      max_credits = float(r.max_units)
      lookup_query = """select contact_hours, primary_component, min_credits, max_credits, components
                          from courses
                         where course_id = %s
                           and offer_nbr = %s
                           and discipline = %s
                           and catalog_number = %s"""
      lookup_cursor.execute(lookup_query, (course_id, offer_nbr, discipline, catalog_number))
      if lookup_cursor.rowcount > 0:
        if lookup_cursor.rowcount > 1:
          print(f'{lookup_query} returned multiple rows', file=sys.stderr)
          exit()
        else:
          lookup = lookup_cursor.fetchone()
          # Make sure contact_hours, primary_component, and credits haven’t changed
          if contact_hours != lookup.contact_hours or \
             primary_component != lookup.primary_component or \
             min_credits != lookup.min_credits or \
             max_credits != lookup.max_credits:
            print('Inconsistent hours/credits/component for {}-{} {} {}'.format(course_id,
                                                                                offer_nbr,
                                                                                discipline,
                                                                                catalog_number),
                  file=sys.stderr)
            exit
          components = [Component._make(c) for c in lookup.components]
          if component not in components:
            components.append(component)
            # Do the following at display time, putting the primary_component first.
            # Order components alphabetically, but LEC is always first if present.
            # components.sort()
            # if 'LEC' in components and components[0] != 'LEC':
            #   components.remove('LEC')
            #   components = ['LEC'] + components
            update_query = """update courses set components = %s
                            where course_id = %s
                              and offer_nbr = %s
                              and discipline = %s
                              and catalog_number = %s"""
            lookup_cursor.execute(update_query, (Json(components),
                                               course_id, offer_nbr, discipline, catalog_number))
          else:
            if args.report:
              print('Repeated component: {} {} {} {} {} :: {}'.format(course_id,
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

        designation = r.designation

        requisite_str = 'None'
        if (institution, discipline, catalog_number) in requisites.keys():
          requisite_str = requisites[(institution, discipline, catalog_number)]
        description = r.descr.replace("'", "’")
        career = r.career
        course_status = r.crse_catalog_status
        discipline_status = r.subject_eff_status
        can_schedule = r.schedule_course
        try:
          cursor.execute("""insert into courses values
                            (%s, %s, %s, %s, %s,
                             %s, %s, %s, %s,
                             %s, %s, %s, %s, %s,
                             %s, %s, %s, %s, %s,
                             %s, %s, %s)""",
                          (course_id, offer_nbr, equivalence_group, institution, cuny_subject,
                           department, discipline, catalog_number, title,
                           Json(components), contact_hours, min_credits, max_credits, primary_component,
                           requisite_str, designation, description, career, course_status,
                           discipline_status, can_schedule, attributes))
          num_courses += 1
        except psycopg2.Error as e:
          print(e.pgerror)
          exit(e.pgerror)
if args.progress:
  print(file=sys.stderr)
if args.report:
  run_time = perf_counter() - start_time
  minutes = int(run_time / 60.)
  min_suffix = 's'
  if minutes == 1: min_suffix = ''
  seconds = run_time - (minutes * 60)
  print('Inserted {:,} courses in {} minute{} and {:0.1f} seconds.'.format(num_courses,
                                                                           minutes,
                                                                           min_suffix,
                                                                           seconds))

# The date the catalog information for institutions was updated
cursor.execute("update institutions set date_updated='{}'".format(cat_date))
db.commit()
db.close()
