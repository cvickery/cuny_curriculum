import psycopg2
from psycopg2.extras import NamedTupleCursor, Json

import csv
import argparse

from datetime import date
from time import perf_counter
import os
import sys
import re

from collections import namedtuple

start_time = perf_counter()
parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--report', '-r', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()
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
cursor.execute('select code from institutions')
all_colleges = [x[0] for x in cursor.fetchall()]

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
total_rows = sum(1 for line in open(cat_file))
num_rows = 0
num_courses = 0
skipped = 0
Component = namedtuple('Component', 'component hours min_credits max_credits')
with open(cat_file, newline='') as csvfile:
  cat_reader = csv.reader(csvfile)
  cols = None
  for row in cat_reader:
    num_rows += 1
    if 0 == num_rows % 1000: print(f'Row {num_rows:,} / {total_rows:,}\r', end='', file=sys.stderr)
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
      if department == 'PEES-BKL' or department == 'SOC-YRK' or department == 'JOUR-GRD':
        continue
      course_id = r.course_id
      offer_nbr = r.offer_nbr
      discipline = r.subject
      catalog_number = r.catalog_number.strip()
      component = Component._make( (r.component_course_component,
                                        float(r.contact_hours),
                                        float(r.min_units),
                                        float(r.max_units)) )
      lookup_query = """select components
                          from courses
                         where course_id = %s
                           and offer_nbr = %s
                           and discipline = %s
                           and catalog_number = %s"""
      lookup_cursor.execute(lookup_query, (course_id, offer_nbr, discipline, catalog_number))
      if lookup_cursor.rowcount > 0:
        if lookup_cursor.rowcount > 1:
          print(f'{lookup_query} returned multiple rows')
          exit()
        else:
          lookup = lookup_cursor.fetchone()
          components = lookup.components
          # print(f'Lookup found 1: {course_id} {offer_nbr} {discipline} {catalog_number} {components}', file=sys.stderr)
          if component not in components:
            components.append(component)
            update_query = """update courses set components = %s
                            where course_id = %s
                              and offer_nbr = %s
                              and discipline = %s
                              and catalog_number = %s"""
            lookup_cursor.execute(update_query, (Json(components),
                                               course_id, offer_nbr, discipline, catalog_number))
          else:
            print(f'Repeated component: {course_id} {offer_nbr} {discipline} {catalog_number} :: {component}')
      else:
        # print(f'Lookup found 0: {course_id} {offer_nbr} {discipline} {catalog_number}', file=sys.stderr)
        components = [component]
        institution = r.institution
        cuny_subject = r.subject_external_area
        if cuny_subject == '':
          cuny_subject = 'missing'
        title = r.long_course_title.replace("'", "’")\
                                   .replace('\r', '')\
                                   .replace('\n', ' ')\
                                   .replace('( ', '(')

        designation = row[cols.index('designation')]

        requisite_str = 'None'
        if (institution, discipline, catalog_number) in requisites.keys():
          requisite_str = requisites[(institution, discipline, catalog_number)]
        description = row[cols.index('descr')].replace("'", "’")
        career = row[cols.index('career')]
        course_status = row[cols.index('crse_catalog_status')]
        discipline_status = row[cols.index('subject_eff_status')]
        can_schedule = row[cols.index('schedule_course')]
        cursor.execute("""insert into courses values
                          (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (course_id, offer_nbr, institution, cuny_subject, department, discipline,
                        catalog_number, title, Json(components), requisite_str, designation,
                        description, career, course_status, discipline_status, can_schedule))
        num_courses += 1

if args.report:
  print('Inserted or ignored {:,} courses.'.format(num_courses))
  cursor.execute('select count(*) from courses')
  num_found = cursor.fetchone()[0]
  print('  {:,} retained; {:,} duplicates ignored'.format(num_found, num_courses - num_found))
  run_time = perf_counter() - start_time
  minutes = int(run_time / 60.)
  suffix = 's'
  if minutes == 1: suffix = ''
  seconds = run_time - (minutes * 60)
  print('Completed in {minutes} minute{suffix} and {seconds:0.1f} seconds.')

# The date the catalog information for institutions was updated
cursor.execute("update institutions set date_updated='{}'".format(cat_date))
db.commit()
db.close()
