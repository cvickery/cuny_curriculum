import psycopg2
import csv
import argparse

from datetime import date
import os
import re

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('--report', '-r', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()
cursor.execute('select code from institutions')
all_colleges = [x[0] for x in cursor.fetchall()]

all_files = [x for x in os.listdir('./queries/') if x.endswith('.csv')]
# Find most recent catalog, requisite, and attribute files; be sure they all
# have the same date.
latest_cat = '0000-00-00'
latest_req = '0000-00-00'
latest_att = '0000-00-00'
for file in all_files:
  mdate = date.fromtimestamp(os.lstat('./queries/' + file).st_mtime).strftime('%Y-%m-%d')
  if re.search('catalog_np', file, re.I) and mdate > latest_cat:
    latest_cat = mdate
    cat_file = file
  if re.search('requisites_np', file, re.I) and mdate > latest_req:
    latest_req = mdate
    req_file = file
  if re.search('attributes_np', file, re.I) and mdate > latest_att:
    latest_att = mdate
    att_file = file
if not ((latest_cat != '0000-00-00') and (latest_cat == latest_req) and (latest_req == latest_att)):
  print('*** FILE DATES DO NOT MATCH ***')
  for d, file in [[latest_att, att_file], [latest_cat, cat_file], [latest_req, req_file]]:
    print('  {} {}'.format(date.fromtimestamp(os.lstat('./queries/' + file).st_mtime).strftime('%Y-%m-%d'), file))
    exit()
cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'courses'""".format(latest_cat, cat_file))

if args.report:
  print("""Catalog file\t{} ({})\nRequisites file\t{} ({})\nAttributes file\t{} ({})
        """.format(cat_file, latest_cat, req_file, latest_req, att_file, latest_att  ))

# Update the attributes table for all colleges
cursor.execute("delete from course_attributes")
with open('./queries/' + att_file, newline='') as csvfile:
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

# Build dictionary of course requisites; key is (institution, discipline, course_number)
with open('./queries/' + req_file, newline='') as csvfile:
  req_reader = csv.reader(csvfile)
  requisites = {}
  cols = None
  for row in req_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      if 'Institution' == row[0]:
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      # discipline and course number are called subject and catalog
      value = row[cols.index('descr_of_pre_co-requisites')].strip()
      if value != '':
        key = (row[cols.index('institution')],
               row[cols.index('subject')],
               row[cols.index('catalog')])
        requisites[key] = value
if args.debug: print('{:,} requisites'.format(len(requisites)))

# Now process the rows from the courses query.
skip_log = open('./skipped_courses.{}.log'.format(os.getenv('HOSTNAME').split('.')[0]), 'w')
num_courses = 0
skipped = 0
with open('./queries/' + cat_file, newline='') as csvfile:
  cat_reader = csv.reader(csvfile)
  cols = None
  for row in cat_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      if 'Institution' == row[0]:
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      # Skip inactive and administrative courses; insert others
      #   2017-07-12: Retain inactive courses
      #   2017-07-26: Retain all courses!
      # if row[cols.index('approved')] == 'A' and \
      #    row[cols.index('schedule_course')] == 'Y':
      course_id = row[cols.index('course_id')]
      institution = row[cols.index('institution')]
      cuny_subject = row[cols.index('subject_external_area')]
      department = row[cols.index('acad_org')]
      discipline = row[cols.index('subject')]
      catalog_number = row[cols.index('catalog_number')]
      title = row[cols.index('long_course_title')].replace("'", "’")\
                                                  .replace('\r', '')\
                                                  .replace('\n', ' ')\
                                                  .replace('( ', '(')
      catalog_component = row[cols.index('catalog_course_component')]
      hours = row[cols.index('contact_hours')]
      credits = row[cols.index('progress_units')]
      designation = row[cols.index('designation')]
      requisite_str = 'None'
      if (institution, discipline, catalog_number) in requisites.keys():
        requisite_str = requisites[(institution, discipline, catalog_number)].replace("'", "’")
      description = row[cols.index('descr')].replace("'", "’")
      career = row[cols.index('career')]
      course_status = row[cols.index('crse_catalog_status')]
      discipline_status = row[cols.index('subject_eff_status')]
      can_schedule = row[cols.index('schedule_course')]
      q = """
        insert into courses values (
        {}, '{}', '{}', '{}', '{}', '{}', '{}', '{:0.1f}',
        '{:0.1f}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
        on conflict(course_id) do nothing
        """.format(
        course_id,
        institution,
        cuny_subject,
        department,
        discipline,
        catalog_number,
        title,
        float(hours),
        float(credits),
        requisite_str,
        designation,
        description,
        career,
        course_status,
        discipline_status,
        can_schedule)
      if department == 'PEES-BKL' or department == 'SOC-YRK':
        skipped += 1
        skip_log.write('Skipping {} {} {} {} {} {} {} {:0.1f} {:0.1f}\n'.format(course_id,
                                                                               institution,
                                                                               cuny_subject,
                                                                               department,
                                                                               discipline,
                                                                               catalog_number,
                                                                               title,
                                                                               float(hours),
                                                                               float(credits)))
        continue
      cursor.execute(q)
      num_courses += 1
skip_log.close()
if args.report:
  print('Inserted or ignored {:,} courses.'.format(num_courses))
  cursor.execute('select count(*) from courses')
  num_found = cursor.fetchone()[0]
  print('  {:,} retained; {:,} duplicates ignored'.format(num_found, num_courses - num_found))
  print('Skipped {} courses.'.format(skipped))
cursor.execute("update institutions set date_updated='{}'".format(latest_cat))
db.commit()
db.close()
