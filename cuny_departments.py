#! /usr/local/bin/python3
# Clear and re-populate the cuny_departments table.
# To determine the division each department belongs to, count how many times the department is
# paired with a division in the course catalog, and pick the one that has the largest number of
# pairings. Generate a log file of anomalies found.

import os
import re
from collections import namedtuple
from datetime import date

import psycopg2
from psycopg2.extras import NamedTupleCursor

import csv

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--active_only', '-a', action='store_true')
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Get list of known institutions
cursor.execute("""
                 select code
                 from institutions
               """)
institutions = [institution.code for institution in cursor.fetchall()]
ignore_institutions = ['MHC01']  # Because the University Registrar said to for this app

# Capture the most recent list of CUNY Academic Organizations (departments)
cursor.execute('drop table if exists cuny_departments cascade')
cursor.execute("""
  create table cuny_departments (
  department text primary key,
  division text not null,
  institution text references institutions,
  description text not null,
  num_courses integer,
  foreign key (institution, division) references cuny_divisions)
  """)

Dept = namedtuple('Department', 'department name status')
cols = None
departments = dict()
department_names = dict()
with open('./latest_queries/QNS_CV_ACADEMIC_ORGANIZATIONS.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  for line in csv_reader:
    if cols is None:
      line[0] = line[0].replace('\ufeff', '')
      if line[0].lower() == 'acad org':
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      institution = row.institution
      if institution in ['CUNY', 'UAPC1', 'MHC01']:
        continue
      if institution not in departments.keys():
        departments[institution] = []
        department_names[institution] = []
      departments[institution].append(Dept._make((row.acad_org, row.formaldesc, row.status)))
      department_names[institution].append(row.acad_org)

known_bogus_departments = ['PEES-BKL', 'SOC-YRK', 'JOUR-GRD']

# The problem is that departments are not consistently paired with their divisions in CUNYfirst,
# so we go through the entire catalog to see how many courses have a department-division pairing,
# and pick the pairing with the largest number of courses. Generate a report where for all cases
# where the pairing is not unanimous.
Course = namedtuple('Course', 'discipline catalog_number')
# Open the report file
with open('./divisions_report.log', 'w') as report:
  anomalies = 0
  courses = dict()
  # Process the catalog file
  with open('./latest_queries/QNS_QCCV_CU_CATALOG_NP.csv', newline='') as csvfile:
    cat_reader = csv.reader(csvfile)
    cols = None
    divisions = dict()
    for row in cat_reader:
      if cols is None:
        row[0] = row[0].replace('\ufeff', '')
        if 'Institution' == row[0]:
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
          Col = namedtuple('Col', cols)
      else:
        row = Col._make(row)
        institution = row.institution
        _course_id = int(row.course_id)
        _offer_nbr = int(row.offer_nbr)
        course_key = (_course_id, _offer_nbr)
        discipline = row.subject.strip()
        catalog_number = row.catalog_number.strip()
        courses[course_key] = Course(discipline, catalog_number)

        # If active-only, skip rows for inactive courses
        course_status = row.crse_catalog_status
        can_schedule = row.schedule_course
        discipline_status = row.subject_eff_status
        if args.active_only and \
           (course_status != 'A' or can_schedule != 'Y' or discipline_status != 'A'):
          continue

        # Report and ignore rows with unknown institution
        institution = row.institution
        if institution in ignore_institutions:
          continue
        if institution not in institutions:
          report.write(f'Unknown institution for {course_id:06}: {institution}.\n')
          continue

        # Ignore rows for known bogus departments
        department = row.acad_org
        if department in known_bogus_departments:
          continue
        # Report and ignore rows where the department is not in cuny_departments for the institution
        if department not in department_names[institution]:
          report.write(f'Unknown department for {course_id:06}: {institution} {department}.\n')
          continue
        division = row.acad_group

        if args.debug:
          print(institution, department, division, course_id)
        divisions_key = (institution, department)
        found = False
        if divisions_key in divisions.keys():
          if args.debug:
            print(divisions_key, ' has ', len(divisions[divisions_key]), ' courses')
          for i in range(len(divisions[divisions_key])):
            # print(i)
            # print(divisions[key][i], division)

            if divisions[divisions_key][i][0] == division:
              course_keys = divisions[divisions_key][i][2]
              course_keys.append(course_key)
              divisions[divisions_key][i] = (division,
                                             divisions[divisions_key][i][1] + 1,
                                             course_keys)
              found = True
              break
          if not found:
            divisions[divisions_key].append((division, 1, [course_key]))
        else:
          divisions[divisions_key] = [(division, 1, [course_key])]

    # Now go through the divisions course-counts and clean up non-singleton pairings
    for divisions_key in divisions.keys():
      which_division = divisions[divisions_key][0]
      if len(divisions[divisions_key]) > 1:
        report.write('\n')
        for other_division in divisions[divisions_key]:
          report.write(f'{key[0]:6} {other_division[0]:12} {key[1]:12} {other_division[1]:5}\n')
          if other_division[1] > which_division[1]:
            which_division = other_division
        report.write(' Using {}.\n'.format(which_division[0]))
        # For each course that needs to be fixed, show its course_id, division, department, the
        # wrong division, and the correct one.
        for other in divisions[divisions_key]:
          if other[0] != which_division[0]:
            for course_key in other[2]:
              report.write('Changing division for {:06}:{} {:>5} {:<8}({}, '
                           '{:>10}) from {:<5} to {}\n'
                           .format(course_key[0], course_key[1],  # course_id:offer_nbr
                                   courses[course_key].discipline,
                                   courses[course_key].catalog_number,
                                   key[0],
                                   key[1],
                                   other[0],
                                   value[0]))
              anomalies += 1
      cursor.execute(f"""
                     insert into cuny_departments values(
                     '{divisions_key[0]}', '{which_division[0]}', '{divisions_key[1]}', {which_division[1]})""")
#
  suffix = 's'
  if anomalies == 1:
    suffix = ''
  if anomalies == 0:
    anomalies = 'No'
  report.write(f'{anomalies:,} course{suffix} found with inconsistent division{suffix}.\n')



      # q = """insert into cuny_departments values('{}', '{}', '{}')""".format(
      #     row[cols.index('acad_org')],
      #     row[cols.index('institution')],
      #     row[cols.index('formaldesc')].replace('\'', '’'))
      # cursor.execute(q)
  db.commit()
  db.close()
