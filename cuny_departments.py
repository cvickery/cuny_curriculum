#! /usr/local/bin/python3
# Clear and re-populate the cuny_departments table.
# This table includes {institution, division, department} tuples.
#
# The problem is that there is no table in CUNYfirst that pairs departments with divisions. Rather,
# each course in the course catalog has both acad_org (department) and acad_grp (division) fields,
# albeit with the same department being paired with different divisions for different courses.
#
# To determine the division each department belongs to, count how many times the department is
# paired with a division in the course catalog, and pick the division that has the largest number of
# pairings when there is more than one.
#
# Generates a log file of anomalies found.

import os
import re
import sys
import csv
from collections import namedtuple
from collections import Counter
from datetime import date

import psycopg2
from psycopg2.extras import NamedTupleCursor

from cuny_divisions import ignore_institutions

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_curriculum')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

ignore_departments = ['PEES-BKL', 'SOC-YRK', 'JOUR-GRD']

# Get list of known institutions
cursor.execute("""
                 select code
                 from cuny_institutions
               """)
known_institutions = [institution.code for institution in cursor.fetchall()]

# Get list of known institution-division pairs
divisions = dict()
Division_Key = namedtuple('Division_Key', 'institution division')
Division_Info = namedtuple('Division_Info', 'courses')

cursor.execute("""
                  select institution, division from cuny_divisions;
               """)
known_divisions = [(r.institution, r.division) for r in cursor.fetchall()]

# Create our cuny_departments table (CUNYfirst academic organizations)
cursor.execute('drop table if exists cuny_departments cascade')
cursor.execute("""
  create table cuny_departments (
  institution text references cuny_institutions,
  division text not null,
  department text primary key,
  department_name text not null,
  department_status text,
  num_courses integer,
  foreign key (institution, division) references cuny_divisions)
  """)

# Create a dict of all known departments from CUNYfirst. Initialize each entry with an empty list of
# divisions.
known_departments = dict()
Department_Key = namedtuple('Department_Key', 'institution department')
Department_Info = namedtuple('Department_Info',
                             """department_name
                                status
                                divisions
                             """)

cols = None
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
      if institution in ignore_institutions:
        continue
      department_key = Department_Key._make([institution, row.acad_org])
      if department_key not in known_departments.keys():
        known_departments[department_key] = Department_Info._make([
                                                                  row.formaldesc.replace('\'', '’'),
                                                                  row.status,
                                                                  []
                                                                  ])

# Go through the entire course catalog and record the division for each one in the department’s
# dict entry.
# Report data integrity anomalies.
courses = dict()
Course_Key = namedtuple('Course_Key', 'course_id offer_nbr')
Course_Info = namedtuple('Course_Info', 'discipline catalog_number')

# Open the log file and course catalog query file
with open('./divisions_report.log', 'w') as report:
  anomalies = 0
  with open('./latest_queries/QNS_QCCV_CU_CATALOG_NP.csv', newline='') as csvfile:
    cat_reader = csv.reader(csvfile)
    cols = None
    for row in cat_reader:
      if cols is None:
        row[0] = row[0].replace('\ufeff', '')
        if 'Institution' == row[0]:
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
          Col = namedtuple('Col', cols)
      else:
        row = Col._make(row)
        institution = row.institution
        discipline = row.subject.strip()

        # If active_only, skip rows for inactive courses
        #   Option removed: it breaks cuny_subjects.py
        #   Key (department)=(BAR01) is not present in table "cuny_departments".
        # course_status = row.crse_catalog_status
        # can_schedule = row.schedule_course
        # discipline_status = row.subject_eff_status
        # if args.active_only and \
        #    (course_status != 'A' or can_schedule != 'Y' or discipline_status != 'A'):
        #   continue

        # Report and ignore courses with unknown institution
        if institution in ignore_institutions:
          continue
        if institution not in known_institutions:
          report.write(f'Unknown institution ({institution}) for {_course_id:06}:{_offer_nbr} .\n')
          continue

        # Ignore rows for known bogus departments
        department = row.acad_org
        if department in ignore_departments:
          continue
        # Report and ignore rows where the department is not in cuny_departments for the institution
        department_key = Department_Key._make([institution, department])
        if department_key not in known_departments.keys():
          report.write(f'Bogus department for {department} at {institution}.\n')
          continue

        # Report and ignore rows where the institution-division pair is not in cuny_divisions
        division = row.acad_group
        if (institution, division) not in known_divisions:
          report.write(f'Bogus institution-division pair: ({institution}-{division})\n')
          continue

        # Record the division claimed for this course’s department
        known_departments[department_key].divisions.append(division)

    # Tally phase complete. Now determine the correct division for each department
    for department_key in known_departments.keys():
      num_divisions = len(known_departments[department_key].divisions)
      if num_divisions == 0:
        # Report and ignore departments with no courses
        qualifier = ''
        # if args.active_only:
        #   qualifier = 'active '
        report.write(f'{department_key.department} at {department_key.institution} '
                     f'ignored because it has no {qualifier}courses\n')
        continue
      elif num_divisions == 1:
        # Counter would return empty list
        which_division = known_departments[department_key].divisions[0]
        num_courses = 1
      else:
        # Get list of (division, frequency) tuples, most frequent in position 0.
        votes = Counter(known_departments[department_key].divisions).most_common()
        which_division = votes[0][0]
        num_courses = votes[0][1]
        if len(votes) > 1:
          if votes[0][1] != 1:
            suffix = 's'
          else:
            suffix = ''
          report.write(f'{department_key.department} at {department_key.institution} '
                       f'has {len(votes)} different divisions\n'
                       f'  Using {which_division} for {votes[0][1]} course{suffix}\n')
          for index in range(1, len(votes)):
            num_courses += votes[index][1]
            if votes[index][1] != 1:
              suffix = 's'
            else:
              suffix = ''
            report.write(f'  Using {which_division} instead of {votes[index][0]} '
                         f'for {votes[index][1]} course{suffix}\n')
          anomalies += 1
      # Insert institution, division, department, department_name, status, num_courses
      query = f"""
                 insert into cuny_departments values(
                 '{department_key.institution}',
                 '{which_division}',
                 '{department_key.department}',
                 '{known_departments[department_key].department_name}',
                 '{known_departments[department_key].status}',
                 '{num_courses}')
               """
      cursor.execute(query)

  suffix = 's'
  if anomalies == 1:
    suffix = ''
  if anomalies == 0:
    anomalies = 'No'
  report.write(f'{anomalies:,} course{suffix} found with inconsistent division{suffix}.\n')

  db.commit()
  db.close()
