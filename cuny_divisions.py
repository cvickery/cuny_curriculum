#! /usr/local/bin/python3
"""
    Find the division for each course in the cuny catalog and populate the divisions table with a
    count of how many courses are offered in each division at each college. Checks for cases where
    the same department appears in different divisions and picks the division with the largest
    number of courses. Generates a report of all courses in the "wrong" division.
"""

import argparse
import os
import re
import csv
from collections import namedtuple
from datetime import date, datetime

import psycopg2
from psycopg2.extras import NamedTupleCursor

parser = argparse.ArgumentParser()
parser.add_argument('--active_only', '-a', action='store_true')
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

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

# Get list of known institutions
cursor.execute("""
                 select code
                 from institutions
               """)
institutions = [institution.code for institution in cursor.fetchall()]

# Get list of known departments
departments = dict()
cursor.execute("""
                select department, institution
                from cuny_departments
                group by department
               """)
for row in cursor.fetchall():
  if row.institution not in departments.keys():
    departments[row.institution] = []
  departments[row.institution].append(row.department)

# Get names of known divisions (“academic groups”)
groups = dict()
cols = None
with open('./latest_queries/ACADEMIC_GROUPS.csv') as csv_file:
  csv_reader = csv.reader(csv_file)
  for line in csv_reader:
    if cols is None:
      cols = [c.lower().replace(' ', '_') for c in line]
      Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      groups[(row.institution, row.academic_group)] = (row.description,
                                                       row.status,
                                                       row.effective_date)
print(groups)
exit()
Course = namedtuple('Course', 'discipline catalog_number')
# Open the report file
with open('./divisions_report.log', 'w') as report:
  anomalies = 0
  courses = dict()
  # Process the catalog file
  with open(cat_file, newline='') as csvfile:
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
        course_id = int(row.course_id)
        offer_nbr = int(row.offer_nbr)
        discipline = row.subject.strip()
        catalog_number = row.catalog_number.strip()
        courses[(course_id, offer_nbr)] = Course(discipline, catalog_number)

        # If active-only, skip rows for inactive courses
        course_status = row.crse_catalog_status
        can_schedule = row.schedule_course
        discipline_status = row.subject_eff_status
        if args.active_only and \
           (course_status != 'A' or can_schedule != 'Y' or discipline_status != 'A'):
          continue

        # Report and ignore rows with unknown institution
        institution = row.institution
        if institution not in institutions:
          report.write(f'Unknown institution for {course_id:06}: {institution}.\n')
          continue

        department = row.acad_org
        # Ignore known bogus departments
        if department == 'PEES-BKL' or department == 'SOC-YRK' or department == 'JOUR-GRD':
          continue
        # Report and ignore rows where the department is not in cuny_departments for the institution
        if department not in departments[institution]:
          report.write(f'Unknown department for {course_id:06}: {institution} {department}.\n')
          continue
        division = row.acad_group

        if args.debug:
          print(institution, department, division, course_id)
        key = (institution, department)
        found = False
        if key in divisions.keys():
          # print(key, divisions[key])
          for i in range(len(divisions[key])):
            # print(i)
            # print(divisions[key][i], division)
            if divisions[key][i][0] == division:
              course_ids = divisions[key][i][2]
              course_ids.append(course_id)
              divisions[key][i] = (division,
                                   divisions[key][i][1] + 1,
                                   course_ids)
              found = True
              break
          if not found:
            divisions[key].append((division, 1, [course_id]))
        else:
          divisions[key] = [(division, 1, [course_id])]
    cursor.execute("""
                   drop table if exists cuny_divisions cascade;
                   create table cuny_divisions (
                     institution text references institutions,
                     division text,
                     department text references cuny_departments,
                     courses integer,
                     primary key (institution, division, department)
                   )
                   """)
    for key in divisions.keys():
      value = divisions[key][0]
      if len(divisions[key]) > 1:
        report.write('\n')
        for other in divisions[key]:
          report.write('{:6} {:12} {:12} {:5}\n'.format(key[0], other[0], key[1], other[1]))
          if other[1] > value[1]:
            value = other
        report.write(' Using {}.\n'.format(value[0]))
        # For each course that needs to be fixed, show its course_id, division, department, the
        # wrong division, and the correct one.
        for other in divisions[key]:
          if other[0] != value[0]:
            for course_id in other[2]:
              report.write(' Changing division for {:06} {:>5} {:<8}({}, {:>10}) from {:<5} to {}\n'
                           .format(course_id,
                                   courses[(course_id, 1)].discipline,
                                   courses[(course_id, 1)].catalog_number,
                                   key[0],
                                   key[1],
                                   other[0],
                                   value[0]))
              anomalies += 1
      cursor.execute("insert into cuny_divisions values('{}', '{}', '{}', {})"
                     .format(key[0], value[0], key[1], value[1]))
#
  suffix = 's'
  if anomalies == 1:
    suffix = ''
  report.write('{:,} course{} found with inconsistent group{}.\n'.format(anomalies, suffix, suffix))

db.commit()
db.close()
