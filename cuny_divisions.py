import psycopg2
import csv
import argparse

from datetime import date, datetime
import os
import re

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()
# *********************************************************************************#
# Find the division for each course in the cuny catalog and populate the divisions #
# table with a count of how many courses are offered in each division at each      #
# college. Checkes for cases where the same department appears in different        #
# divisions and picks the division with the largest number of courses. Generates a #
# report of all courses in the "wrong" division.                                   #
# *********************************************************************************#

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
institutions = [institution[0] for institution in cursor.fetchall()]

# Get list of known departments
cursor.execute("""
                select department
                from cuny_departments
                group by department
               """)
departments = [department[0] for department in cursor.fetchall()]

# Open the report file
with open ('./divisions_report_{}.log'.format(datetime.now().strftime('%Y-%m-%d')), 'w') \
  as report:
  anomalies = 0
  # Process the catalog file
  with open(cat_file, newline='') as csvfile:
    cat_reader = csv.reader(csvfile)
    cols = None
    divisions = dict()
    for row in cat_reader:
      if cols == None:
        row[0] = row[0].replace('\ufeff', '')
        if 'Institution' == row[0]:
          cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
          # print(cols)
      else:
        institution = row[cols.index('institution')]
        if institution not in institutions: continue
        department = row[cols.index('acad_org')]
        division = row[cols.index('acad_group')]
        course_id = row[cols.index('course_id')]
        # Ignore courses where the department is not in cuny_departments
        if department not in departments: continue
        if args.debug: print(institution, department, division, course_id)
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
        # For each course that needs to be fixed, show its course_id, the wrong division, and the
        # correct one.
        for other in divisions[key]:
          if other[0] != value[0]:
            for course_id in other[2]:
              report.write('  {}: Change group from {} to {}.\n'.format(course_id, other[0], value[0]))
              anomalies += 1
      cursor.execute( """
                        insert into cuny_divisions values('{}', '{}', '{}', {})
                      """.format(key[0], value[0], key[1], value[1]))
#
  suffix = 's'
  if anomalies == 1: suffix = ''
  report.write('{:,} course{} found with inconsistent group{}.\n'.format(anomalies, suffix, suffix))

db.commit()
db.close()
