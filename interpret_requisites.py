#! /usr/local/bin/python3
""" Extract formal descriptions of course requisite structures.
"""

# /Users/vickery/CUNY_Courses/interpret_requisites.py

import re
import csv
from collections import namedtuple
from pymongo import MongoClient

db = MongoClient()

this_course = None
with open('latest_queries/QNS_QCCV_CU_REQUISITES_NP.csv') as csvfile:
  reader = csv.reader(csvfile)
  cols = None
  for line in reader:
    if cols is None:
      if 'Institution' == line[0]:
        cols = [val.lower().replace(' ', '_').replace('/', '_').replace('-', '') for val in line]
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      course = (row.institution, row.subject, row.catalog)
      if course != this_course:
        if this_course is not None:
          # Report the current course requisites
          if requisites.strip() != '':
            requisites = re.sub(' {2,}', ' ', requisites).replace('( ', '(').replace(' )', ')')
            print(f'{this_course} "{requisites}"\n==>{description}')
        # Start new course
        this_course = course
        description = row.descr_of_pre_corequisites
        requisites = ''
      requisites += f" {row.logical_connector.upper()} {row.lp} {row.rqs_type.replace('-Rqs', ':')}"
      requisites += f" {row.req_subject} {row.req_catalog} "
      if row.min_grade != '':
        requisites += f'(min {row.min_grade})'
      requisites += f"{row.cond_code} {row.operator.replace(' or ', '').replace('Not Equal', '!=')}"
      requisites += f" {row.value}"
      if row.consent.startswith('Dept'):
        requisites += ' with Department Approval '
      if row.consent.startswith('Instr'):
        requisites += ' with Instructor Approval '
      requisites += row.rp
