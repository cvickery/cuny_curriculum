#! /usr/local/bin/python3

import csv
from collections import namedtuple

Row = None
with open('latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES-38880151.csv') as csvfile:
  reader = csv.reader(csvfile)
  for line in reader:
    if Row is None:
      Row = namedtuple('Row', [c.lower().replace(' ', '_') for c in line])
      continue
    row = Row._make(line)
    if row.source_institution[0:4] == '0000':
      continue
    if float(row.min_grade_pts) > 1.9 and float(row.max_grade_pts) < 3.0:
      print(f'{row.source_institution}-{row.destination_institution}-{row.component_subject_area}-'
            f'{row.src_equivalency_component}\t'
            f'{row.source_course_id}\t{row.source_offer_nbr}\t'
            f'{row.src_min_units}\t{row.src_max_units}\t'
            f'{row.dest_min_units}\t{row.dest_max_units}\t'
            f'{row.min_grade_pts}\t{row.max_grade_pts}\t'
            f'{row.subject_credit_source}\t'
            f'{row.component_credit_source}\t{row.contingent_credit}\t'
            f'{row.transfer_course}')
