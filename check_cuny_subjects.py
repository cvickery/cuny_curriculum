#! /usr/local/bin/python3
""" Is there a relationship among external subject areas, CIP codes, and HEGIS codes?
    This is a tool for exploring them, and how they relate to college's subjects. The conclusion
    I got was that they correlate, but not perfectly enough to be useful.

    Code preserved as part of the archaeological record.
"""
import argparse
import csv
from collections import namedtuple, defaultdict

parser = argparse.ArgumentParser()
parser.add_argument('name')
args = parser.parse_args()

by_subject = defaultdict(set)
by_external = defaultdict(set)
by_cip = defaultdict(set)
by_hegis = defaultdict(set)
names = dict({
             'Subject': by_subject,
             'External': by_external,
             'CIP': by_cip,
             'HEGIS': by_hegis
             })

Row = None
with open('./latest_queries/QNS_CV_CUNY_SUBJECT_TABLE.csv') as csv_file:
  csv_reader = csv.reader(csv_file)
  for line in csv_reader:
    if Row is None:
      Row = namedtuple('Row', [col.lower().replace(' ', '_') for col in line])
      continue
    row = Row._make(line)
    by_subject[row.subject].add((row.external_subject_area, row.cip_code, row.hegis_code))
    by_external[row.external_subject_area].add((row.cip_code, row.hegis_code))
    by_cip[row.cip_code].add((row.subject, row.external_subject_area, row.hegis_code))
    by_hegis[row.hegis_code].add((row.subject, row.external_subject_area, row.cip_code))

for name, values in names.items():
  if name.lower() == args.name.lower():
    print(f'\n{name}')
    for value in sorted(values):
      print(f'  {value}: {values[value]}')
