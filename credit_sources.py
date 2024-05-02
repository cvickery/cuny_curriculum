#! /usr/local/bin/python3
"""Count the types of credit sources."""

import csv
from collections import defaultdict, namedtuple

component_credit_sources = defaultdict(int)
subject_credit_sources = defaultdict(int)

with open('latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES.csv', 'r') as csv_file:
  csv_reader = csv.reader(csv_file)
  for line in csv_reader:
    if csv_reader.line_num == 1:
      Row = namedtuple('Row', [h.lower().replace(' ', '_') for h in line])
    else:
      row = Row._make(line)
      component_credit_sources[row.component_credit_source] += 1
      subject_credit_sources[row.subject_credit_source] += 1

print('Component Credit Sources')
for source, count in component_credit_sources.items():
  print(f'{source} {count:10,}')

print('\nSubject Credit Sources')
for source, count in subject_credit_sources.items():
  print(f'{source} {count:10,}')

  # Component Credit Sources
  # R  1,346,916
  # E    351,588
  # C         24

  # Subject Credit Sources
  # R  1,269,145
  # E    427,933
  # C      1,450
