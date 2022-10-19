#! /usr/local/bin/python3
""" Count patterns of subject credit source and component credit source across rules.
    C = Catalog
    R = Я забув
    E = External

    This was an experiment to try to understand the values used to make the destination credits
    match the sending credits. Understanding did not ensue.
"""
import csv
import os
import sys

from collections import defaultdict, namedtuple

with open('./latest_queries/QNS_CV_SR_TRNS_INTERNAL_RULES.csv') as csvfile:
  counters = defaultdict(int)
  reader = csv.reader(csvfile)
  for line in reader:
    if reader.line_num == 1:
      Row = namedtuple('Row', [c.lower().replace(' ', '_') for c in line])
      # for c in line:
      #   print(c.lower().replace(' ', '_'))
    else:
      row = Row._make(line)
      counters[(row.subject_credit_source, row.component_credit_source)] += 1

counters = dict(sorted(counters.items(), key=lambda kv: kv[1], reverse=True))
for key, value in counters.items():
  print(f'{key[0]} {key[1]}: {value:10,}')
