#! /usr/local/bin/python3
""" Compare two rule archives at report changes
    The default is to compare the two latest archive sets.
"""

import csv
import sys
from argparse import ArgumentParser
from collections import defaultdict, namedtuple
from pathlib import Path

# An ArchiveSet consists of lists of source courses and destination courses, linked by a common
# rule_key.
ArchiveSet = namedtuple('ArchiveSet', 'destination_courses source_courses')
SourceCourses = namedtuple('SourceCourses', 'rule_key course_id offer_nbr '
                           ' min_credits max_credits credits_source min_gpa max_gpa')
DestinationCourses = namedtuple('DestinationCourses', 'rule_key course_id offer_nbr '
                                ' transfer_credits')

# Command line options
parser = ArgumentParser('Compare two rule archives')
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('dates', nargs='*')
args = parser.parse_args()
if len(args.dates) > 0:
  sys.exit('Archive date selection not implemented yet')

# Find the two most recent archive sets.
archive_files = Path('./rules_archive').glob('*')
all_archives = defaultdict(list)
for archive_file in archive_files:
  date = archive_file.name[0:10]
  all_archives[date].append(archive_file)
if len(all_archives) < 2:
  sys.exit(f'Not enough archive sets available. {len(all_archives)} found.')

first_date, second_date = sorted(all_archives.keys())[-2:]
first_set = ArchiveSet._make(sorted(all_archives[first_date]))
second_set = ArchiveSet._make(sorted(all_archives[second_date]))

if args.debug:
  print(first_set.destination_courses.name, first_set.source_courses.name)
  print(second_set.destination_courses.name, second_set.source_courses.name)

# The CSV files have a separate row for each course that is part of a rule.
# Here, we build four dictionaries, keyed by the rule_keys, containing lists of course_ids
first_rules_source = defaultdict(list)
first_rules_destination = defaultdict(list)

print('first set source ... ', end='')
with open(first_set.source_courses) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    row = SourceCourses._make(line)
    first_rules_source[row.rule_key].append(row.course_id)
print(f'{len(first_rules_source):,} rows')

print('first set destination ... ', end='')
with open(first_set.destination_courses) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    row = DestinationCourses._make(line)
    first_rules_destination[row.rule_key].append(row.course_id)
print(f'{len(first_rules_destination):,} rows')

second_rules_source = defaultdict(list)
second_rules_destination = defaultdict(list)

print('second set source ... ', end='')
with open(second_set.source_courses) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    row = SourceCourses._make(line)
    second_rules_source[row.rule_key].append(row.course_id)
print(f'{len(second_rules_source):,} rows')

print('second set destination ... ', end='')
with open(second_set.destination_courses) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    row = DestinationCourses._make(line)
    second_rules_destination[row.rule_key].append(row.course_id)
print(f'{len(second_rules_destination):,} rows')

# Be sure the source and destination keys match within the two sets
assert set(first_rules_source.keys()) == set(first_rules_destination.keys())
assert set(second_rules_source.keys()) == set(second_rules_destination.keys())

# Work with the union of the two sets of rules
all_keys = set(first_rules_source.keys()) | set(second_rules_destination.keys())
print(f'{len(all_keys):,} rules to check')
for key in all_keys:
  try:
    if (first_rules_source[key].sort() == second_rules_source[key].sort()
       and first_rules_destination[key].sort() == second_rules_destination[key].sort()):
      continue
    else:
      print(f'{key}')
  except KeyError as e:
    print(f'{key}')
