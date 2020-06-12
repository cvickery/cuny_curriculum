#! /usr/local/bin/python3
""" Compare two rule archives at report changes
    The default is to compare the two latest archive sets.
"""

import csv
import sys
from argparse import ArgumentParser
from collections import defaultdict, namedtuple
from pathlib import Path

ArchiveSet = namedtuple('ArchiveSet', 'destination_courses source_courses transfer_rules')

parser = ArgumentParser('Compare two rule archives')
parser.add_argument('dates', nargs='*')
args = parser.parse_args()

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

print(first_set.destination_courses.name, first_set.source_courses.name)
print(second_set.destination_courses.name, second_set.source_courses.name)
