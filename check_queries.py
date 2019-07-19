#! /usr/local/bin/python3
""" Some queries have been coming in truncated. This utility checks the files in queries for
    emptyness and compares sizes with the corresponding files in latest_queries for size differences
    of 10% or more.
"""
from pathlib import Path
from datetime import date
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('-sd', '--skip_date_check', action='store_true')
parser.add_argument('-ss', '--skip_size_check', action='store_true')
parser.add_argument('-sa', '--skip_archive', action='store_true')
args = parser.parse_args()

new_queries = Path('/Users/vickery/CUNY_Courses/queries')
previous_queries = Path('/Users/vickery/CUNY_Courses/latest_queries/')
archive_dir = Path('/Users/vickery/CUNY_Courses/query_archive')

# All new_queries must have the same modification date
new_mod_date = None

for previous_query in previous_queries.glob('*.csv'):
  new_query = [q for q in new_queries.glob(f'{previous_query.stem}*.csv')]
  if len(new_query) == 0:
    exit(f'No new query for {previous_query.name}')
  assert len(new_query) == 1, f'{len(new_query)} matches for {previous_query.name}'
  new_query = new_query[0]
  if args.debug:
    print(f'found new query: {new_query}')

  # Date check
  if not args.skip_date_check:
    new_date = date.fromtimestamp(new_query.stat().st_mtime).strftime('%Y-%m-%d')
    if new_mod_date is None:
      new_mod_date = new_date
    elif new_date != new_mod_date:
      exit(f'File date for {new_query.name}' is not {new_mod_date})
    elif args.verbose:
      print(new_query.name, 'date ok')
    else:
      pass

  # Size check
  if not args.skip_size_check:
    previous_size = previous_query.stat().st_size
    new_size = new_query.stat().st_size
    if new_size == 0:
      exit(f'{new_query.name} has zero bytes')
    if abs(previous_size - new_size) > 0.1 * previous_size:
      exit('{} ({}) differs from {} ({}) by more than 10%'
           .format(new_query.name, new_size, previous_query.name, previous_size))
    if args.verbose:
      print(f'{new_query.name} size compares favorably to {previous_query.name}')

# Sizes and dates did not cause a problem: do Archive
if not args.skip_archive:
  # move each query in latest_queries to query_archive, with stem appended with its new_mod_date
  prev_mod_date = None
  for previous_query in [q for q in previous_queries.glob('*.csv')]:
    if prev_mod_date is None:
      prev_mod_date = date.fromtimestamp(previous_query.stat().st_mtime).strftime('%Y-%m-%d')
    previous_query.rename(archive_dir / f'{previous_query.stem}_{prev_mod_date}.csv')
  # move each query in queries to latest_queries with process_id removed from its stem
  for new_query in [q for q in new_queries.glob('*')]:
    new_query.rename(previous_queries / f'{new_query.stem.strip("0123456789-")}.csv')

exit(0)
