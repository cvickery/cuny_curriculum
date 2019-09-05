#! /usr/local/bin/python3
""" This is a query set integrity checker, with provisions for “when things go wrong.”
    There is a set of queries that get downloaded to the queries folder, checked for integrity, and
    moved to latest_queries. Previous occupants of latest_queries get archived with their dates in
    archived_queries.
    But:
     * Some queries have been coming in truncated. This utility checks the files in queries for
       emptyness and compares sizes with the corresponding files in latest_queries for size
       differences of 10% or more.
    The idea is that if this programs completes normally, queries will be empty; latest_queries will
    contain all the latest queries, with the same dates and assured size correctness; and all
    previous queries will have been archived. Otherwise, nothing will be changed from the way things
    were when the program started.
"""

import sys
from pathlib import Path
from datetime import date
from collections import namedtuple
import argparse


def is_copacetic():
  """ Checks that everything is copascetic. If true before running, there is nothing to do.
      If not true at any other time, it’s an error condition.
      Possible reasons for failure:
      * new_queries folder contains .csv files
      * not all queries are in the latest_queries folder and/or the ones there have different dates.
  """
  # Check dates of latest_queries
  latest_query_date = None
  for latest_query in [latest_queries / (query_name + '.csv') for query_name in query_names]:
    if latest_query_date is None:
      latest_query_date = date.fromtimestamp(latest_query.stat().st_mtime).strftime('%Y-%m-%d')
    this_query_date = date.fromtimestamp(latest_query.stat().st_mtime).strftime('%Y-%m-%d')
    if this_query_date != latest_query_date:
      return Copacetic(False, f'Bad query date ({this_query_date}) for {latest_query}.')

  # Check that new_queries is empty (of .csv files)
  num_new = len([f for f in new_queries.glob('*.csv')])
  if num_new != 0:
    if num_new == 1:
      return Copacetic(False, 'There is one new query.')
    return Copacetic(False, f'There are {num_new} new queries.')

  return Copacetic(True, 'All queries present and accounted for.')


parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('-sd', '--skip_date_check', action='store_true')
parser.add_argument('-ss', '--skip_size_check', action='store_true')
parser.add_argument('-sa', '--skip_archive', action='store_true')
args = parser.parse_args()

if args.debug:
  print(args, file=sys.stderr)

Copacetic = namedtuple('Copacetic', 'status message')
new_queries = Path('/Users/vickery/CUNY_Courses/queries')
latest_queries = Path('/Users/vickery/CUNY_Courses/latest_queries/')
archive_dir = Path('/Users/vickery/CUNY_Courses/query_archive')
query_names = ['QCCV_RQMNT_DESIG_TBL',
               'QNS_QCCV_CU_CATALOG_NP',
               'QNS_CV_CUNY_SUBJECT_TABLE',
               'QCCV_PROG_PLAN_ORG',
               'SR742A___CRSE_ATTRIBUTE_VALUE',
               'QNS_CV_ACADEMIC_ORGANIZATIONS',
               'ACAD_SUBPLN_TBL',
               'ACAD_CAREER_TBL',
               'ACADEMIC_GROUPS',
               'QNS_CV_CUNY_SUBJECTS',
               'SR701____INSTITUTION_TABLE',
               'QNS_QCCV_COURSE_ATTRIBUTES_NP',
               'QNS_CV_CRSE_EQUIV_TBL',
               'QNS_CV_SR_TRNS_INTERNAL_RULES',
               'QNS_QCCV_CU_REQUISITES_NP']

if is_copacetic().status:
  print(is_copacetic().message)
  exit(0)
if args.verbose:
  print(f'Precondition: {is_copacetic().message}')

# All new_queries must have the same modification date (unless suppressed)
new_mod_date = None
if not args.skip_date_check:
  for new_query in new_queries.glob('*.csv'):
    new_date = date.fromtimestamp(new_query.stat().st_mtime).strftime('%Y-%m-%d')
    if new_mod_date is None:
      new_mod_date = new_date
    if new_date != new_mod_date:
      exit(f'File date problem for {new_query.name}. Expected {new_mod_date}, but got {new_date}.')
    if args.verbose:
      print(new_query.name, 'date ok', file=sys.stderr)

# There has to be one new query for each previous query, and the sizes must not differ by more than
# 10%
for previous_query in latest_queries.glob('*.csv'):
  new_query = [q for q in new_queries.glob(f'{previous_query.stem}*.csv')]
  if len(new_query) == 0:
    exit(f'No new query for {previous_query.name}')
  assert len(new_query) == 1, f'{len(new_query)} matches for {previous_query.name}'
  new_query = new_query[0]
  if args.debug:
    print(f'found new query: {new_query.name}', file=sys.stderr)

  # Size check (unless suppressed)
  if not args.skip_size_check:
    previous_size = previous_query.stat().st_size
    new_size = new_query.stat().st_size
    if new_size == 0:
      exit(f'{new_query.name} has zero bytes')
    if abs(previous_size - new_size) > 0.1 * previous_size:
      exit('{} ({}) differs from {} ({}) by more than 10%'
           .format(new_query.name, new_size, previous_query.name, previous_size))
    if args.verbose:
      print(f'{new_query.name} size compares favorably to {previous_query.name}', file=sys.stderr)

# Sizes and dates did not cause a problem: do Archive (unless suppressed)
if not args.skip_archive:
  # move each query in latest_queries to query_archive, with stem appended with its new_mod_date
  prev_mod_date = None
  for previous_query in [q for q in latest_queries.glob('*.csv')]:
    if prev_mod_date is None:
      prev_mod_date = date.fromtimestamp(previous_query.stat().st_mtime).strftime('%Y-%m-%d')
    previous_query.rename(archive_dir / f'{previous_query.stem}_{prev_mod_date}.csv')
    if args.verbose:
      print(f'{previous_query} moved to {archive_dir}/{previous_query.stem}_{prev_mod_date}.csv',
            file=sys.stderr)
  # move each query in queries to latest_queries with process_id removed from its stem
  for new_query in [q for q in new_queries.glob('*')]:
    new_query.rename(latest_queries / f'{new_query.stem.strip("0123456789-")}.csv')

# Confirm that everything is copacetic
if is_copacetic().status:
  exit(0)
exit(is_copacetic().message)
