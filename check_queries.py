#! /usr/local/bin/python3
""" This is a query set integrity checker, with provisions for “when things go wrong.”
    There is a set of queries that get downloaded to the queries folder, checked for integrity, and
    moved to latest_queries. Previous occupants of latest_queries get archived with their dates in
    archived_queries.
    But:
     * Some queries have been coming in truncated. This utility checks the files in queries for
       emptyness and compares sizes with the corresponding files in latest_queries for size
       differences of 10% or more.
     * Sometimes there will be multiple copies of a query. If they are the same size, this utility
       discards all but the newest.
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
parser.add_argument('-l', '--list', action='store_true')
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

# This is the definitive list of queries used by the project. The check_references.sh script
# uses this list to be sure each one is referenced by a Python script.
query_names = ['ACAD_CAREER_TBL',
               'ACAD_SUBPLN_TBL',
               'ACADEMIC_GROUPS',
               'QCCV_PROG_PLAN_ORG',
               'QCCV_RQMNT_DESIG_TBL',
               'QNS_CV_ACADEMIC_ORGANIZATIONS',
               'QNS_CV_CRSE_EQUIV_TBL',
               'QNS_CV_CUNY_SUBJECT_TABLE',
               'QNS_CV_CUNY_SUBJECTS',
               'QNS_CV_SR_TRNS_INTERNAL_RULES',
               'QNS_QCCV_COURSE_ATTRIBUTES_NP',
               'QNS_QCCV_CU_CATALOG_NP',
               'QNS_QCCV_CU_REQUISITES_NP',
               'SR701____INSTITUTION_TABLE',
               'SR742A___CRSE_ATTRIBUTE_VALUE']

# The list option is used by check_references.sh to get a copy of the query names.
if args.list:
  for query_name in query_names:
    print(query_name)
  exit()

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

# There has to be one new query for each required query, and its size must not differ by more than
# 10% from the corresponding latest_query. Missing latest queries are ignored.
for query_name in query_names:
  target_query = Path(latest_queries, query_name + '.csv')
  target_size = target_query.stat().st_size
  new_instances = [q for q in new_queries.glob(f'{query_name}*.csv')]
  new_query = None
  if len(new_instances) == 0:
    exit(f'No new query for {query_name}')
  if len(new_instances) > 1:
    # Multiple copies; look at sizes and dates. If size test fails, discard the instance.
    if not args.skip_size_check:
      for query in new_queries:
        if query.stat().st_size == 0:
          print(f'Deleting {query.name} because it is empty. ', file=sys.stderr)
          query.unlink()
          new_instances.delete(query)
        if abs(target_size - new_size) > 0.1 * target_size:
          print(f'Deleting {query.name} because its size ({new_size}) is not within 10% of the '
                f'previous query’s size ({target_size})', file=sys.stderr)
          query_unlink()
          new_instances.delete(query)

  # Remove all but newest
  if len(new_queries) > 0:
    new_query = new_queries.pop()
    new_timestamp = new_query.stat().st_mtime
    for query in new_queries:
      if query.stat().st_mtime > new_timestamp:
        print(f'Deleting {new_query.name} because there is a newer one.')
        new_query.unlink()
        new_query = query
        new_time_stamp = new_query.stat.st_mtime
      else:
        print(f'Deleting {query.name} because there is a newer one.')
        query.unlink()
  if new_query is None:
    exit(f'No valid query file found for {query_name}')
  if args.debug:
    print(f'found new query: {new_query.name}', file=sys.stderr)

  # Size check (unless suppressed)
  if not args.skip_size_check:
    new_size = new_query.stat().st_size
    if new_size == 0:
      exit(f'{new_query.name} has zero bytes')
    if abs(target_size - new_size) > 0.1 * target_size:
      exit('{} ({}) differs from {} ({}) by more than 10%'
           .format(new_query.name, new_size, target_query.name, previous_size))
    if args.verbose:
      print(f'{new_query.name} size compares favorably to {target_query.name}', file=sys.stderr)

# Sizes and dates did not cause a problem: do Archive (unless suppressed)
if not args.skip_archive:
  # move each query in latest_queries to query_archive, with stem appended with its new_mod_date
  prev_mod_date = None
  for target_query in [q for q in latest_queries.glob('*')]:
    if prev_mod_date is None:
      prev_mod_date = date.fromtimestamp(target_query.stat().st_mtime).strftime('%Y-%m-%d')
    target_query.rename(archive_dir / f'{target_query.stem}_{prev_mod_date}.csv')
    if args.verbose:
      print(f'{target_query} moved to {archive_dir}/{target_query.stem}_{prev_mod_date}.csv',
            file=sys.stderr)

  # move each query in queries to latest_queries with process_id removed from its stem
  for new_query in [q for q in new_queries.glob('*')]:
    new_query.rename(latest_queries / f'{new_query.stem.strip("0123456789-")}.csv')

# Confirm that everything is copacetic
if is_copacetic().status:
  exit(0)
exit(is_copacetic().message)
