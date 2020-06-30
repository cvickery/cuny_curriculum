#! /usr/local/bin/python3
""" This is a query set integrity checker, with provisions for “when things go wrong.” There is a
    set of queries that get downloaded to the new queries folder, checked for integrity, and moved
    to latest_queries. Previous occupants of latest_queries get archived, with their dates added to
    their names, in archived_queries.
    But:
     * Some queries have been coming in truncated. This utility checks the files in queries for
       emptyness and compares sizes with the corresponding files in latest_queries_dir for size
       differences of 10% or more.
     * Sometimes there will be multiple copies of a query. If they are the same size, this utility
       discards all but the newest.
    The idea is that if this programs completes normally, queries will be empty; latest_queries_dir
    will contain all the latest queries, with the same dates and assured size correctness; and all
    previous queries will have been archived. Otherwise, nothing will be changed from the way things
    were when the program started.
    But stray files in queries and/or latest_queries are notices, not stops.
"""

import sys
from pathlib import Path
from datetime import date
from collections import namedtuple
import argparse

# This is the definitive list of queries used by the project. The check_references.sh script
# uses this list to be sure each one is referenced by a Python script.
# required_query_names = ['ACAD_CAREER_TBL',
#                         'ACAD_SUBPLN_TBL',
#                         'ACADEMIC_GROUPS',
#                         'QCCV_PROG_PLAN_ORG',
#                         'QCCV_RQMNT_DESIG_TBL',
#                         'QNS_CV_ACADEMIC_ORGANIZATIONS',
#                         'QNS_CV_CRSE_EQUIV_TBL',
#                         'QNS_CV_CUNY_SUBJECT_TABLE',
#                         'QNS_CV_CUNY_SUBJECTS',
#                         'QNS_CV_SR_TRNS_INTERNAL_RULES',
#                         'QNS_QCCV_COURSE_ATTRIBUTES_NP',
#                         'QNS_QCCV_CU_CATALOG_NP',
#                         'QNS_QCCV_CU_REQUISITES_NP',
#                         'SR701____INSTITUTION_TABLE',
#                         'SR742A___CRSE_ATTRIBUTE_VALUE']
run_control_ids = {
    'ACAD_CAREER_TBL': 'acad_career',
    'ACAD_SUBPLN_TBL': 'subplans',
    'ACADEMIC_GROUPS': 'groups',
    'QCCV_PROG_PLAN_ORG': 'qccv_prog_plan_org',
    'QCCV_RQMNT_DESIG_TBL': 'qccv_rqmnt_desig_tbl',
    'QNS_CV_ACADEMIC_ORGANIZATIONS': 'cuny_departments',
    'QNS_CV_CRSE_EQUIV_TBL': 'crse_equiv',
    'QNS_CV_CUNY_SUBJECT_TABLE': 'subjects',
    'QNS_CV_CUNY_SUBJECTS': 'cuny_subjects',
    'QNS_CV_SR_TRNS_INTERNAL_RULES': 'transfer_rules_complete',
    'QNS_QCCV_COURSE_ATTRIBUTES_NP': 'cuny_attrs',
    'QNS_QCCV_CU_CATALOG_NP': 'cuny_catalog',
    'QNS_QCCV_CU_REQUISITES_NP': 'cuny_reqs',
    'SR701____INSTITUTION_TABLE': 'institutions',
    'SR742A___CRSE_ATTRIBUTE_VALUE': 'attribute_values'}

required_query_names = [key for key in run_control_ids.keys()]

Copacetic = namedtuple('Copacetic', 'notices stops')
new_queries_dir = Path('/Users/vickery/CUNY_Curriculum/queries')
latest_queries_dir = Path('/Users/vickery/CUNY_Curriculum/latest_queries/')
archive_dir = Path('/Users/vickery/CUNY_Curriculum/query_archive')


def if_copacetic():
  """ Checks if everything is copascetic, which is defined as all needed queries are in the
      latest_queries folder with the same dates and there are no newer queries in the queries
      folder.
      If everything is copacetic before running other checks, despite possible warnings, there is
      nothing for the script to do, and it can exit without further action.
      If the checks are run and there are stop conditions, the script must error-exit so the
      remainder of the update process does not get started.
      Notices:
        * There are files the queries folder, but they are not .csv files that are newer than the
          ones in latest_queries.
        * The latest_queries folder contains file(s) (other than .DS_Store) that are not required
          .csv files
      Stops:
        * Not all required queries are in the latest_queries folder
        * Required queries in latest_queries have different dates
        * There are required queries in the queries folder and they are newer than the ones in
          latest_queries
  """

  notices = []
  stops = []

  # Check the latest queries folder
  latest_queries = [f for f in latest_queries_dir.glob('*') if f.name != '.DS_Store']
  # Check that all the dates of the latest_queries_dir are the same
  latest_query_date = None
  for latest_query in [latest_queries_dir / (query_name + '.csv')
                       for query_name
                       in required_query_names]:
    try:
      # If this file is a required query, remove it from the list of files found in latest_queries.
      # At the end of this loop, any files remaining in the list are stray files, not required
      # queries
      latest_queries.remove(latest_query)
    except ValueError as ve:
      pass
    if not latest_query.exists():
      stops.append(f'No csv file for {latest_query}.')
    else:
      if latest_query_date is None:
        latest_query_date = date.fromtimestamp(latest_query.stat().st_mtime).strftime('%Y-%m-%d')
      this_query_date = date.fromtimestamp(latest_query.stat().st_mtime).strftime('%Y-%m-%d')
      if this_query_date != latest_query_date:
        stops.append(f'Bad query date ({this_query_date}) for {latest_query}.')
  for file in latest_queries:
    notices.append(f'NOTICE: stray file in latest_queries: {file}')

  # Check the new queries folder
  new_queries = [Path(f) for f in new_queries_dir.glob('*') if f.name != '.DS_Store']
  num_new = len(new_queries)
  if num_new == 0:
    notices.append('The queries folder is empty')
  elif num_new == 1:
    notices.append('There is one file in the queries folder.')
  else:
    notices.append(f'There are {num_new} files in the queries folder.')

  #  Stop if a new file is a required query file and is newer than corresponding latest query file
  #  Note if a new file is not a required query file
  for f in new_queries:
    query_name = f.stem.strip('-0123456789')
    if query_name == '.DS_Store':
      continue
    if query_name in required_query_names:
      # check this date against corresponding entry in latest_queries
      new_date = f.stat().st_mtime
      latest_query = latest_queries_dir / (query_name + '.csv')
      if latest_query.exists() and latest_query.stat().st_mtime < new_date:
        stops.append(f'queries/{latest_query.name} is newer than latest_queries')
    else:
      notices.append(f'NOTICE: stray file in queries {f}')

  return Copacetic(notices, stops)


parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-c', '--cleanup', action='store_true')
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('-l', '--list', action='store_true')
parser.add_argument('-n', '--num_queries', action='store_true')
parser.add_argument('-r', '--run_control_ids', action='store_true')
parser.add_argument('-sd', '--skip_date_check', action='store_true')
parser.add_argument('-ss', '--skip_size_check', action='store_true')
parser.add_argument('-sa', '--skip_archive', action='store_true')
args = parser.parse_args()

if args.debug:
  print(args, file=sys.stderr)


# The list option is used by check_references.sh to get a copy of the query names.
if args.list:
  for query_name in required_query_names:
    print(query_name)
  if args.num_queries:
    print(f'{len(required_query_names)} queries')
  exit()

# Expanded version of -l option, for human reference.
if args.run_control_ids:
  for query_name, run_control_id in run_control_ids.items():
    print(f'{query_name:32} {run_control_id}')
  if args.num_queries:
    print(f'{len(required_query_names)} queries')
  exit()

# Precheck: is there anything to check
is_copacetic = if_copacetic()
if len(is_copacetic.stops) == 0:
  for notice in is_copacetic.notices:
    print(f'{notice}', file=sys.stderr)
  print('Copacetic Precheck OK', file=sys.stderr)
  exit(0)

# All new_queries_dir csv files must have the same modification date (unless suppressed)
new_mod_date = None
if not args.skip_date_check:
  for new_query in new_queries_dir.glob('*.csv'):
    new_date = date.fromtimestamp(new_query.stat().st_mtime).strftime('%Y-%m-%d')
    if new_mod_date is None:
      new_mod_date = new_date
    if new_date != new_mod_date:
      # This is a stop if the file is required, but just a notice if it is not
      if new_query.stem.strip('-123456789') in required_query_names:
        exit(f'File date problem for {new_query.name}. '
             f'Expected {new_mod_date}, but got {new_date}.')
      else:
        print(f'ALERT stray new csv file with mis-matched date: {new_query.name}')
    else:
      if args.verbose:
        print(new_query.name, 'date ok', file=sys.stderr)

# There has to be one new query for each required query, and its size must not differ by more than
# 10% from the corresponding latest_query. Missing latest queries are ignored.
for query_name in required_query_names:
  target_query = Path(latest_queries_dir, query_name + '.csv')
  if target_query.exists():
    target_size = target_query.stat().st_size
  else:
    target_size = None
  new_instances = [q for q in new_queries_dir.glob(f'{query_name}*.csv')]
  newest_query = None
  if len(new_instances) == 0:
    exit(f'No new query for {query_name}')
  if len(new_instances) > 1:
    # Multiple copies; look at sizes. If size test fails, discard the instance.
    if not args.skip_size_check:
      for query in new_instances:
        newest_size = query.stat().st_size
        if newest_size == 0:
          print(f'ALERT: Deleting {query.name} because it is empty. ', file=sys.stderr)
          query.unlink()
          new_instances.remove(query)
        elif target_size is not None and abs(target_size - newest_size) > 0.1 * target_size:
          print(f'ALERT: Ignoring {query.name} because its size ({newest_size}) is not within 10% '
                f'of the previous query’s size ({target_size:,})', file=sys.stderr)
          if args.cleanup:
            print(f'CLEANUP: deleting {query.name}')
            query.unlink()
          new_instances.remove(query)

  # Remove all but newest new_instance
  if len(new_instances) > 0:
    newest_query = new_instances.pop()
    newest_timestamp = newest_query.stat().st_mtime
    for query in new_instances:
      if query.stat().st_mtime > newest_timestamp:
        # This one is newer, so get rid of the previous "newest" one, and replace it with this
        print(f'NOTICE: Ignoring {newest_query.name} because there is a newer one.',
              file=sys.stderr)
        newest_query = query
        newest_time_stamp = newest_query.stat().st_mtime
      else:
        # This one is older, so just get rid of it
        print(f'NOTICE: Ignoring {query.name} because there is a newer one.', file=sys.stderr)
  if newest_query is None:
    exit(f'No valid query file found for {query_name}')
  if args.debug:
    print(f'found new query: {newest_query.name}', file=sys.stderr)

  # Size check (unless suppressed)
  if not args.skip_size_check:
    newest_size = newest_query.stat().st_size
    if newest_size == 0:
      exit(f'{newest_query.name} has zero bytes')
    if target_size is not None and abs(target_size - newest_size) > 0.1 * target_size:
      exit(f'STOP: {newest_query.name} ({newest_size}) differs from {target_query.name} '
           f'({target_size}) by more than 10%')
    if args.verbose:
      if target_size is not None:
        print(f'{newest_query.name} size compares favorably to {target_query.name}',
              file=sys.stderr)
      else:
        print(f'{newest_query.name} has {newest_size:,} bytes.')

# Sizes and dates did not cause a problem: do Archive (unless suppressed)
if not args.skip_archive:
  # move each query from latest_queries to query_archive, with stem appended with its
  # new_mod_date
  prev_mod_date = None
  for target_query in [Path(latest_queries_dir, f'{q}.csv') for q in required_query_names]:
    if target_query.exists():
      if prev_mod_date is None:
        prev_mod_date = date.fromtimestamp(target_query.stat().st_mtime).strftime('%Y-%m-%d')
      target_query.rename(archive_dir / f'{target_query.stem}_{prev_mod_date}.csv')
      if args.verbose:
        print(f'{target_query} moved to {archive_dir}/{target_query.stem}_{prev_mod_date}.csv',
              file=sys.stderr)
    else:
      print(f'NOTICE: Unable to archive {target_query} because it does not exist', file=sys.stderr)

  # latest_queries_dir should now be empty
  remnants = [q.name for q in latest_queries_dir.glob('*')]
  try:
    remnants.remove('.DS_Store')
  except ValueError as ve:
    pass
  if len(remnants) != 0:
    if len(remnants) == 1:
      suffix = ''
    else:
      suffix = 's'
    print(f'NOTICE: Stray file{suffix} found in latest_queries_dir: '
          f'{", ".join(remnants)}', file=sys.stderr)
    if args.cleanup:
      for file in remnants:
        print(f'CLEANUP: deleting {file}')
        Path(file).unlink()

  # move each query in queries to latest_queries_dir with process_id removed from its stem
  for new_query in required_query_names:
    query = [q for q in Path('queries').glob(f'{new_query}*')][0]
    query.rename(latest_queries_dir / f'{query.stem.strip("0123456789-")}.csv')

# Confirm that everything is copacetic
is_copacetic = if_copacetic()
for notice in is_copacetic.notices:
  print(notice, file=sys.stderr)
for stop in is_copacetic.stops:
  print(stop, file=sys.stderr)
if len(is_copacetic.stops) == 0:
  print('Copacetic Postcheck OK', file=sys.stderr)
  exit(0)
else:
  print('Copacetic Postcheck NOT OK', file=sys.stderr)
  exit(1)
