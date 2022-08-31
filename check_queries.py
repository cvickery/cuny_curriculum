#! /usr/local/bin/python3
"""
    This is a manager/utility for CUNYfirst queries used by the transfer_app project. As a utility,
    it provides reporting facilities for the "definitive list" of CUNYfirst queries that it manages
    and maintains an archive of previous queries. When one of the reporting options is used, none of
    the other features listed next are run.

    When not reporting, the program exits normally only if the latest_queries folder contains a full
    set of query files. The update_db script will not touch the cuny_curriculum database unless this
    test passes.

    Normally, the program performs a sequence of management tasks, which can be overridden by
    command line options. During these operation "notices" may be generated, which are displayed,
    but do not prevent an successful exit. But several "stop" conditions may be found. These are
    displayed and cause the program to exit with an error code as a signal to update_db.

    The --ignore_new option causes the program to exit normally if the latest_queries folder is in a
    copacetic state (all queries present and with the same last-modified date), without doing any of
    the following operations.

    The queries folder is checked to be sure it contains at least one copy of each of the required
    queries and, unless the --no_size_check option was specified, that these queries are
    approximately the same sizes as the corresponding query files in the latest_queries folder. If
    this step succeeds, the files in the latest_queries folder are archived and replaced with the
    files from the queries foler.

    If this programs completes normally, the queries folder will be "empty"; latest_queries folder
    will contain all the latest queries, with the same dates and assured size correctness; and all
    previous queries will have been archived. Otherwise, nothing will be changed from the way things
    were when the program started.
    Stray files in the queries and/or latest_queries folders are noted, but do not prevent the
    queries folder from being declared "empty."
"""

import os
import sys
from pathlib import Path
from datetime import date
from collections import namedtuple
import argparse

QUERY_CHECK_LIMIT = os.getenv('QUERY_CHECK_LIMIT')
if QUERY_CHECK_LIMIT is None:
  QUERY_CHECK_LIMIT = '10'
QUERY_CHECK_LIMIT = int(QUERY_CHECK_LIMIT) / 100

# This is the definitive list of queries and their CUNYfirst run_control_ids used by the project.
run_control_ids = {
    'ACAD_CAREER_TBL': 'acad_career',
    'ACAD_PLAN_TBL': 'acad_plan_tbl',
    'ACAD_PLAN_ENRL': 'acad_plan_enrl',
    'ACAD_SUBPLAN_TBL': 'acad_subplan_tbl',
    'ACAD_SUBPLAN_ENRL': 'acad_subplan_enrl',
    'ACADEMIC_GROUPS': 'groups',
    'CIP_CODE_TBL': 'cip_code_tbl',
    'QCCV_PROG_PLAN_ORG': 'qccv_prog_plan_org',
    'QCCV_RQMNT_DESIG_TBL': 'qccv_rqmnt_desig_tbl',
    'QNS_CV_ACADEMIC_ORGANIZATIONS': 'cuny_departments',
    'QNS_CV_CLASS_MAX_TERM': 'qns_cv_class_max_term',
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
new_queries_dir = Path('/Users/vickery/Projects/cuny_curriculum/queries')
latest_queries_dir = Path('/Users/vickery/Projects/cuny_curriculum/latest_queries/')
archive_dir = Path('/Users/vickery/Projects/cuny_curriculum/query_archive')


def if_copacetic():
  """ Checks if everything is copascetic, which is defined as all needed queries are in the
      latest_queries folder with the same dates and non-zero sizes.
  """
  notices = []
  stops = []

  # Check the latest queries folder
  latest_query_names = [f.name for f in latest_queries_dir.glob('*') if f.name != '.DS_Store']

  # Check that all the dates of the latest_queries_dir are the same
  required_query_date = None
  for required_query in [Path(latest_queries_dir, query_name + '.csv')
                         for query_name in required_query_names]:
    if not required_query.exists():
      stops.append(f'STOP: No latest_query file for {required_query.name}.')
    else:
      if required_query_date is None:
        required_query_date = date.fromtimestamp(required_query.stat()
                                                               .st_mtime).strftime('%Y-%m-%d')
      this_query_date = date.fromtimestamp(required_query.stat().st_mtime).strftime('%Y-%m-%d')
      if this_query_date != required_query_date:
        stops.append(f'STOP: Bad query date ({this_query_date}) for {required_query}.')
    try:
      latest_query_names.remove(required_query.name)
    except ValueError:
      pass

  for file in latest_query_names:
    notices.append(f'NOTICE: Stray file in latest_queries: {file}')

  return Copacetic(notices, stops)


if __name__ == '__main__':
  # Command line options
  parser = argparse.ArgumentParser()

  parser.add_argument('-c', '--cleanup', action='store_true')
  parser.add_argument('-d', '--debug', action='store_true')

  parser.add_argument('-l', '--list', action='store_true')
  parser.add_argument('-n', '--num_queries', action='store_true')
  parser.add_argument('-r', '--run_control_ids', action='store_true')

  parser.add_argument('-i', '--ignore_new', action='store_true')
  parser.add_argument('-sa', '--skip_archive', action='store_true')
  parser.add_argument('-sd', '--skip_date_check', action='store_true')
  parser.add_argument('-ss', '--skip_size_check', action='store_true')
  args = parser.parse_args()

  if args.debug:
    print(args, file=sys.stderr)

  # Reporting Functions
  # ===============================================================================================
  # The --list option is used by check_references.sh to get a copy of the query names.
  if args.list:
    for query_name in required_query_names:
      print(query_name)
    if args.num_queries:
      print(f'{len(required_query_names)} queries')
    print('Copaceticity not checked')
    sys.exit(1)

  # Expanded version of the --list option, shows run_control_ids.
  if args.run_control_ids:
    for query_name, run_control_id in run_control_ids.items():
      print(f'{query_name:32} {run_control_id}')
    if args.num_queries:
      print(f'{len(required_query_names)} queries')
    print('Copaceticity not checked')
    sys.exit(1)

  # Precheck: is there anything to check?
  is_copacetic = if_copacetic()
  for notice in is_copacetic.notices:
    print(notice)

  for stop in is_copacetic.stops:
    print(stop)

  if len(is_copacetic.stops) == 0:
    print('Copacetic Precheck OK')
  else:
    print('Copacetic Precheck NOT OK')
    if args.ignore_new:
      # Pre-check failed with no actions on files: error exit.
      s = '' if len(is_copacetic.stops) == 1 else 's'
      print(f'{len(is_copacetic.stops)} STOP{s}')
      sys.exit(1)

  # New query integrity checks
  # ===============================================================================================
  # All new_queries_dir csv files must have the same modification date (unless suppressed)
  if args.ignore_new:
    # This is a normal exit when pre-check is okay and new queries are not to be processed
    sys.exit(0)

  print('Check new queries')
  stops = []
  notices = []

  new_mod_date = None
  if not args.skip_date_check:
    for new_query in new_queries_dir.glob('*.csv'):
      new_date = date.fromtimestamp(new_query.stat().st_mtime).strftime('%Y-%m-%d')
      if new_mod_date is None:
        new_mod_date = new_date
      if new_date != new_mod_date:
        # This is a stop if the file is required, but just a notice if it is not
        if new_query.stem.strip('-123456789') in required_query_names:
          stops.append(f'STOP: {new_query.name}. Expected {new_mod_date}, but got {new_date}.')
        else:
          notices.append(f'NOTICE: Stray new csv file with mis-matched date: {new_query.name}')
      else:
        if args.debug:
          print(new_query.name, 'date ok', file=sys.stderr)

  # There has to be one new query for each required query, and its size must not differ by more
  # than QUERY_CHECK_LIMIT percent from the corresponding latest_query. Missing latest queries are
  # ignored.
  for query_name in required_query_names:
    target_query = Path(latest_queries_dir, query_name + '.csv')
    if target_query.exists():
      target_size = target_query.stat().st_size
    else:
      target_size = None
    new_instances = [q for q in new_queries_dir.glob(f'{query_name}*.csv')]
    newest_query = None
    if len(new_instances) == 0:
      stops.append(f'STOP: No new query for {query_name}')
      continue
    if len(new_instances) > 1:
      # Multiple copies; look at sizes. If size test fails, discard the instance.
      if not args.skip_size_check:
        for query in new_instances:
          newest_size = query.stat().st_size
          if newest_size == 0:
            notices.append(f'NOTICE: Deleting {query.name} because it is empty. ')
            query.unlink()
            new_instances.remove(query)
          elif (target_size is not None
                and abs(target_size - newest_size) > QUERY_CHECK_LIMIT * target_size):
            notices.append(f'NOTICE: Ignoring {query.name} because its size ({newest_size}) is '
                           f'not within {QUERY_CHECK_LIMIT * 100}% of the previous query’s size '
                           f'({target_size:,})')
            if args.cleanup:
              notices.append(f'NOTICE: Deleting {query.name}')
              query.unlink()
            new_instances.remove(query)

    # Remove all but newest new_instance
    if len(new_instances) > 0:
      newest_query = new_instances.pop()
      newest_timestamp = newest_query.stat().st_mtime
      for query in new_instances:
        if query.stat().st_mtime > newest_timestamp:
          # This one is newer, so get rid of the previous "newest" one, and replace it with this
          notices.append(f'NOTICE: Ignoring {newest_query.name} because there is a newer one.')
          newest_query = query
          newest_time_stamp = newest_query.stat().st_mtime
        else:
          # This one is older, so just get rid of it
          notices.append(f'NOTICE: Ignoring {query.name} because there is a newer one.')
    if newest_query is None:
      stops.append(f'STOP: No valid query file found for {query_name}')
      continue
    if args.debug:
      print(f'found new query: {newest_query.name}', file=sys.stderr)

    # Size check (unless suppressed)
    if not args.skip_size_check:
      newest_size = newest_query.stat().st_size
      if newest_size == 0:
        stops.append(f'STOP: {newest_query.name} has zero bytes')
      if (target_size is not None
              and abs(target_size - newest_size) > QUERY_CHECK_LIMIT * target_size):
        stops.append(f'STOP: {newest_query.name} ({newest_size}) differs from {target_query.name} '
                     f'({target_size}) by more than {QUERY_CHECK_LIMIT * 100}%')
      if args.debug:
        if target_size is not None:
          print(f'{newest_query.name} size compares favorably to {target_query.name}',
                file=sys.stderr)
        else:
          print(f'{newest_query.name} has {newest_size:,} bytes.', file=sys.stderr)

  # Sizes and dates did not cause a problem: do Archive (unless suppressed)
  if len(stops) == 0 and not args.skip_archive:
    # move each query from latest_queries to query_archive, with stem appended with its
    # new_mod_date
    print('Archiving')
    prev_mod_date = None
    for target_query in [Path(latest_queries_dir, f'{q}.csv') for q in required_query_names]:
      if target_query.exists():
        if prev_mod_date is None:
          prev_mod_date = date.fromtimestamp(target_query.stat().st_mtime).strftime('%Y-%m-%d')
        target_query.rename(archive_dir / f'{target_query.stem}_{prev_mod_date}.csv')
        if args.debug:
          print(f'{target_query} moved to {archive_dir}/{target_query.stem}_{prev_mod_date}.csv',
                file=sys.stderr)
      else:
        notices.append(f'NOTICE: Unable to archive {target_query} because it does not exist')

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
      notices.append(f'NOTICE: Stray file{suffix} found in latest_queries folder: '
                     f'{", ".join(remnants)}')
      if args.cleanup:
        for file in remnants:
          notices.append(f'CLEANUP: deleting {file}')
          Path(file).unlink()

    # Move each query in queries to latest_queries_dir with process_id removed from its stem
    print('Moving new queries to latest_queries')
    for new_query in required_query_names:
      new_copies = Path('queries').glob(f'{new_query}*')
      for new_copy in new_copies:
        query.rename(latest_queries_dir / f'{new_copy.stem.strip("0123456789-")}.csv')

  # Any notices to report?
  for notice in notices:
    print(notice, file=sys.stderr)
  for stop in stops:
    print(stop, file=sys.stderr)
  if len(stops) > 0:
    s = '' if len(stops) == 1 else 's'
    print(f'Check Queries: {len(stops)} STOP{s}')
    sys.exit(1)

  # Confirm that everything is now copacetic
  is_copacetic = if_copacetic()
  for notice in is_copacetic.notices:
    print(notice, file=sys.stderr)
  for stop in is_copacetic.stops:
    print(stop, file=sys.stderr)
  if len(is_copacetic.stops) == 0:
    print('Copacetic Postcheck OK', file=sys.stderr)
    sys.exit(0)
  else:
    print('Copacetic Postcheck NOT OK', file=sys.stderr)
    sys.exit(1)
