#! /usr/local/bin/python3
"""Check integrity of CUNYfirst query files.

This is a manager/utility for CUNYfirst queries used by the transfer_app project. As a utility,
it provides reporting facilities for the "definitive list" of CUNYfirst queries that it manages
and maintains an archive of previous queries. When one of the reporting options is used, none of
the other features listed next are run.

    When not reporting, the program exits normally only if the latest_queries folder contains a full
    set of query files. The update_db script will not touch the cuny_curriculum database unless this
    test passes.

    Normally, the program performs a sequence of management tasks, which can be overridden by
    command line options. During these operation "notices" may be generated, which are displayed,
    but do not prevent a successful exit. But several "stop" conditions may be found. These are
    displayed and cause the program to exit with an error code as a signal not to update the db.

    The --precheck_only option causes the program to exit normally if the latest_queries folder is
    already in a copacetic state (all queries present and have the same last-modified date), without
    doing any of the following operations. The queries, latest_queries, and query_archive
    directories will not be changed, whether the precheck passes or not.

    Without the --precheck_only option:
      * The queries folder is checked to be sure it contains at least one copy of each of the
        required queries and, unless the --no_size_check option was specified, that these queries
        are approximately the same sizes as the corresponding query files in the latest_queries
        folder. If this step succeeds, the files in the latest_queries folder are archived and
        replaced with the files from the queries folder. The --no_date_check option can be used to
        suppress checking that all the query files were created on the same date.

      * Stray files in the queries and/or latest_queries folders are noted, but do not prevent the
        queries folder from being declared "empty."

      * Normal exit means the queries folder will be "empty"; the latest_queries
        folder will contain all the latest queries, with the same dates and assured size
        correctness; and all previous queries will have been archived.

      * Otherwise, nothing will be changed from the way things were when the program started.
"""

import argparse
import os
import sys

from collections import namedtuple
from datetime import date
from pathlib import Path

DEBUG = os.getenv('DEBUG_CHECK_QUERIES')

SIZE_CHECK_LIMIT = os.getenv('SIZE_CHECK_LIMIT')
if SIZE_CHECK_LIMIT is None:
  SIZE_CHECK_LIMIT = '10'

size_check_limit = float(SIZE_CHECK_LIMIT) / 100.0

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
    'QNS_CV_SESSION_TABLE': 'qns_cv_session_table',
    'SR701____INSTITUTION_TABLE': 'institutions',
    'SR742A___CRSE_ATTRIBUTE_VALUE': 'attribute_values'}

required_query_names = [key for key in run_control_ids.keys()]

Copacetic = namedtuple('Copacetic', 'notices stops')

home_dir = Path.home()
new_queries_dir = Path(home_dir, 'Projects/cuny_curriculum/queries')
latest_queries_dir = Path(home_dir, 'Projects/cuny_curriculum/latest_queries/')
archive_dir = Path(home_dir, 'Projects/cuny_curriculum/query_archive')

for dir in [home_dir, new_queries_dir, latest_queries_dir, archive_dir]:
  assert dir.is_dir(), f'{dir.name} does not exist'


def if_copacetic():
  """Check whether everything is copacetic.

  Copacetic is defined as all needed queries are in the latest_queries folder with the same dates
  and non-zero sizes.
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

  if len(stops) == 0:
    print('Query files are all dated', required_query_date)

  return Copacetic(notices, stops)


if __name__ == '__main__':
  # Command line options
  parser = argparse.ArgumentParser()

  parser.add_argument('-c', '--cleanup', action='store_true')
  parser.add_argument('-d', '--debug', action='store_true')

  parser.add_argument('-l', '--list', action='store_true')
  parser.add_argument('-n', '--num_queries', action='store_true')
  parser.add_argument('-r', '--run_control_ids', action='store_true')

  parser.add_argument('-po', '--precheck_only', action='store_true')
  parser.add_argument('-sd', '--skip_date_check', action='store_true')
  parser.add_argument('-ss', '--skip_size_check', action='store_true')
  parser.add_argument('-sa', '--skip_archive', action='store_true')
  parser.add_argument('-scl', '--size_check_limit', type=int)
  args = parser.parse_args()

  if args.size_check_limit:
    size_check_limit = float(args.size_check_limit) / 100.0

  if args.debug:
    DEBUG = True

  if DEBUG:
    print(f'{args=}', file=sys.stderr)

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

  # Precheck
  # ===============================================================================================
  # Verify that latest_queries folder is okay, a necessary precondition for archiving it and
  # moving in new queries from the (new) queries folder.
  is_copacetic = if_copacetic()
  for notice in is_copacetic.notices:
    print(notice)

  for stop in is_copacetic.stops:
    print(stop)

  if len(is_copacetic.stops) == 0:
    print('Copacetic Precheck OK')
    if args.precheck_only:
      # This is a normal exit when pre-check is okay and new queries are not to be processed
      sys.exit(0)
  else:
    print('Copacetic Precheck NOT OK')
    if args.precheck_only:
      # Pre-check failed with no actions on files: error exit.
      s = '' if len(is_copacetic.stops) == 1 else 's'
      print(f'{len(is_copacetic.stops)} STOP{s}')
      sys.exit(1)

  # New query integrity checks
  # ===============================================================================================
  # All new_queries_dir csv files must have the same modification date (unless suppressed)
  stops = []
  notices = []

  # Check new query dates
  # -----------------------------------------------------------------------------------------------
  # To creare a valid cache of CUNYfirst data, all queries must have been run on the same day.
  new_mod_date = None
  if args.skip_date_check:
    print('Skip date checking')
  else:
    print('Checking dates')
    for new_query in new_queries_dir.glob('*.csv'):
      new_date = date.fromtimestamp(new_query.stat().st_mtime).strftime('%Y-%m-%d')
      if new_mod_date is None:
        new_mod_date = new_date
      if new_date != new_mod_date:
        # This is a stop if the file is required, but just a notice if it is not
        if new_query.stem.strip('-123456789') in required_query_names:
          stops.append(f'STOP: {new_query.name:>36} Expected {new_mod_date}, but got {new_date}.')
        else:
          notices.append(f'NOTICE: Stray file in queries dir with mis-matched date: '
                         f'{new_query.name}')
      else:
        if DEBUG:
          print(new_query.name, 'date ok', file=sys.stderr)

  # Check for complete set of new queries, and their sizes.
  # -----------------------------------------------------------------------------------------------
  #   There has to be one new query for each required query, and its size must be within ± 10% of
  #   the size of the corresponding file in latest_queries (if there is one). The 10% value can be
  #   overridden by the SIZE_CHECK_LIMIT environment variable or the --size_check_limit (-qs)
  #   command line option. The size check can be suppressed altogether with the --skip_size_check
  #   (-ss) command line option.
  print('Check all queries present')  # This can’t be overridden
  for query_name in required_query_names:
    target_query = Path(latest_queries_dir, query_name + '.csv')
    if target_query.exists():
      target_size = target_query.stat().st_size
    else:
      target_size = None
    new_instances = [q for q in new_queries_dir.glob(f'{query_name}*.csv')]

    # There might be multiple copies (or none!)
    newest_query = None
    for query in new_instances:
      newest_size = query.stat().st_size
      if newest_size == 0:
        notices.append(f'NOTICE: Deleting {query.name} because it is empty. ')
        query.unlink()
        new_instances.remove(query)
        continue

      if args.skip_size_check or (target_size is None):
        notices.append(f'NOTICE: size check skipped for {query}')
      elif abs(target_size - newest_size) > (size_check_limit * target_size):
        new_instances.remove(query)
        notices.append(f'NOTICE: Ignoring {query.name} because its size ({newest_size:,}) is '
                       f'not within {int(size_check_limit * 100)}% of the previous query’s '
                       f'size ({target_size:,})')
        if args.cleanup:
          # The --cleanup (-c) option can be used to delete mal-sized files.
          notices.append(f'NOTICE: Deleting {query.name}')
          query.unlink()
        continue

      if newest_query is None:
        newest_query = query
        continue

      if query.stat().st_mtime > newest_query.stat().st_mtime:
        # Keep only the newest instance
        notices.append(f'NOTICE: Deleting {newest_query} because there is a newer one.')
        newest_query.unlink()
        newest_query = query
      else:
        # This one is not newer, so just get rid of it
        notices.append(f'NOTICE: Deleting {query} because there is a newer one.')
        query.unlink()

    if newest_query is None:
      stops.append(f'STOP: No valid query file found for {query_name}')
      continue
    else:
      notices.append(f'NOTICE: found new query file {newest_query}')

  # If there is a full set of valid new queries, archive the latest_queries and move in the new ones
  if len(stops) == 0 and not args.skip_archive:
    # Move each latest_queries file to query_archive, with the file's date appended to its name
    print('Archive previous queries')
    prev_mod_date = None
    for target_query in [Path(latest_queries_dir, f'{q}.csv') for q in required_query_names]:
      if target_query.exists():
        if prev_mod_date is None:
          prev_mod_date = date.fromtimestamp(target_query.stat().st_mtime).strftime('%Y-%m-%d')
        target_query.rename(archive_dir / f'{target_query.stem}_{prev_mod_date}.csv')
        if DEBUG:
          print(f'{target_query} moved to {archive_dir}/{target_query.stem}_{prev_mod_date}.csv',
                file=sys.stderr)
      else:
        # This happens when new queries are to the project: there is no 'latest' query available yet
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
          file.unlink()

    # Move each new query to latest_queries_dir with process_id removed from its stem
    print('Move new queries to latest_queries')
    for new_query in required_query_names:
      new_copies = [qf for qf in Path('queries').glob(f'{new_query}*')]
      assert len(new_copies) == 1, f'ERROR: no file for {new_query}'
      new_copy = new_copies[0]
      new_copy.rename(latest_queries_dir / f'{new_copy.stem.strip("0123456789-")}.csv')

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
