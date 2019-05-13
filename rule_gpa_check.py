""" Report rules that overlap one-another or have gaps in their GPA requirements.
    Triggered by MA 114 from QCC to QNS, which says C- or below is blanket credit, but D or above
    transfers as MATH 115.
    For every source course, look at all rule groups it belongs to, and build a list of gpa ranges;
    then check for overlaps and report them.
    The problem is that there are 1.6 million courses, and it takes 30" just to count them all.
"""
# Aglorithm
#   Select all rules and their associated source courses, ordered by destination_institution and
#   course_id.
#   If this is a new institution-course pair, initialize a list of range tuples with the GPA (min,
#   max) for this course
#   Else add the tuple for this row to the list, and report any gaps/overlaps detected.

from collections import defaultdict
import psycopg2
from psycopg2.extras import NamedTupleCursor


class struct:
  def __init__(self, **kwargs):
    self.__dict__.update(kwargs)


def analyze(institution, course_id, ranges):
  """ Given a list of gpa ranges for how a course transfers to an institution, report any cases
      of gpa overlap and/or gaps. Accumulate sums of both types of anomaly by institution.
  """
  global problems
  if len(ranges) == 1:
    # The only possible problem would be if there is a gap
    if ranges[0][0] > 0 or ranges[0][1] < 4.3:
      print(f'    GAP: {institution} {course_id:06} {ranges[0][0]} - {ranges[0][1]}')
      gaps[(institution, frozenset(ranges))] += 1
    return

  # Sort the ranges, which assures only that their minima are in sequence. If the maxima are out of
  # sequence, that will show up as overlaps. Assumes max_gpa is always >= min_gpa. (No exception at
  # this time (May, 2019)).
  ranges = sorted(ranges)

  range_min, range_max = ranges[0]
  gaps_ok = range_min == 0
  laps_ok = True  # ’laps, as in overlaps
  for range in ranges[1:]:
    if range[0] - range_max > 0.3:
      gaps_ok = False
    if range_max - range[0] > 0.3:
      laps_ok = False
    range_max = range[1]
  if not gaps_ok:
    print(f'    GAP: {institution} {course_id:06} {ranges[0][0]} - {ranges[0][1]}')
    gaps[(institution, frozenset(ranges))] += 1
  if not laps_ok:
    print(f'    LAP: {institution} {course_id:06} {ranges[0][0]} - {ranges[0][1]}')
    laps[(institution, frozenset(ranges))] += 1


conn = psycopg2.connect('dbname=cuny_courses')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)

laps = defaultdict(int)
gaps = defaultdict(int)

previous_row = struct(course_id=-1, institution='')
cursor.execute("""
                select r.destination_institution as institution, r.id, s.course_id, min_gpa, max_gpa
                from transfer_rules r, source_courses s
                where r.id = s.rule_id
                order by destination_institution, course_id;
               """)
ranges = []
for row in cursor.fetchall():
  if previous_row.course_id != row.course_id or previous_row.institution != row.institution:
    if len(ranges) > 1:
      analyze(previous_row.institution, previous_row.course_id, ranges)
    ranges = set()
  ranges.add((row.min_gpa, min(4.3, row.max_gpa)))
  previous_row = row

print('Gap counts')
for key, count in gaps.items():
  institution = key[0]
  tuples = str(sorted(key[1]))
  print(f'{institution} {tuples:<54} {count:4}')

print('Overlap counts')
for key, count in laps.items():
  institution = key[0]
  tuples = str(sorted(key[1]))
  print(f'{institution} {tuples:<54} {count:4}')
