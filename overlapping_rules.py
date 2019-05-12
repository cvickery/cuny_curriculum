""" Report rules that overlap one-another in their GPA requirements.
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
  """ Report if a range has gaps or overlaps. In the report, tell the source course and the rule
  sets. (think about the latter) Does not coalesce rule sets.
  """
  global problems
  if len(ranges) == 1:
    # There can’t be an issue if there is only one rule
    return
    # Sort the ranges and be sure, for each pair, that there is no gap and no overlap
  ranges_ok = True
  ranges = sorted(ranges)
  prev = ranges[0]
  for range in ranges[1:]:
    if (prev[1] > range[0]) or (abs(range[0] - prev[1]) > 0.3):
      ranges_ok = False
      break
    prev = range
  if ranges_ok:
    return
  # Add to the set of problems
  problems[(institution, frozenset(ranges))] += 1


conn = psycopg2.connect('dbname=cuny_courses')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)

problems = defaultdict(int)
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
  ranges.add((row.min_gpa, min(4.5, row.max_gpa)))
  previous_row = row

for key, count in problems.items():
  institution = key[0]
  tuples = str(sorted(key[1]))
  print(f'{institution} {tuples:<54} {count:4}')
