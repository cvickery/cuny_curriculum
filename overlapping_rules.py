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
  """
  """
  global differ_only_in_D_Dminus
  global problems
  ranges = sorted(ranges)
  if len(ranges) == 2:
    if ranges[0][1] < ranges[1][0]:
      return
  if len(ranges) == 3:
    if ranges[0][1] < ranges[1][0] and ranges[1][1] < ranges[2][0]:
      return
  problems[(institution, frozenset(ranges))] += 1


conn = psycopg2.connect('dbname=cuny_courses')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)

problems = defaultdict(int)
last_row = struct(course_id=-1, institution='')
cursor.execute("""
                select r.destination_institution as institution, r.id, s.course_id, min_gpa, max_gpa
                from transfer_rules r, source_courses s
                where r.id = s.rule_id
                order by destination_institution, course_id;
               """)
ranges = []
for row in cursor.fetchall():
  if last_row.course_id != row.course_id or last_row.institution != row.institution:
    if len(ranges) > 1:
      analyze(last_row.institution, last_row.course_id, ranges)
    ranges = set()
  ranges.add((row.min_gpa, min(4.5, row.max_gpa)))
  last_row = row

for key, count in problems.items():
  institution = key[0]
  tuples = str(sorted(key[1]))
  print(f'{institution} {tuples:<54} {count:4}')
