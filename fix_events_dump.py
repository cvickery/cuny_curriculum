# Convert integer group numbers to reals with default fraction part of 0.1 in a dump of the events
# table. Read the dump file from stdin; write the converted file to stdout.

# import re
# import sys
# for line in sys.stdin:
#   # If a line starts with a digit (event.id) and the second number on the line is an int (group
#   # number), add ".1" to the int.
#   if line[0].isdigit():
#     match = re.search('\t([\d\.]+)\t', line)
#     group = float(match[1])
#     if group == int(group):
#       group += 0.1
#       line = re.sub('\t[0-9\.]+\t', '\t{:.1f}\t'.format(group), line, 1)
#   print(line.strip('\n'))

# Version 2: convert source, destination, subject, group to rule_id by using them to lookup the
# transfer_rule.id values in the db.
#
# You have to edit the COPY statement in the dump to account for the new table schema first.
import psycopg2
from psycopg2.extras import NamedTupleCursor

import re
import sys

conn = psycopg2.connect('dbname=cuny_curriculum')
cursor = conn.cursor(cursor_factory=NamedTupleCursor)

for line in sys.stdin:
  if line.startswith('COPY'):
    line = 'COPY public.events (id, rule_id, event_type, who, what, event_time) FROM stdin;'
  # If a line starts with a digit (event.id), use the (source, destination, subject, group) columns
  # to look up the corresponding transfer_rules.id, and replace them with it.
  if line[0].isdigit():
    fields = line.split('\t')
    cursor.execute("""
                   select id
                     from transfer_rules
                    where source_institution = %s
                      and destination_institution = %s
                      and subject_area = %s
                      and group_number = %s
                   """, (fields[1],
                         fields[4],
                         fields[2],
                         fields[3]))
    if cursor.rowcount != 1:
      print(f'\n{line}\n{cursor.query} returned {cursor.rowcount} rows', file=sys.stderr)
      continue
    else:
      fields.pop(1)
      fields.pop(1)
      fields.pop(1)
      fields.pop(1)
      fields.insert(1, f'{cursor.fetchone()[0]}')
      line = '\t'.join(fields)
  # print the line, which may or may not have been altered above.
  print(line.strip('\n'))
