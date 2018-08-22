# Use the events table to set rule statuses.
#

import psycopg2
from collections import namedtuple
from psycopg2.extras import NamedTupleCursor

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Clear all existing status bits: only status changes from the events table
# will be reflected in the rules table.
cursor.execute('select count(*) as num_rules from transfer_rules where review_status != 0')
print('  Reset status for {:,} rules ...'.format(cursor.fetchone().num_rules))
cursor.execute('update transfer_rules set review_status = 0 where review_status != 0')

# Initialize the bitmasks dict for this script to work from
cursor.execute('select * from review_status_bits')
Event_Type = namedtuple('Event_Type', [d[0] for d in cursor.description])
event_types = map(Event_Type._make, cursor.fetchall())
bitmasks = dict()
for event_type in event_types:
  bitmasks[event_type.abbr] = event_type.bitmask

# Process the events table
cursor.execute('select * from events')
print('  Process {} events ...'.format(cursor.rowcount))
events = cursor.fetchall()
for event in events:
  cursor.execute("""select review_status
                      from transfer_rules
                     where id = %s
                 """, (event.rule_id,))
  review_status = cursor.fetchone().review_status
  # print('status is {}\n  event_type is {}\n  bitmask is {}'.format(status, event.event_type, bitmasks[event.event_type]))
  review_status = review_status | bitmasks[event.event_type]
  # print('new status:', status)
  cursor.execute("""
                  update transfer_rules set review_status = %s
                   where id = %s
               """, (review_status, event.rule_id))
  db.commit()
print('  Done')
db.close()
