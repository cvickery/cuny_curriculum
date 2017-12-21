# Use the events table to update rule statuses.
#

import psycopg2
from collections import namedtuple

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()

cursor.execute('select * from review_status_bits')
Event_Type = namedtuple('Event_Type', [d[0] for d in cursor.description])
event_types = map(Event_Type._make, cursor.fetchall())
bitmasks = dict()
for event_type in event_types:
  bitmasks[event_type.abbr] = event_type.bitmask

cursor.execute('select * from events')
Event = namedtuple('Event', [d[0] for d in cursor.description])
events = map(Event._make, cursor.fetchall())
for event in events:
  cursor.execute("""select status
                      from rule_groups
                     where source_institution = '{}'
                       and discipline = '{}'
                       and group_number = {}
                       and destination_institution = '{}'
                 """.format(event.source_institution,
                            event.discipline,
                            event.group_number,
                            event.destination_institution))
  status = cursor.fetchone()[0]
  # print('status is {}\n  event_type is {}\n  bitmask is {}'.format(status, event.event_type, bitmasks[event.event_type]))
  status = status | bitmasks[event.event_type]
  # print('new status:', status)
  cursor.execute("""
                  update rule_groups set status = {}
                   where source_institution = '{}'
                     and discipline = '{}'
                     and group_number = {}
                     and destination_institution = '{}'
               """.format(status,
                          event.source_institution,
                          event.discipline,
                          event.group_number,
                          event.destination_institution))
  db.commit()
db.close()